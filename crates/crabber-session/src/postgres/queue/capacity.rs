//! Former full Queue-record capacity, without transferring prepared bodies.
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::{
    StoreError,
    queue::{MAX_PENDING, corrupt, former_encoded_len},
};
use crabber_core::SessionId;
use sqlx::{Postgres, Row};

// The result contains only three signed64-bit lengths/options and one bool.
// 128 bytes conservatively covers their complete serialized tuple, with no
// private JSON/identifier transfer. The query enforces at most65 such rows.
const ROW_BYTES: usize = 128;
// Only genuine JSON NUL escapes change; paired literal backslashes stay exact.
// Both escapes occupy six bytes. The json type retains original subtree bytes.
// The replacement control is forbidden by every selected identifier domain and
// is explicitly refused below; ignored private keys/values can contain either.
const CAPACITY: &str = r"WITH projections AS MATERIALIZED (
 SELECT q.*, regexp_replace(q.data_json,
 $rx$(?<!\\)((?:\\\\)*)\\u0000$rx$, $replacement$\1\\u0001$replacement$, 'g')::json AS document
 FROM future_queue q WHERE q.session_id=$1 AND q.state IN ('Queued','Claimed')
 ORDER BY q.sequence LIMIT 65
), referenced AS MATERIALIZED (
 SELECT q.*, document->'prepared' AS reference,
 document->>'prepare_operation' AS prepare_operation FROM projections q
), selected AS MATERIALIZED (
 SELECT q.*, regexp_replace(a.data_json,
 $rx$(?<!\\)((?:\\\\)*)\\u0000$rx$, $replacement$\1\\u0001$replacement$, 'g')::json AS audit,
 s.source_digest AS actual_source_digest, s.kind AS source_kind,
 c.protocol_version AS cohort_protocol, c.session_id AS cohort_session
 FROM referenced q
 LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.operation_id=(q.reference->>'operation')
 LEFT JOIN queue_publication_sources s ON s.session_id=q.session_id AND s.queue_id=q.queue_id AND s.operation_id=(q.reference->>'operation')
 LEFT JOIN queue_publication_cohorts c ON c.session_id=q.session_id AND c.queue_id=q.queue_id
)
 SELECT CAST(octet_length(q.data_json) AS BIGINT) AS stored_bytes,
 CASE WHEN q.prepared THEN CAST(octet_length(q.reference::text) AS BIGINT) END AS reference_bytes,
 CASE WHEN q.prepared THEN CAST(octet_length((q.audit->'request'->'action'->'Prepare'->'prepared')::text) AS BIGINT) END AS original_bytes,
 ((q.document->>'version')='2' AND strpos(q.session_id,chr(1))=0 AND strpos(q.queue_id,chr(1))=0
 AND ((NOT q.prepared AND json_typeof(q.reference)='null' AND q.prepare_operation IS NULL)
 OR (q.prepared AND json_typeof(q.reference)='object'
 AND (q.reference->>'operation')=q.prepare_operation
 AND NOT EXISTS (SELECT 1 FROM unnest(ARRAY[q.prepare_operation,q.reference->>'operation',
 q.audit->'request'->>'id',q.audit->'request'->>'session_id',q.audit->'receipt'->>'operation_id',
 q.audit->'receipt'->'summary'->>'queue_id',q.audit->'receipt'->>'session_id']) AS identity(value)
 WHERE value IS NULL OR strpos(value,chr(1))<>0)
 AND octet_length(q.reference::text)<=1024
 AND octet_length((q.audit->'request'->'action'->'Prepare'->'prepared')::text) BETWEEN 1 AND 33554432
 AND (q.audit->'request'->>'id')=(q.reference->>'operation')
 AND (q.audit->'request'->>'session_id')=q.session_id
 AND (q.audit->'receipt'->>'operation_id')=(q.reference->>'operation')
 AND (q.audit->'receipt'->'summary'->>'queue_id')=q.queue_id
 AND (q.audit->'receipt'->>'session_id')=q.session_id
 AND ((q.source_protocol=1 AND (q.reference->>'source_digest') ~ '^[0-9a-f]{64}$'
 AND (q.reference->>'source_digest')=q.actual_source_digest
 AND q.source_kind='prepare' AND q.cohort_protocol=1)
 OR (q.source_protocol=0 AND (q.reference->>'source_digest') IS NULL AND q.cohort_session IS NULL
 AND NOT EXISTS(SELECT 1 FROM queue_publication_sources x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id)))))) AS valid
 FROM selected q ORDER BY q.sequence";

pub(super) async fn pending(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(usize, usize), StoreError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM future_queue WHERE session_id=$1 AND state IN ('Queued','Claimed')",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(super::super::db)?;
    let count = usize::try_from(count).map_err(|_| corrupt())?;
    if count > MAX_PENDING {
        return Err(StoreError::Busy);
    }
    proof
        .charge(count.checked_mul(ROW_BYTES).ok_or_else(corrupt)?)
        .map_err(|_| corrupt())?;
    let rows = sqlx::query(CAPACITY)
        .bind(&session.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(super::super::db)?;
    if rows.len() != count {
        return Err(corrupt());
    }
    let mut bytes = 0_usize;
    for row in rows {
        if !row.try_get::<bool, _>("valid").map_err(|_| corrupt())? {
            return Err(corrupt());
        }
        let stored = usize::try_from(
            row.try_get::<i64, _>("stored_bytes")
                .map_err(|_| corrupt())?,
        )
        .map_err(|_| corrupt())?;
        let reference = length(
            row.try_get::<Option<i64>, _>("reference_bytes")
                .map_err(|_| corrupt())?,
        )?;
        let original = length(
            row.try_get::<Option<i64>, _>("original_bytes")
                .map_err(|_| corrupt())?,
        )?;
        bytes = bytes
            .checked_add(former_encoded_len(stored, reference, original)?)
            .ok_or_else(corrupt)?;
    }
    Ok((count, bytes))
}
fn length(value: Option<i64>) -> Result<Option<usize>, StoreError> {
    value
        .map(|value| usize::try_from(value).map_err(|_| corrupt()))
        .transpose()
}

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;
