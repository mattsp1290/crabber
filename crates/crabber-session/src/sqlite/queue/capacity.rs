//! Former full Queue-record capacity, without transferring prepared bodies.
use crate::public_session::v2::proof::Proof;
use crate::transaction::Transaction;
use crate::{
    StoreError,
    queue::{MAX_PENDING, corrupt, former_encoded_len},
};
use crabber_core::SessionId;
use sqlx::{Row, Sqlite};

// The result contains only three signed64-bit lengths/options and one bool.
// 128 bytes conservatively covers their complete serialized tuple, with no
// private JSON/identifier transfer. The query enforces at most65 such rows.
const ROW_BYTES: usize = 128;
const CAPACITY: &str = r"SELECT CAST(length(CAST(q.data_json AS BLOB)) AS BIGINT) AS stored_bytes,
 CASE WHEN q.prepared THEN CAST(length(CAST(json_extract(q.data_json,'$.prepared') AS BLOB)) AS BIGINT) END AS reference_bytes,
 CASE WHEN q.prepared THEN CAST(length(CAST(json_extract(a.data_json,'$.request.action.Prepare.prepared') AS BLOB)) AS BIGINT) END AS original_bytes,
 (json_extract(q.data_json,'$.version')=2 AND ((NOT q.prepared AND json_extract(q.data_json,'$.prepared') IS NULL AND json_extract(q.data_json,'$.prepare_operation') IS NULL)
 OR (q.prepared AND json_type(q.data_json,'$.prepared')='object' AND json_extract(q.data_json,'$.prepared.operation')=json_extract(q.data_json,'$.prepare_operation')
 AND length(CAST(json_extract(q.data_json,'$.prepared') AS BLOB))<=1024 AND length(CAST(json_extract(a.data_json,'$.request.action.Prepare.prepared') AS BLOB)) BETWEEN 1 AND 33554432
 AND json_extract(a.data_json,'$.request.id')=json_extract(q.data_json,'$.prepared.operation') AND json_extract(a.data_json,'$.request.session_id')=q.session_id AND json_extract(a.data_json,'$.receipt.operation_id')=json_extract(q.data_json,'$.prepared.operation') AND json_extract(a.data_json,'$.receipt.summary.queue_id')=q.queue_id AND json_extract(a.data_json,'$.receipt.session_id')=q.session_id AND ((q.source_protocol=1 AND json_extract(q.data_json,'$.prepared.source_digest')=s.source_digest AND s.kind='prepare' AND c.protocol_version=1)
 OR (q.source_protocol=0 AND json_extract(q.data_json,'$.prepared.source_digest') IS NULL AND c.session_id IS NULL
 AND NOT EXISTS(SELECT 1 FROM queue_publication_sources x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id)))))) AS valid
 FROM future_queue q
 LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.operation_id=json_extract(q.data_json,'$.prepared.operation')
 LEFT JOIN queue_publication_sources s ON s.session_id=q.session_id AND s.queue_id=q.queue_id AND s.operation_id=json_extract(q.data_json,'$.prepared.operation')
 LEFT JOIN queue_publication_cohorts c ON c.session_id=q.session_id AND c.queue_id=q.queue_id
 WHERE q.session_id=$1 AND q.state IN ('Queued','Claimed') ORDER BY q.sequence LIMIT 65";

pub(super) async fn pending(
    tx: &mut Transaction<'_, Sqlite>,
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
