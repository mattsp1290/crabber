use super::super::{PostgresStore, db};
use super::selection::LoadedRecord;
use crate::postgres::transaction::Transaction;
use crate::{
    StoreError,
    queue::{
        Audit, QueuePage, QueuePageRequest, QueueReceipt, QueueSummary, Record, RetainedQueue,
        corrupt, decode, identifier, invalid, page, scope, validate_summary, verify_audit,
    },
};
use crabber_core::{ControlId, QueueId, SessionId, SessionRevision};
use sqlx::{Postgres, Row};
const RECORD: &str = "SELECT sequence,state,prepared,CASE WHEN octet_length(data_json)<=33554432 AND octet_length(data_json)<=$3 THEN data_json END AS data FROM future_queue WHERE session_id=$1 AND queue_id=$2";
pub(super) const SUMMARY: &str = "SELECT CASE WHEN octet_length(q.queue_id)<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN octet_length(q.summary_json)<=16384 THEN q.summary_json END AS summary, CASE WHEN octet_length(r.data_json)<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN octet_length(a.operation_id)<=128 THEN a.operation_id END AS latest_operation,CASE WHEN octet_length(CAST(ar.data AS TEXT))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.queue_id=$2";

// Workspace discovery uses its own bounded SQL shape and the canonical summary decoder.
const HEADS: &str = r#"WITH entries AS (SELECT q.session_id,q.queue_id,q.sequence,q.state,q.prepared,q.summary_json,q.admission_key FROM future_queue q JOIN sessions s ON s.id=q.session_id WHERE s.workspace_id=$1 AND q.state IN ('Queued','Claimed') AND q.sequence=(SELECT MIN(h.sequence) FROM future_queue h WHERE h.session_id=q.session_id AND h.state IN ('Queued','Claimed')) AND ($2::text IS NULL OR q.session_id COLLATE "C">$2)
ORDER BY q.session_id COLLATE "C" LIMIT $3)
SELECT CASE WHEN octet_length(q.session_id)<=512 THEN q.session_id END AS session_id, CASE WHEN octet_length(q.queue_id)<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN octet_length(q.summary_json)<=16384 AND SUM(octet_length(q.summary_json)+COALESCE(octet_length(r.data_json),0)) OVER ()<=262144 THEN q.summary_json END AS summary, CASE WHEN octet_length(r.data_json)<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN octet_length(a.operation_id)<=128 THEN a.operation_id END AS latest_operation,CASE WHEN octet_length(CAST(ar.data AS TEXT))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM entries q
LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id)
LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL
LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key
ORDER BY q.session_id COLLATE "C""#;
pub(super) async fn record_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &QueueId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<LoadedRecord>, StoreError> {
    super::super::deletion::ensure_live(tx, session).await?;
    let allowance = i64::try_from(proof.budget().remaining()).map_err(|_| corrupt())?;
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST(octet_length(data_json) AS BIGINT) FROM future_queue WHERE session_id=$1 AND queue_id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    proof
        .charge(usize::try_from(bytes.unwrap_or(0)).map_err(|_| corrupt())?)
        .map_err(|_| corrupt())?;
    let Some(row) = sqlx::query(RECORD)
        .bind(&session.0)
        .bind(&id.0)
        .bind(allowance)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
    else {
        return Ok(None);
    };
    let result: Record = decode(
        &row.try_get::<Option<String>, _>("data")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    let seq =
        u64::try_from(row.try_get::<i64, _>("sequence").map_err(db)?).map_err(|_| corrupt())?;
    result.validate(session, id, seq)?;
    if row.try_get::<String, _>("state").map_err(db)? != format!("{:?}", result.summary.state)
        || row.try_get::<bool, _>("prepared").map_err(db)? != result.summary.prepared
    {
        return Err(corrupt());
    }
    let summary_allowance = i64::try_from(proof.budget().remaining()).map_err(|_| corrupt())?;
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST(octet_length(q.summary_json)+COALESCE(octet_length(r.data_json),0)+COALESCE(octet_length(a.operation_id),0)+COALESCE(octet_length(CAST(ar.data AS TEXT)),0) AS BIGINT) FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.queue_id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    proof
        .charge(usize::try_from(bytes.unwrap_or(0)).map_err(|_| corrupt())?)
        .map_err(|_| corrupt())?;
    let query=SUMMARY.replace(" THEN "," AND (octet_length(q.summary_json)+COALESCE(octet_length(r.data_json),0)+COALESCE(octet_length(a.operation_id),0)+COALESCE(octet_length(CAST(ar.data AS TEXT)),0))<=$3 THEN ");
    let row = sqlx::query(&query)
        .bind(&session.0)
        .bind(&id.0)
        .bind(summary_allowance)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    if summary(&row, session)? != result.summary {
        return Err(corrupt());
    }
    let originals = super::protocol::load_record(tx, &result, proof)
        .await
        .map_err(super::protocol::domain)?;
    result.former_encoded_len(originals.prepared(&result)?)?;
    Ok(Some(LoadedRecord {
        projection: result,
        originals,
    }))
}
pub(super) fn summary(
    row: &sqlx::postgres::PgRow,
    session: &SessionId,
) -> Result<QueueSummary, StoreError> {
    let value: QueueSummary = decode(
        &row.try_get::<Option<String>, _>("summary")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    let id = QueueId(
        row.try_get::<Option<String>, _>("queue_id")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    );
    let seq =
        u64::try_from(row.try_get::<i64, _>("sequence").map_err(db)?).map_err(|_| corrupt())?;
    validate_summary(&value, session, &id, seq)?;
    if row.try_get::<String, _>("state").map_err(db)? != format!("{:?}", value.state)
        || row.try_get::<bool, _>("prepared").map_err(db)? != value.prepared
    {
        return Err(corrupt());
    }
    let latest: QueueReceipt = decode(
        &row.try_get::<Option<String>, _>("latest_receipt")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    let revision: i64 = row
        .try_get::<Option<i64>, _>("latest_revision")
        .map_err(db)?
        .ok_or_else(corrupt)?;
    let operation: String = row
        .try_get::<Option<String>, _>("latest_operation")
        .map_err(db)?
        .ok_or_else(corrupt)?;
    if u64::try_from(revision).ok() != Some(latest.revision.get())
        || latest.operation_id.0 != operation
    {
        return Err(corrupt());
    }
    let actual = if row.try_get::<bool, _>("admission_exists").map_err(db)? {
        Some(decode::<crabber_core::AdmissionReceipt>(
            &row.try_get::<Option<String>, _>("admission_receipt")
                .map_err(db)?
                .ok_or_else(corrupt)?,
        )?)
    } else {
        None
    };
    crate::queue::verify_current(&value, &latest, actual.as_ref())?;
    Ok(value)
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<QueueReceipt>, StoreError> {
    Ok(audited_receipt(tx, session, id)
        .await?
        .map(|(receipt, _)| receipt))
}
async fn audited_receipt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<(QueueReceipt, Audit)>, StoreError> {
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    receipt_and_audit_with_proof(tx, session, id, &mut proof).await
}
pub(super) async fn receipt_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<QueueReceipt>, StoreError> {
    Ok(receipt_and_audit_with_proof(tx, session, id, proof)
        .await?
        .map(|(receipt, _)| receipt))
}
async fn receipt_and_audit_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<(QueueReceipt, Audit)>, StoreError> {
    let result = audited_leaf(tx, session, id, proof).await?;
    if let Some((_, audit)) = &result {
        super::protocol::selected(tx, audit, proof)
            .await
            .map_err(super::protocol::domain)?;
    }
    Ok(result)
}
pub(super) async fn audited_leaf(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<(QueueReceipt, Audit)>, StoreError> {
    super::super::deletion::ensure_live(tx, session).await?;
    let allowance = i64::try_from(proof.budget().remaining()).map_err(|_| corrupt())?;
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST(octet_length(r.kind)+octet_length(r.semantic_digest)+octet_length(r.data_json)+COALESCE(octet_length(a.data_json),0) AS BIGINT) FROM operation_receipts r LEFT JOIN future_queue_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    proof
        .charge(usize::try_from(bytes.unwrap_or(0)).map_err(|_| corrupt())?)
        .map_err(|_| corrupt())?;

    let Some(row) = sqlx::query("SELECT CASE WHEN octet_length(r.kind)<=64 AND (octet_length(r.kind)+octet_length(r.semantic_digest)+octet_length(r.data_json)+COALESCE(octet_length(a.data_json),0))<=$3 THEN r.kind END AS kind,CASE WHEN octet_length(r.semantic_digest)<=64 AND (octet_length(r.kind)+octet_length(r.semantic_digest)+octet_length(r.data_json)+COALESCE(octet_length(a.data_json),0))<=$3 THEN r.semantic_digest END AS semantic_digest,r.event_seq,CASE WHEN octet_length(r.data_json)<=16384 AND (octet_length(r.kind)+octet_length(r.semantic_digest)+octet_length(r.data_json)+COALESCE(octet_length(a.data_json),0))<=$3 THEN r.data_json END AS receipt,CASE WHEN octet_length(a.data_json)<=33554432 AND (octet_length(r.kind)+octet_length(r.semantic_digest)+octet_length(r.data_json)+COALESCE(octet_length(a.data_json),0))<=$3 THEN a.data_json END AS audit FROM operation_receipts r LEFT JOIN future_queue_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2")
        .bind(&session.0).bind(&id.0).bind(allowance).fetch_optional(&mut **tx).await.map_err(db)? else { return Ok(None); };
    if row
        .try_get::<Option<String>, _>("kind")
        .map_err(db)?
        .ok_or_else(corrupt)?
        != "future_queue"
    {
        return Err(StoreError::AdmissionConflict);
    }
    if row
        .try_get::<Option<i64>, _>("event_seq")
        .map_err(db)?
        .is_some()
    {
        return Err(corrupt());
    }
    let saved: QueueReceipt = decode(
        &row.try_get::<Option<String>, _>("receipt")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    let audit: Audit = decode(
        &row.try_get::<Option<String>, _>("audit")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    if saved.session_id != *session
        || saved.operation_id != *id
        || saved.semantic_digest.as_str()
            != row
                .try_get::<Option<String>, _>("semantic_digest")
                .map_err(db)?
                .ok_or_else(corrupt)?
    {
        return Err(corrupt());
    }
    verify_audit(&audit, &saved)?;
    Ok(Some((saved, audit)))
}
pub(super) async fn head(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<Option<QueueSummary>, StoreError> {
    let row = sqlx::query("SELECT CASE WHEN octet_length(q.queue_id)<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN octet_length(q.summary_json)<=16384 THEN q.summary_json END AS summary, CASE WHEN octet_length(r.data_json)<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN octet_length(a.operation_id)<=128 THEN a.operation_id END AS latest_operation,CASE WHEN octet_length(CAST(ar.data AS TEXT))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.state IN ('Queued','Claimed') ORDER BY q.sequence LIMIT 1")
        .bind(&session.0).fetch_optional(&mut **tx).await.map_err(db)?;
    row.as_ref().map(|row| summary(row, session)).transpose()
}
impl PostgresStore {
    pub(in crate::postgres) async fn queue_heads(
        &self,
        request: crate::QueueHeadsRequest,
    ) -> Result<crate::QueueHeadsPage, StoreError> {
        crate::queue::validate_heads(&request)?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Queue)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        // Restrict the candidate page before transferring or validating summaries.
        // Reuse the exact item decoder and its receipt/identity checks.
        let rows = sqlx::query(HEADS)
            .bind(&request.workspace_id)
            .bind(request.cursor.as_ref().map(|id| &id.0))
            .bind(i64::from(request.limit) + 1)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        let heads = rows
            .iter()
            .map(|row| {
                let session = SessionId(
                    row.try_get::<Option<String>, _>("session_id")
                        .map_err(db)?
                        .ok_or_else(corrupt)?,
                );
                summary(row, &session)
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        tx.commit().await.map_err(db)?;
        Ok(crate::queue::heads_page(&request, heads))
    }

    pub(in crate::postgres) async fn queue_session_snapshot(
        &self,
        id: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        scope(id, &ControlId("session".into()))?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Queue)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let (session, metadata, revision) =
            current(&mut tx, id).await?.ok_or(StoreError::NotFound)?;
        let summary = crate::management::summary(&session, metadata, revision);
        tx.commit().await.map_err(db)?;
        Ok(summary)
    }

    pub(in crate::postgres) async fn queue_lookup(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<QueueReceipt>, StoreError> {
        scope(session, id)?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Queue)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let result = receipt(&mut tx, session, id).await?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    pub(in crate::postgres) async fn queue_private(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<RetainedQueue, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Queue)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let selected = record_with_proof(&mut tx, session, id, &mut proof)
            .await?
            .ok_or(StoreError::NotFound)?;
        let result = selected.retained(&mut proof)?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    pub(in crate::postgres) async fn queue_get(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<QueueSummary, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Queue)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        super::super::deletion::ensure_live(&mut tx, session).await?;
        let row = sqlx::query(SUMMARY)
            .bind(&session.0)
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let result = summary(&row, session)?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    pub(in crate::postgres) async fn queue_list(
        &self,
        request: QueuePageRequest,
    ) -> Result<QueuePage, StoreError> {
        page(
            &request,
            request
                .expected_revision
                .unwrap_or(SessionRevision::INITIAL),
            vec![],
        )?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Queue)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        super::super::deletion::ensure_live(&mut tx, &request.session_id).await?;
        super::protocol::whole_scope(
            &mut tx,
            &request.session_id,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
        .await
        .map_err(super::protocol::domain)?;
        let raw: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&request.session_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let revision = SessionRevision::new(u64::try_from(raw).map_err(|_| corrupt())?)?;
        let after = i64::try_from(request.after_sequence.unwrap_or(0)).map_err(|_| invalid())?;
        let rows = sqlx::query("WITH entries AS (SELECT session_id,queue_id,sequence,state,prepared,summary_json,admission_key FROM future_queue WHERE session_id=$1 AND sequence>$2 ORDER BY sequence LIMIT $3) SELECT CASE WHEN octet_length(q.queue_id)<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN octet_length(q.summary_json)<=16384 AND SUM(octet_length(q.summary_json)+octet_length(r.data_json)) OVER ()<=262144 THEN q.summary_json END AS summary, CASE WHEN octet_length(r.data_json)<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN octet_length(a.operation_id)<=128 THEN a.operation_id END AS latest_operation,CASE WHEN octet_length(CAST(ar.data AS TEXT))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM entries q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key ORDER BY q.sequence")
            .bind(&request.session_id.0).bind(after).bind(i64::from(request.limit)+1).fetch_all(&mut *tx).await.map_err(db)?;
        let entries = rows
            .iter()
            .map(|row| summary(row, &request.session_id))
            .collect::<Result<Vec<_>, _>>()?;
        let result = page(&request, revision, entries)?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
}

type Current = (
    crabber_core::Session,
    crate::management::Metadata,
    SessionRevision,
);
pub(super) async fn current(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<Option<Current>, StoreError> {
    current_with_proof(
        tx,
        session,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await
    .map_err(super::protocol::domain)
}
/// Charge all selected context text before database transfer or JSON decoding.
pub(in crate::postgres) async fn current_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<Current>, crate::public_session::v2::proof::ReadError<StoreError>> {
    use crate::public_session::v2::proof::ReadError;
    super::super::deletion::ensure_live(tx, session).await?;
    let allowance = i64::try_from(proof.budget().remaining()).map_err(|_| corrupt())?;
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST((octet_length(CAST(s.data AS TEXT))+COALESCE(octet_length(m.data_json),0)+COALESCE(octet_length(m.workspace_id),0)+64) AS BIGINT) FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE s.id=$1")
        .bind(&session.0).fetch_optional(&mut **tx).await.map_err(db)?;
    proof
        .charge(usize::try_from(bytes.unwrap_or(0)).map_err(|_| corrupt())?)
        .map_err(ReadError::Budget)?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(CAST(s.data AS TEXT))<=32768 AND (octet_length(CAST(s.data AS TEXT))+COALESCE(octet_length(m.data_json),0)+COALESCE(octet_length(m.workspace_id),0)+64)<=$2 THEN CAST(s.data AS TEXT) END AS session_data,s.public_revision,CASE WHEN octet_length(m.workspace_id)<=512 AND (octet_length(CAST(s.data AS TEXT))+COALESCE(octet_length(m.data_json),0)+COALESCE(octet_length(m.workspace_id),0)+64)<=$2 THEN m.workspace_id END AS workspace_id,m.archived,m.format_version,CASE WHEN octet_length(m.data_json)<=2048 AND (octet_length(CAST(s.data AS TEXT))+COALESCE(octet_length(m.data_json),0)+COALESCE(octet_length(m.workspace_id),0)+64)<=$2 THEN m.data_json END AS metadata FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE s.id=$1").bind(&session.0).bind(allowance)
        .fetch_optional(&mut **tx).await.map_err(db)?;
    row.as_ref()
        .map(|row| decode_current(row, session))
        .transpose()
        .map_err(Into::into)
}
fn decode_current(row: &sqlx::postgres::PgRow, session: &SessionId) -> Result<Current, StoreError> {
    let stored: crabber_core::Session = decode(
        &row.try_get::<Option<String>, _>("session_data")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    if stored.id != *session {
        return Err(corrupt());
    }
    let metadata: crate::management::Metadata = match row
        .try_get::<Option<i32>, _>("format_version")
        .map_err(db)?
    {
        None => crate::management::Metadata::default(),
        Some(1) => {
            let metadata: crate::management::Metadata = decode(
                &row.try_get::<Option<String>, _>("metadata")
                    .map_err(db)?
                    .ok_or_else(corrupt)?,
            )?;
            if row
                .try_get::<Option<String>, _>("workspace_id")
                .map_err(db)?
                .as_deref()
                != Some(&stored.workspace_id)
                || row.try_get::<Option<bool>, _>("archived").map_err(db)?
                    != Some(metadata.archived)
            {
                return Err(corrupt());
            }
            metadata
        }
        Some(_) => return Err(corrupt()),
    };
    crate::queue::validate_session(&stored, &metadata)?;
    let raw: i64 = row.try_get("public_revision").map_err(db)?;
    Ok((
        stored,
        metadata,
        SessionRevision::new(u64::try_from(raw).map_err(|_| corrupt())?)?,
    ))
}
