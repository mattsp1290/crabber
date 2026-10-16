use super::super::{SqliteStore, db};
use crate::{
    StoreError,
    queue::{
        Audit, QueuePage, QueuePageRequest, QueueReceipt, QueueSummary, Record, RetainedQueue,
        corrupt, decode, identifier, invalid, page, scope, validate_summary, verify_audit,
        verify_sources,
    },
};
use crabber_core::{ControlId, QueueId, SessionId, SessionRevision};
use sqlx::{Row, Sqlite, Transaction};
const RECORD: &str = "SELECT sequence,state,prepared,CASE WHEN length(CAST(data_json AS BLOB))<=33554432 THEN data_json END AS data FROM future_queue WHERE session_id=$1 AND queue_id=$2";
pub(super) const SUMMARY: &str = "SELECT CASE WHEN length(CAST(q.queue_id AS BLOB))<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN length(CAST(q.summary_json AS BLOB))<=16384 THEN q.summary_json END AS summary, CASE WHEN length(CAST(r.data_json AS BLOB))<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN length(CAST(a.operation_id AS BLOB))<=128 THEN a.operation_id END AS latest_operation,CASE WHEN length(CAST(CAST(ar.data AS TEXT) AS BLOB))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.queue_id=$2";
pub(super) async fn record(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &QueueId,
) -> Result<Option<Record>, StoreError> {
    super::super::deletion::ensure_live(tx, session).await?;
    let Some(row) = sqlx::query(RECORD)
        .bind(&session.0)
        .bind(&id.0)
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
    let row = sqlx::query(SUMMARY)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    if summary(&row, session)? != result.summary {
        return Err(corrupt());
    }
    let (_, enqueue) = audited_receipt(tx, session, &result.enqueue_operation)
        .await?
        .ok_or_else(corrupt)?;
    let prepare = if let Some(id) = &result.prepare_operation {
        Some(
            audited_receipt(tx, session, id)
                .await?
                .ok_or_else(corrupt)?
                .1,
        )
    } else {
        None
    };
    let claim = if let Some(claim) = &result.claim {
        Some(
            audited_receipt(tx, session, &claim.operation)
                .await?
                .ok_or_else(corrupt)?
                .1,
        )
    } else {
        None
    };
    verify_sources(&result, &enqueue, prepare.as_ref(), claim.as_ref())?;
    Ok(Some(result))
}
pub(super) fn summary(
    row: &sqlx::sqlite::SqliteRow,
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
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<QueueReceipt>, StoreError> {
    Ok(audited_receipt(tx, session, id)
        .await?
        .map(|(receipt, _)| receipt))
}
async fn audited_receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<(QueueReceipt, Audit)>, StoreError> {
    super::super::deletion::ensure_live(tx, session).await?;
    let Some(row) = sqlx::query("SELECT CASE WHEN length(CAST(r.kind AS BLOB))<=64 THEN r.kind END AS kind,CASE WHEN length(CAST(r.semantic_digest AS BLOB))<=64 THEN r.semantic_digest END AS semantic_digest,r.event_seq,CASE WHEN length(CAST(r.data_json AS BLOB))<=16384 THEN r.data_json END AS receipt,CASE WHEN length(CAST(a.data_json AS BLOB))<=33554432 THEN a.data_json END AS audit FROM operation_receipts r LEFT JOIN future_queue_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)? else { return Ok(None); };
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
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<Option<QueueSummary>, StoreError> {
    let row = sqlx::query("SELECT CASE WHEN length(CAST(q.queue_id AS BLOB))<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN length(CAST(q.summary_json AS BLOB))<=16384 THEN q.summary_json END AS summary, CASE WHEN length(CAST(r.data_json AS BLOB))<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN length(CAST(a.operation_id AS BLOB))<=128 THEN a.operation_id END AS latest_operation,CASE WHEN length(CAST(CAST(ar.data AS TEXT) AS BLOB))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.state IN ('Queued','Claimed') ORDER BY q.sequence LIMIT 1")
        .bind(&session.0).fetch_optional(&mut **tx).await.map_err(db)?;
    row.as_ref().map(|row| summary(row, session)).transpose()
}
impl SqliteStore {
    pub(in crate::sqlite) async fn queue_session_snapshot(
        &self,
        id: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        scope(id, &ControlId("session".into()))?;
        let mut tx = self.readers.begin().await.map_err(db)?;

        let (session, metadata, revision) =
            current(&mut tx, id).await?.ok_or(StoreError::NotFound)?;
        let summary = crate::management::summary(&session, metadata, revision);
        tx.commit().await.map_err(db)?;
        Ok(summary)
    }

    pub(in crate::sqlite) async fn queue_lookup(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<QueueReceipt>, StoreError> {
        scope(session, id)?;
        let mut tx = self.readers.begin().await.map_err(db)?;

        let result = receipt(&mut tx, session, id).await?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    pub(in crate::sqlite) async fn queue_private(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<RetainedQueue, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        let mut tx = self.readers.begin().await.map_err(db)?;

        let result = record(&mut tx, session, id)
            .await?
            .ok_or(StoreError::NotFound)?
            .retained();
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    pub(in crate::sqlite) async fn queue_get(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<QueueSummary, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        let mut tx = self.readers.begin().await.map_err(db)?;
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
    pub(in crate::sqlite) async fn queue_list(
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
        let mut tx = self.readers.begin().await.map_err(db)?;

        super::super::deletion::ensure_live(&mut tx, &request.session_id).await?;
        let raw: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&request.session_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let revision = SessionRevision::new(u64::try_from(raw).map_err(|_| corrupt())?)?;
        let after = i64::try_from(request.after_sequence.unwrap_or(0)).map_err(|_| invalid())?;
        let rows = sqlx::query("WITH entries AS (SELECT session_id,queue_id,sequence,state,prepared,summary_json,admission_key FROM future_queue WHERE session_id=$1 AND sequence>$2 ORDER BY sequence LIMIT $3) SELECT CASE WHEN length(CAST(q.queue_id AS BLOB))<=128 THEN q.queue_id END AS queue_id,q.sequence,q.state,q.prepared,CASE WHEN length(CAST(q.summary_json AS BLOB))<=16384 AND SUM(length(CAST(q.summary_json AS BLOB))+length(CAST(r.data_json AS BLOB))) OVER ()<=262144 THEN q.summary_json END AS summary, CASE WHEN length(CAST(r.data_json AS BLOB))<=16384 THEN r.data_json END AS latest_receipt,a.revision AS latest_revision,CASE WHEN length(CAST(a.operation_id AS BLOB))<=128 THEN a.operation_id END AS latest_operation,CASE WHEN length(CAST(CAST(ar.data AS TEXT) AS BLOB))<=16384 THEN CAST(ar.data AS TEXT) END AS admission_receipt,ar.data IS NOT NULL AS admission_exists  FROM entries q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key ORDER BY q.sequence")
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

pub(super) async fn current(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<
    Option<(
        crabber_core::Session,
        crate::management::Metadata,
        SessionRevision,
    )>,
    StoreError,
> {
    super::super::deletion::ensure_live(tx, session).await?;
    let Some(row) = sqlx::query("SELECT CASE WHEN length(CAST(CAST(s.data AS TEXT) AS BLOB))<=32768 THEN CAST(s.data AS TEXT) END AS session_data,s.public_revision,m.workspace_id,m.archived,m.format_version,CASE WHEN length(CAST(m.data_json AS BLOB))<=2048 THEN m.data_json END AS metadata FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE s.id=$1").bind(&session.0).fetch_optional(&mut **tx).await.map_err(db)? else { return Ok(None); };
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
    Ok(Some((
        stored,
        metadata,
        SessionRevision::new(u64::try_from(raw).map_err(|_| corrupt())?)?,
    )))
}
