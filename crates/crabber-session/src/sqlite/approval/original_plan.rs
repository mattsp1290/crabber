//! SQL transfers only a bounded original batch, independent of current private state.
use super::super::SqliteStore;
use super::Error;
use crabber_core::{InputFingerprint, RunId, SessionId};
use sqlx::Row;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
impl SqliteStore {
    pub(in super::super) async fn read_original_approval_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<InputFingerprint, Error> {
        crate::continuation_origin::validate_scope(session, run, target)
            .map_err(|()| Error::InvalidRequest)?;
        let mut tx = self.readers.begin().await.map_err(unknown)?;

        let result = async {
            let batch = batch_with_proof(
                &mut tx,
                session,
                run,
                target,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
            )
            .await?;
            let receipt = super::receipt(&mut tx, session, &batch.id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            crate::continuation_origin::approval(&batch, session, run, target, &receipt)
        }
        .await;
        tx.rollback().await.map_err(unknown)?;
        result
    }
}

/// Exact immutable-batch projection; caller authenticates its suspension source.
pub(in crate::sqlite) async fn batch_with_proof(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session: &SessionId,
    run: &RunId,
    target: &crate::ContinuationTarget,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crate::ApprovalSuspension, Error> {
    crate::continuation_origin::validate_scope(session, run, target)
        .map_err(|()| Error::InvalidRequest)?;
    let generation = i64::try_from(target.generation.get()).map_err(unknown)?;
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(COST)
            .bind(&session.0)
            .bind(&run.0)
            .bind(&target.id.0)
            .bind(generation),
        proof,
    )
    .await
    .map_err(unknown)?;
    let row = sqlx::query(BATCH)
        .bind(&session.0)
        .bind(&run.0)
        .bind(&target.id.0)
        .bind(generation)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if row.try_get::<i64, _>("version").map_err(unknown)? != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    let raw = row
        .try_get::<Option<String>, _>("batch")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let batch: crate::ApprovalSuspension = serde_json::from_str(&raw).map_err(unknown)?;
    if batch.continuation != *target {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(batch)
}
const COST: &str = r"WITH original AS (SELECT format_version,json_extract(data_json,'$.batch') AS batch FROM continuations WHERE session_id=?1 AND run_id=?2 AND id=?3 AND generation=?4) SELECT CAST(COALESCE(length(CAST(batch AS BLOB)),0)+8 AS BIGINT) FROM original";
const BATCH: &str = r"WITH original AS (SELECT format_version,json_extract(data_json,'$.batch') AS batch FROM continuations WHERE session_id=?1 AND run_id=?2 AND id=?3 AND generation=?4) SELECT CAST(format_version AS BIGINT) AS version,CASE WHEN length(CAST(batch AS BLOB))<=4194304 THEN batch END AS batch FROM original";
