//! Bounded read-only scope loaders; callers retain operation eligibility and provenance.
use super::{Error, Proof, unknown};
use crate::transaction::Transaction;
use crabber_core::{Run, Session};
use sqlx::{Row, Sqlite};
pub(super) async fn run(
    tx: &mut Transaction<'_, Sqlite>,
    session_id: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<Run, Error> {
    super::super::proof::charge(tx, RUN_COST, &session_id.0, &run_id.0, proof).await?;
    let row = sqlx::query(RUN)
        .bind(&session_id.0)
        .bind(&run_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let run: Run = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    if run.id != *run_id
        || run.session_id != *session_id
        || row
            .try_get::<Option<String>, _>("token")
            .map_err(unknown)?
            .as_deref()
            != Some(run.claim_token.as_str())
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some(match run.status {
                crabber_core::RunStatus::Running => "running",
                crabber_core::RunStatus::Paused => "paused",
                _ => return Err(Error::InvalidTransition),
            })
        || row.try_get::<i64, _>("lease_until").map_err(unknown)?
            != crate::sqlite::records::micros(run.lease_until)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
pub(super) async fn context(
    tx: &mut Transaction<'_, Sqlite>,
    session_id: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    context: &crabber_core::SessionContext,
    revision: crabber_core::SessionRevision,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::proof::charge(tx, SESSION_COST, &session_id.0, &run_id.0, proof).await?;
    let row = sqlx::query(SESSION)
        .bind(&session_id.0)
        .bind(&run_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let session: Session = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    if session.id != *session_id
        || session.workspace_id != context.workspace_id()
        || session.directory != context.directory()
    {
        return Err(Error::IdentityMismatch);
    }
    if u64::try_from(row.try_get::<i64, _>("revision").map_err(unknown)?).map_err(unknown)?
        != revision.get()
    {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
const RUN_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+length(CAST(claim_token AS BLOB))+length(CAST(status AS BLOB))+8 AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const RUN: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS data,CASE WHEN length(CAST(claim_token AS BLOB))<=512 THEN claim_token END AS token,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,lease_until FROM runs WHERE session_id=$1 AND id=$2";
const SESSION_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+8 AS BIGINT) FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
const SESSION: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=16384 THEN data END AS data,public_revision AS revision FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
