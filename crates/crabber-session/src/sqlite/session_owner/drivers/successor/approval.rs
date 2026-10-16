//! Live parent/continuation eligibility, distinct from immutable original source.
#[path = "approval/witness.rs"]
mod witness;
use super::{Error, Proof, unknown};
use crabber_core::Run;
use sqlx::{Row, Sqlite, Transaction};
pub(super) async fn check(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    target: &crate::ContinuationTarget,
    parent: &crate::AttemptTarget,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    witness::unstarted(tx, run, parent, proof).await?;
    let (record, _) = crate::sqlite::approval::load_with_proof(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if record.batch.continuation != *target
        || record.batch.next_attempt != *parent
        || record.state != crate::ApprovalContinuationState::Claimed
    {
        return Err(Error::StaleOwner);
    }
    crate::sqlite::approval::write::active(tx, &record)
        .await
        .map_err(unknown)?;
    proof.charge(16).map_err(unknown)?;
    let blocked: bool = sqlx::query_scalar(BLOCKED)
        .bind(&run.session_id.0)
        .bind(&run.id.0)
        .bind(&parent.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if blocked {
        return Err(Error::StaleOwner);
    }
    let current = crate::sqlite::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?
        .0;
    let old = crate::sqlite::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        Some(&record.batch.attempt.id),
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?
    .0;
    if current.snapshot.target != *parent
        || current.snapshot.state != crate::AttemptState::Unstarted
        || current.snapshot.cleanup != crate::AttemptCleanup::Pending
        || current.run_token != run.claim_token
        || current.lease_until <= now
        || current.snapshot.session_id != run.session_id
        || current.snapshot.run_id != run.id
        || old.snapshot.target != record.batch.attempt
        || old.snapshot.state != crate::AttemptState::Suspended
        || old.snapshot.cleanup != crate::AttemptCleanup::Complete
    {
        return Err(Error::StaleOwner);
    }
    proof.charge(8).map_err(unknown)?;
    let count: i64 = sqlx::query_scalar(COUNT)
        .bind(&run.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if usize::try_from(count).map_err(unknown)? != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    for frozen in &record.batch.calls {
        super::super::proof::charge(tx, CALL_COST, &run.id.0, &frozen.target.call_id.0, proof)
            .await?;
        let row = sqlx::query(CALL)
            .bind(&run.id.0)
            .bind(&frozen.target.call_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        let call: crabber_core::ToolCallRecord = serde_json::from_str(
            &row.try_get::<Option<String>, _>("record")
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(unknown)?;
        if call.run_id != run.id
            || call.id != frozen.target.call_id
            || call.status != crabber_core::ToolCallStatus::Pending
            || call.result.is_some()
            || row
                .try_get::<Option<String>, _>("status")
                .map_err(unknown)?
                .as_deref()
                != Some("pending")
            || crate::phase::call_binding(&call).map_err(unknown)? != frozen.call_binding
        {
            return Err(Error::SemanticConflict);
        }
    }
    Ok(())
}
const BLOCKED: &str = "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=?2 AND attempt_id=?3) OR EXISTS(SELECT 1 FROM tool_continuation_barriers WHERE session_id=?1)";
const COUNT: &str = "SELECT COUNT(*) FROM (SELECT 1 FROM tool_calls WHERE run_id=?1 AND status IN ('pending','running') LIMIT 65) AS bounded";
const CALL_COST: &str = "SELECT CAST(length(CAST(status AS BLOB))+length(CAST(data AS BLOB)) AS BIGINT) FROM tool_calls WHERE run_id=?1 AND id=?2";
const CALL: &str = "SELECT CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS record FROM tool_calls WHERE run_id=?1 AND id=?2";
