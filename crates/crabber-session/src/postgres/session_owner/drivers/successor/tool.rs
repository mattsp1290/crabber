//! Current compound guards, with the caller's enclosing source budget.
use super::{Error, Proof, unknown};
use crate::postgres::transaction::Transaction;
use crate::session_owner::driver::successor::tool_fresh as validate;
use crabber_core::Run;
use sqlx::{Postgres, Row};
pub(super) async fn check(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    target: &crate::ContinuationTarget,
    parent: &crate::AttemptTarget,
    receipt: &crate::ToolContinuationReceipt,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::approval::unstarted(tx, run, parent, proof).await?;
    let record = crate::postgres::tool_continuation::read::origin_with_proof(
        tx,
        &run.session_id,
        &run.id,
        None,
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?;
    crate::postgres::tool_continuation::current::verify_with_proof(tx, &record, proof)
        .await
        .map_err(unknown)?;
    if record.batch.continuation != *target
        || record.batch.next_attempt != *parent
        || record.state != crate::ToolContinuationState::Claimed
    {
        return Err(Error::StaleOwner);
    }
    validate::source(&record, receipt)?;
    proof.charge(16).map_err(unknown)?;
    let blocked: bool = sqlx::query_scalar(BLOCKED)
        .bind(&run.session_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if blocked {
        return Err(Error::StaleOwner);
    }
    let current = crate::postgres::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?
        .0;
    validate::parent(run, &current, parent, true, now)?;
    let old = crate::postgres::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        Some(&record.batch.attempt.id),
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?
    .0;
    validate::parent(run, &old, &record.batch.attempt, false, now)?;
    phases(tx, run, &record.batch, proof).await?;
    calls(tx, run, &record.batch, proof).await
}
async fn phases(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    batch: &crate::ToolContinuationSuspension,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.charge(8).map_err(unknown)?;
    let count: i64 = sqlx::query_scalar(PHASE_COUNT)
        .bind(&run.id.0)
        .bind(&batch.next_attempt.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if usize::try_from(count).map_err(unknown)? != batch.phases.len() + batch.frontend.len() {
        return Err(Error::StaleOwner);
    }
    for (target, binding) in validate::expected_phases(batch) {
        let current = crate::postgres::phase::load_with_proof(tx, &run.id, &target.id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        validate::phase(run, &current, target, binding, true)?;
    }
    for frozen in &batch.phases {
        let old = crate::postgres::phase::load_with_proof(tx, &run.id, &frozen.previous.id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        validate::phase(run, &old, &frozen.previous, &frozen.call_binding, false)?;
    }
    Ok(())
}
async fn calls(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    batch: &crate::ToolContinuationSuspension,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.charge(8).map_err(unknown)?;
    let count: i64 = sqlx::query_scalar(CALL_COUNT)
        .bind(&run.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if usize::try_from(count).map_err(unknown)? != batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    for frozen in &batch.calls {
        super::super::proof::charge(
            tx,
            super::approval::CALL_COST,
            &run.id.0,
            &frozen.target.call_id.0,
            proof,
        )
        .await?;
        let row = sqlx::query(super::approval::CALL)
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
        if row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some("pending")
        {
            return Err(Error::StaleOwner);
        }
        validate::call(run, &call, frozen)?;
        proof.charge(8).map_err(unknown)?;
        let later: bool = sqlx::query_scalar(LATER_PHASE)
            .bind(&run.id.0)
            .bind(&frozen.target.call_id.0)
            .bind(i64::try_from(frozen.target.generation.get()).map_err(unknown)?)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
        if later {
            return Err(Error::StaleOwner);
        }
    }
    Ok(())
}
const BLOCKED: &str = "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)";
pub(super) const PHASE_COUNT: &str = "SELECT COUNT(*) FROM (SELECT 1 FROM tool_phases WHERE run_id=$1 AND attempt_id=$2 LIMIT 65) AS bounded";
// Only a bounded readable status carrier agreeing with the terminal route is ignored.
// No historical outputs are transferred; this does not bound database scan work.
pub(super) const CALL_COUNT: &str = "SELECT COUNT(*) FROM (SELECT 1 FROM tool_calls WHERE run_id=$1 AND NOT(CASE WHEN octet_length(snapshot_record)<=4194304 THEN COALESCE(status IN ('completed','failed','interrupted') AND snapshot_record::json->>'status'=status,FALSE) ELSE FALSE END) LIMIT 65) AS bounded";
pub(super) const LATER_PHASE: &str =
    "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND call_id=$2 AND generation>$3)";
