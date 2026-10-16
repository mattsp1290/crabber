use super::{Error, ToolContinuationRecord, unknown};
use crate::{AttemptAuthority, ToolPhaseResume};
use crabber_core::{Run, ToolCallStatus};
use sqlx::{Postgres, Transaction};
pub(super) async fn claim(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    activated: &Run,
    parent: &crate::attempt::AttemptRecord,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
) -> Result<Vec<ToolPhaseResume>, Error> {
    let mut phases = Vec::new();
    for frozen in &record.batch.phases {
        let old = super::super::phase::load_host_record(tx, &run.id, &frozen.previous.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        let mut call = super::super::phase::load_call(tx, run, &frozen.previous.call_id)
            .await
            .map_err(unknown)?;
        let native = old
            .resume_checkpoint(activated, parent, &call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        let capability = native.authority(authority.clone()).map_err(unknown)?;
        phases.push(ToolPhaseResume {
            target: frozen.next.clone(),
            authority: capability,
            identity: frozen.identity.clone(),
            checkpoint: frozen.checkpoint.clone(),
            responses: record.responses(frozen)?,
        });
        super::super::phase::save_host_record(tx, &native, true)
            .await
            .map_err(unknown)?;
        call.status = ToolCallStatus::Pending;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
    }
    Ok(phases)
}
pub(super) async fn recover(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
) -> Result<Vec<ToolPhaseResume>, Error> {
    let mut phases = Vec::new();
    for frozen in &record.batch.phases {
        let mut native = super::super::phase::load_host_record(tx, &run.id, &frozen.next.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        phases.push(ToolPhaseResume {
            target: frozen.next.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            identity: frozen.identity.clone(),
            checkpoint: frozen.checkpoint.clone(),
            responses: record.responses(frozen)?,
        });
        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;
    }
    Ok(phases)
}

pub(super) async fn suspend(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    parent: &crate::attempt::AttemptRecord,
    batch: &crate::ToolContinuationSuspension,
    authority: &crate::ToolSuspensionAuthority,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    for (frozen, capability) in batch.phases.iter().zip(&authority.phases) {
        let mut call = super::super::phase::load_call(tx, run, &frozen.previous.call_id)
            .await
            .map_err(unknown)?;

        let mut native = super::super::phase::load_host_record(tx, &run.id, &frozen.previous.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;

        native
            .suspend_checkpoint(run, parent, &call, frozen, capability, now)
            .map_err(|_| Error::StaleOwner)?;

        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;

        call.status = ToolCallStatus::Waiting;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
    }

    super::super::frontend::mark_waiting(tx, run, batch).await?;
    Ok(())
}

pub(super) async fn claim_frontend(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    parent: &crate::attempt::AttemptRecord,
    record: &ToolContinuationRecord,
    claim: &crate::FrontendContinuationClaim,
    authority: &AttemptAuthority,
) -> Result<Vec<crate::FrontendPhaseResume>, Error> {
    claim.validate_for_batch(&record.batch)?;
    let mut grants = Vec::new();
    for frozen in &record.batch.frontend {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE id=$1 OR (run_id=$2 AND call_id=$3))",
        )
        .bind(&frozen.target.id.0)
        .bind(&run.id.0)
        .bind(&frozen.target.call_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
        if exists {
            return Err(Error::CleanupBlocked);
        }
        let mut call = super::super::phase::load_call(tx, run, &frozen.target.call_id)
            .await
            .map_err(unknown)?;
        let native = crate::phase::PhaseRecord::claim_frontend_result(run, parent, &call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            result: claim
                .results
                .iter()
                .find(|result| result.call_id() == &frozen.target.call_id)
                .ok_or(Error::SemanticConflict)?
                .clone(),
            frozen: frozen.clone(),
        });
        super::super::phase::save_host_record(tx, &native, true)
            .await
            .map_err(unknown)?;
        call.status = ToolCallStatus::Pending;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
    }
    Ok(grants)
}

pub(super) async fn recover_frontend(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
) -> Result<Vec<crate::FrontendPhaseResume>, Error> {
    let claim = record
        .frontend_claim
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    claim.validate_for_batch(&record.batch)?;
    let mut grants = Vec::new();
    for frozen in &record.batch.frontend {
        let mut native = super::super::phase::load_host_record(tx, &run.id, &frozen.target.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            result: claim
                .results
                .iter()
                .find(|r| r.call_id() == &frozen.target.call_id)
                .ok_or(Error::UnknownStoreOutcome)?
                .clone(),
            frozen: frozen.clone(),
        });
        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;
    }
    Ok(grants)
}
