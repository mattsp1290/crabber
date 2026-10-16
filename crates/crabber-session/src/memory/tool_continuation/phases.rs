use super::{Error, Run, State, ToolContinuationRecord, ToolPhaseResume};
use crate::{AttemptAuthority, FrozenToolPhase, attempt::AttemptRecord, phase::PhaseRecord};
use crabber_core::ToolCallStatus;
pub(super) fn frontend_waiting(
    state: &mut State,
    run: &Run,
    batch: &crate::ToolContinuationSuspension,
) -> Result<(), Error> {
    for frontend in &batch.frontend {
        state
            .calls
            .get_mut(&frontend.target.call_id)
            .ok_or(Error::NotFound)?
            .status = ToolCallStatus::Waiting;
        *state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default() += 1;
    }
    Ok(())
}
pub(super) fn claim_frontend(
    state: &mut State,
    run: &Run,
    parent: &AttemptRecord,
    record: &ToolContinuationRecord,
    claim: &crate::FrontendContinuationClaim,
) -> Result<Vec<crate::FrontendPhaseResume>, Error> {
    claim.validate_for_batch(&record.batch)?;
    let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
    let mut grants = Vec::new();
    for frozen in &record.batch.frontend {
        if state.phases.values().any(|p| {
            p.snapshot.run_id == run.id && p.snapshot.target.call_id == frozen.target.call_id
        }) || state.phases.contains_key(&frozen.target.id)
        {
            return Err(Error::CleanupBlocked);
        }
        let call = state
            .calls
            .get(&frozen.target.call_id)
            .ok_or(Error::NotFound)?;
        let native = PhaseRecord::claim_frontend_result(run, parent, call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native
                .authority(authority.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            result: claim
                .results
                .iter()
                .find(|r| r.call_id() == &frozen.target.call_id)
                .ok_or(Error::SemanticConflict)?
                .clone(),
            frozen: frozen.clone(),
        });
        state.phases.insert(frozen.target.id.clone(), native);
        state
            .calls
            .get_mut(&frozen.target.call_id)
            .ok_or(Error::NotFound)?
            .status = ToolCallStatus::Pending;
        *state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default() += 1;
    }
    Ok(grants)
}
pub(super) fn claim(
    state: &mut State,
    run: &Run,
    parent: &AttemptRecord,
    record: &ToolContinuationRecord,
) -> Result<Vec<ToolPhaseResume>, Error> {
    let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
    let mut phases = Vec::new();
    for frozen in &record.batch.phases {
        if state.phases.contains_key(&frozen.next.id) {
            return Err(Error::SemanticConflict);
        }
        let old = state
            .phases
            .get(&frozen.previous.id)
            .ok_or(Error::NotFound)?;
        let call = state
            .calls
            .get(&frozen.previous.call_id)
            .ok_or(Error::NotFound)?;
        let native = old
            .resume_checkpoint(run, parent, call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        phases.push(resume(&native, &authority, record, frozen)?);
        state.phases.insert(frozen.next.id.clone(), native);
        state
            .calls
            .get_mut(&frozen.previous.call_id)
            .ok_or(Error::NotFound)?
            .status = ToolCallStatus::Pending;
        *state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default() += 1;
    }
    Ok(phases)
}
pub(super) fn reclaim(
    state: &mut State,
    parent: &AttemptRecord,
    record: &ToolContinuationRecord,
) -> Result<Vec<ToolPhaseResume>, Error> {
    let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
    let mut phases = Vec::new();
    for frozen in &record.batch.phases {
        let native = state
            .phases
            .get_mut(&frozen.next.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        phases.push(resume(native, &authority, record, frozen)?);
    }
    Ok(phases)
}
pub(super) fn reclaim_frontend(
    state: &mut State,
    parent: &AttemptRecord,
    record: &ToolContinuationRecord,
) -> Result<Vec<crate::FrontendPhaseResume>, Error> {
    let claim = record
        .frontend_claim
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    claim.validate_for_batch(&record.batch)?;
    let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
    let mut grants = Vec::new();
    for frozen in &record.batch.frontend {
        let native = state
            .phases
            .get_mut(&frozen.target.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native
                .authority(authority.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            result: claim
                .results
                .iter()
                .find(|r| r.call_id() == &frozen.target.call_id)
                .ok_or(Error::UnknownStoreOutcome)?
                .clone(),
            frozen: frozen.clone(),
        });
    }
    Ok(grants)
}
fn resume(
    native: &PhaseRecord,
    parent: &AttemptAuthority,
    record: &ToolContinuationRecord,
    frozen: &FrozenToolPhase,
) -> Result<ToolPhaseResume, Error> {
    Ok(ToolPhaseResume {
        target: frozen.next.clone(),
        authority: native
            .authority(parent.clone())
            .map_err(|_| Error::UnknownStoreOutcome)?,
        identity: frozen.identity.clone(),
        checkpoint: frozen.checkpoint.clone(),
        responses: record.responses(frozen)?,
    })
}
