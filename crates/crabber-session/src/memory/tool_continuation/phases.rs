use super::{Error, Run, State, ToolContinuationRecord, ToolPhaseResume};
use crate::native_enrollment::caller::{self, tool::IssuedPhases};
use crate::public_session::v2::proof::Proof;
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
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<crate::FrontendPhaseResume>, Error> {
    claim.validate_for_batch(&record.batch)?;
    let mut issued = IssuedPhases::new(record.batch.frontend.len(), proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        proof
            .measure(frozen)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let native = PhaseRecord::claim_frontend_result(run, parent, call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        caller::measure_phase_record(&native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        caller::measure_authority(Some(authority), proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        proof
            .measure(frozen)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let result = claim
            .results
            .iter()
            .find(|r| r.call_id() == &frozen.target.call_id)
            .ok_or(Error::SemanticConflict)?;
        proof
            .measure(result)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        issued.grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native
                .authority(authority.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            result: result.clone(),
            frozen: frozen.clone(),
        });
        state
            .phases
            .insert(frozen.target.id.clone(), native.clone());
        issued.records.push(native);
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
    Ok(issued)
}
pub(super) fn claim(
    state: &mut State,
    run: &Run,
    parent: &AttemptRecord,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<ToolPhaseResume>, Error> {
    let mut issued = IssuedPhases::new(record.batch.phases.len(), proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
        caller::measure_phase_record(old, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        proof
            .measure(frozen)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let native = old
            .resume_checkpoint(run, parent, call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        caller::measure_phase_record(&native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        issued
            .grants
            .push(resume(&native, authority, record, frozen, proof)?);
        state.phases.insert(frozen.next.id.clone(), native.clone());
        issued.records.push(native);
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
    Ok(issued)
}
pub(super) fn reclaim(
    state: &mut State,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<ToolPhaseResume>, Error> {
    let mut issued = IssuedPhases::new(record.batch.phases.len(), proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    for frozen in &record.batch.phases {
        let native = state
            .phases
            .get_mut(&frozen.next.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        caller::measure_phase_record(native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        issued
            .grants
            .push(resume(native, authority, record, frozen, proof)?);
        caller::measure_phase_record(native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        issued.records.push(native.clone());
    }
    Ok(issued)
}
pub(super) fn reclaim_frontend(
    state: &mut State,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<crate::FrontendPhaseResume>, Error> {
    let claim = record
        .frontend_claim
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    claim.validate_for_batch(&record.batch)?;
    let mut issued = IssuedPhases::new(record.batch.frontend.len(), proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    for frozen in &record.batch.frontend {
        let native = state
            .phases
            .get_mut(&frozen.target.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        caller::measure_phase_record(native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        caller::measure_authority(Some(authority), proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        proof
            .measure(frozen)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let result = claim
            .results
            .iter()
            .find(|r| r.call_id() == &frozen.target.call_id)
            .ok_or(Error::SemanticConflict)?;
        proof
            .measure(result)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        issued.grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native
                .authority(authority.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            result: result.clone(),
            frozen: frozen.clone(),
        });
        caller::measure_phase_record(native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        issued.records.push(native.clone());
    }
    Ok(issued)
}
fn resume(
    native: &PhaseRecord,
    parent: &AttemptAuthority,
    record: &ToolContinuationRecord,
    frozen: &FrozenToolPhase,
    proof: &mut Proof,
) -> Result<ToolPhaseResume, Error> {
    caller::measure_phase_record(native, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    caller::measure_authority(Some(parent), proof).map_err(|_| Error::UnknownStoreOutcome)?;
    proof
        .measure(&(&frozen.next, &frozen.identity, &frozen.checkpoint))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(ToolPhaseResume {
        target: frozen.next.clone(),
        authority: native
            .authority(parent.clone())
            .map_err(|_| Error::UnknownStoreOutcome)?,
        identity: frozen.identity.clone(),
        checkpoint: frozen.checkpoint.clone(),
        responses: record.responses_with_proof(frozen, proof)?,
    })
}
