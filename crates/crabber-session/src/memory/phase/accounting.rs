//! Same-lock selected preflight before the inherited whole-State clone.
use super::{Error, PhaseCaller, PhaseMutation, PhaseMutationOutcome, State, phase};
use crate::native_enrollment::caller;
use crate::public_session::v2::proof::Proof;
use crabber_core::RunId;
pub(super) fn selected(
    state: &State,
    run_id: &RunId,
    request: &PhaseMutation,
    caller: PhaseCaller<'_>,
    effect: phase::PhaseEffect<'_>,
    proof: &mut Proof,
) -> Result<Option<PhaseMutationOutcome>, Error> {
    let run = state.runs.get(run_id).ok_or(Error::NotFound)?;
    proof
        .measure(&(run_id, run, request))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if let Some(start) = effect.start() {
        proof
            .measure(&(&start.begin, &start.turn_id, start.created_at))
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let PhaseCaller::Worker { fence, authority } = caller {
        proof
            .measure(fence)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        match authority {
            Some(crate::PhaseWriteAuthority::Attempt(a)) => {
                caller::measure_authority(Some(a), proof)
            }
            Some(crate::PhaseWriteAuthority::Phase(a)) => {
                caller::measure_phase_authority(Some(a), proof)
            }
            None => Ok(()),
        }
        .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(saved) = state
        .operation_receipts
        .get(&(run.session_id.clone(), request.id.clone()))
    {
        let super::super::OperationReceipt::Phase(receipt) = saved else {
            return Err(Error::SemanticConflict);
        };
        proof
            .measure(receipt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if let Some(event) = state.events.get(receipt.event_cursor) {
            proof
                .measure(event)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        if let Some(call) = state.calls.get(&receipt.phase.target.call_id) {
            proof
                .measure(call)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        let digest = effect.fingerprint(request, &run.session_id, &run.id)?;
        return super::replay(state, run, request, &digest, effect);
    }
    let target = request.command.target();
    if let Some(parent) = state.attempts.get(&target.attempt.id) {
        caller::measure_record(parent, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(call) = state.calls.get(&target.call_id) {
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    for record in state.phases.values().filter(|p| {
        p.snapshot.run_id == run.id
            && (p.snapshot.target.id == target.id || p.snapshot.target.call_id == target.call_id)
    }) {
        caller::measure_phase_record(record, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    super::super::admission_journal::verify_with_proof(state, run_id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if matches!(request.command, crate::PhaseCommand::Claim { .. }) {
        super::super::frontend::origin_with_proof(state, run, &target.call_id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if matches!(request.command, crate::PhaseCommand::Begin { .. }) {
        super::super::native_parent::source_protocol::current(
            state,
            &run.session_id,
            &run.id,
            proof,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if matches!(
        request.command,
        crate::PhaseCommand::Claim { .. } | crate::PhaseCommand::Begin { .. }
    ) {
        super::super::native_enrollment::original(state, &run.session_id, &run.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    Ok(None)
}
