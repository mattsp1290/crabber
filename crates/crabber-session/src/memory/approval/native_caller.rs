//! Shared original caller Proof through actual Approval compound bodies.
use super::{Error, State};
use crate::ApprovalClaimOutcome;
use crate::native_enrollment::caller::{
    Approval, ApprovalCompletion, ApprovalInput, ApprovalResult, Caller, Fresh,
};
use crate::public_session::v2::proof::Proof;

pub(super) fn digest(
    state: &State,
    input: ApprovalInput<'_>,
    proof: &mut Proof,
) -> Result<crabber_core::InputFingerprint, Error> {
    let original = input.original();
    let exists = state
        .operation_receipts
        .keys()
        .any(|(session, id)| *session == original.session_id && id == input.id());
    if !exists {
        input
            .measure(proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    input.fingerprint()
}
pub(in crate::memory) fn selected(
    state: &State,
    input: ApprovalInput<'_>,
    proof: &mut Proof,
) -> Result<(), Error> {
    let original = input.original();
    let run = state
        .runs
        .get(&original.run_id)
        .filter(|r| r.session_id == original.session_id)
        .ok_or(Error::NotFound)?;
    proof.measure(run).map_err(|_| Error::UnknownStoreOutcome)?;
    super::current::session_with_proof(state, &run.session_id, proof)
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?;
    super::super::tool_continuation::pending_in_session_with_proof(state, &run.session_id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    for parent in state
        .attempts
        .values()
        .filter(|p| p.snapshot.run_id == run.id)
    {
        crate::native_enrollment::caller::measure_record(parent, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    for call in state.calls.values().filter(|c| c.run_id == run.id) {
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    for phase in state
        .phases
        .values()
        .filter(|p| p.snapshot.run_id == run.id)
    {
        crate::native_enrollment::caller::measure_phase_record(phase, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    super::super::native_parent::source_protocol::current(state, &run.session_id, &run.id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::super::native_enrollment::original(state, &run.session_id, &run.id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(())
}
pub(super) fn before(
    state: &State,
    input: ApprovalInput<'_>,
    proof: &mut Proof,
) -> Result<Option<(ApprovalClaimOutcome, bool)>, Error> {
    let digest = digest(state, input, proof)?;
    let original = input.original();
    if let Some(saved) = super::replay_with_proof(
        state,
        &original.session_id,
        &original.run_id,
        input.id(),
        &digest,
        proof,
    )? {
        if let ApprovalInput::Recovery(request) = input {
            request.verify_receipt(&saved)?;
        }
        return Ok(Some((ApprovalClaimOutcome::Replayed(saved), false)));
    }
    selected(state, input, proof)?;
    Ok(None)
}
pub(super) fn observe(
    state: &State,
    caller: Caller,
    completed: ApprovalCompletion<'_>,
    outcome: &ApprovalClaimOutcome,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<(), Error> {
    let ApprovalClaimOutcome::Applied { receipt, grant } = outcome else {
        return Err(Error::UnknownStoreOutcome);
    };
    let current = state
        .runs
        .get(&completed.previous.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let parent = state
        .attempts
        .get(&completed.native_receipt.attempt.target.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let native_event = state
        .events
        .get(completed.native_receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    super::super::native_enrollment::record_raw_native(
        state,
        caller,
        &Fresh::Approval(Approval {
            input: completed.input,
            previous: completed.previous,
            activated: completed.activated,
            current,
            record: completed.record,
            parent,
            native_request: completed.native_request,
            native_receipt: completed.native_receipt,
            native_event,
            receipt,
            event,
            grant,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .map_err(|_| Error::UnknownStoreOutcome)
}

pub(super) fn issue(
    state: &State,
    caller: Caller,
    completed: ApprovalCompletion<'_>,
    parts: ApprovalResult,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<crate::ApprovalClaimOutcome, Error> {
    let outcome = completed
        .issue(parts, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    observe(
        state,
        caller,
        completed,
        &outcome,
        proof,
        #[cfg(test)]
        probe,
    )?;
    Ok(outcome)
}

/// Actual selected carrier, charged before this independent same-lock copy.
pub(super) fn selected_run(
    state: &State,
    input: ApprovalInput<'_>,
    proof: &mut Proof,
) -> Result<crabber_core::Run, Error> {
    let original = input.original();
    let run = state
        .runs
        .get(&original.run_id)
        .filter(|r| r.session_id == original.session_id)
        .ok_or(Error::NotFound)?;
    proof.measure(run).map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(run.clone())
}
