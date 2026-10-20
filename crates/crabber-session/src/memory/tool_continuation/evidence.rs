//! Exact native ancestry and terminal/private evidence for every run phase.
use super::{Error, PhaseQuiescence, PhaseState, Run, State, ToolContinuationSuspension};
use crate::public_session::v2::proof::Proof;
use crabber_core::ToolCallStatus;
pub(super) fn validate(
    state: &State,
    run: &Run,
    selected: Option<&ToolContinuationSuspension>,
) -> Result<(), Error> {
    validate_with_proof(state, run, selected, &mut Proof::legacy())
}
pub(super) fn validate_with_proof(
    state: &State,
    run: &Run,
    selected: Option<&ToolContinuationSuspension>,
    proof: &mut Proof,
) -> Result<(), Error> {
    if let Some(selected) = selected {
        proof
            .measure(&selected.phases)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let exceptions = selected
        .map(|batch| {
            batch
                .phases
                .iter()
                .map(|phase| phase.previous.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    validate_excluding_with_proof(state, run, &exceptions, proof)
}
pub(super) fn validate_excluding_with_proof(
    state: &State,
    run: &Run,
    exceptions: &[crate::PhaseTarget],
    proof: &mut Proof,
) -> Result<(), Error> {
    for call in state.calls.values().filter(|call| call.run_id == run.id) {
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if !matches!(
            call.status,
            ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
        ) {
            crate::attempt_control::validate_logical(call)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    }
    for phase in state
        .phases
        .values()
        .filter(|phase| phase.snapshot.run_id == run.id)
    {
        crate::native_enrollment::caller::measure_phase_record(phase, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        phase.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        let parent = state
            .attempts
            .get(&phase.snapshot.target.attempt.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::native_enrollment::caller::measure_record(parent, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        parent.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        if parent.snapshot.target != phase.snapshot.target.attempt
            || parent.snapshot.run_id != run.id
            || parent.snapshot.session_id != run.session_id
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let call = state
            .calls
            .get(&phase.snapshot.target.call_id)
            .filter(|call| call.run_id == run.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
            != phase.snapshot.call_binding
            || phase.snapshot.session_id != run.session_id
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if exceptions.contains(&phase.snapshot.target) {
            continue;
        }
        if phase.snapshot.quiescence != PhaseQuiescence::Complete
            || matches!(
                phase.snapshot.state,
                PhaseState::Claimed | PhaseState::Started | PhaseState::Uncertain
            )
        {
            return Err(Error::CleanupBlocked);
        }
        if phase.snapshot.state == PhaseState::Suspended {
            let origin = state
                .tool_continuations
                .values()
                .find(|record| {
                    record.run_id == run.id
                        && record
                            .batch
                            .phases
                            .iter()
                            .any(|frozen| frozen.previous == phase.snapshot.target)
                })
                .ok_or(Error::UnknownStoreOutcome)?;
            let origin =
                super::checked_record_with_proof(state, &origin.batch.continuation.id, proof)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            if !origin.batch.phases.iter().any(|frozen| {
                frozen.previous == phase.snapshot.target
                    && frozen.call_binding == phase.snapshot.call_binding
            }) {
                return Err(Error::UnknownStoreOutcome);
            }
        } else {
            super::super::phase::terminal_history::validate_with_proof(
                state, run, phase, call, proof,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    }
    Ok(())
}
