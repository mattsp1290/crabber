//! Exact native ancestry and terminal/private evidence for every run phase.
use super::{
    Error, PhaseQuiescence, PhaseState, Run, State, ToolContinuationSuspension, checked_record,
};
use crabber_core::ToolCallStatus;
pub(super) fn validate(
    state: &State,
    run: &Run,
    selected: Option<&ToolContinuationSuspension>,
) -> Result<(), Error> {
    let exceptions = selected
        .map(|batch| {
            batch
                .phases
                .iter()
                .map(|phase| phase.previous.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    validate_excluding(state, run, &exceptions)
}
pub(super) fn validate_excluding(
    state: &State,
    run: &Run,
    exceptions: &[crate::PhaseTarget],
) -> Result<(), Error> {
    for call in state.calls.values().filter(|call| call.run_id == run.id) {
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
        phase.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        let parent = state
            .attempts
            .get(&phase.snapshot.target.attempt.id)
            .ok_or(Error::UnknownStoreOutcome)?;
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
            let origin = checked_record(state, &origin.batch.continuation.id)?;
            if !origin.batch.phases.iter().any(|frozen| {
                frozen.previous == phase.snapshot.target
                    && frozen.call_binding == phase.snapshot.call_binding
            }) {
                return Err(Error::UnknownStoreOutcome);
            }
        } else {
            crate::attempt_control::validate_phase(run, call, phase)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    }
    Ok(())
}
