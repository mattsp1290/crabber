//! Called under `MemoryStore`'s transaction lock after fence/revision/replay checks.
use super::super::{State, execution_binding};
use crate::{AttemptError as Error, attempt};
use crabber_core::Run;

pub(super) fn check(
    state: &State,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let binding = execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::InvalidTransition)?;
    check_with_binding(state, run, &binding, proof)
}
pub(super) fn check_with_binding(
    state: &State,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let count = attempt::pristine::input_count(run, binding)?;
    let controls = super::super::run_control::accepted_event_count(state, run, proof)?;
    if state
        .messages
        .iter()
        .filter(|message| message.run_id.as_ref() == Some(&run.id))
        .take(count + 1)
        .count()
        != count
        || state
            .events
            .iter()
            .filter(|event| event.run_id == run.id)
            .take(controls + 1)
            .count()
            != controls
        || state.calls.values().any(|call| call.run_id == run.id)
        || state
            .phases
            .values()
            .any(|phase| phase.snapshot.run_id == run.id)
        || state
            .attempts
            .values()
            .any(|attempt| attempt.snapshot.run_id == run.id)
        || state
            .epochs
            .values()
            .any(|epoch| epoch.run_id == run.id && epoch.id != run.epoch_id)
    {
        return Err(Error::InvalidTransition);
    }
    Ok(())
}
