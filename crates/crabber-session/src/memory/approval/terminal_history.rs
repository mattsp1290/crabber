//! Fresh termination proof only; public aggregate reads do not load these carriers.
use super::{super::State, Error};
use crate::phase::PhaseRecord;
use crabber_core::{Run, ToolCallRecord};

pub(super) fn validate(
    state: &State,
    run: &Run,
    old: &PhaseRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    let origin = state
        .tool_continuations
        .values()
        .find(|r| {
            r.run_id == run.id
                && r.batch
                    .phases
                    .iter()
                    .any(|p| p.previous == old.snapshot.target)
        })
        .ok_or(Error::UnknownStoreOutcome)?;
    let origin =
        super::super::tool_continuation::checked_record(state, &origin.batch.continuation.id)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    let next = state
        .phases
        .values()
        .filter(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == call.id)
        .max_by_key(|p| p.snapshot.target.generation)
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::approval::terminal_history::origin(run, old, next, &origin, call)?;
    super::super::phase::terminal_history::validate(state, run, next, call)
        .map_err(|_| Error::UnknownStoreOutcome)
}
