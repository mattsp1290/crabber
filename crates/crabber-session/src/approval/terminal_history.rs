//! Evidence for a consumed tool checkpoint; mutable terminal labels are insufficient.
use crate::ApprovalError as Error;
use crate::{PhaseQuiescence, PhaseState, ToolContinuationState};
use crabber_core::{Run, ToolCallRecord};

pub(crate) fn origin(
    run: &Run,
    old: &crate::phase::PhaseRecord,
    next: &crate::phase::PhaseRecord,
    record: &crate::tool_continuation::ToolContinuationRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    old.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if old.snapshot.state != PhaseState::Suspended
        || old.snapshot.quiescence != PhaseQuiescence::Complete
        || record.state != ToolContinuationState::Released
        || record.session_id != run.session_id
        || record.run_id != run.id
        || !record.batch.phases.iter().any(|p| {
            p.previous == old.snapshot.target && p.call_binding == old.snapshot.call_binding
        })
        || next.snapshot.target.call_id != old.snapshot.target.call_id
        || next.snapshot.target.generation <= old.snapshot.target.generation
        || next.snapshot.call_binding != old.snapshot.call_binding
        || old.snapshot.session_id != run.session_id
        || old.snapshot.run_id != run.id
    {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::approval::validate_termination_phase(run, next, call)
}
