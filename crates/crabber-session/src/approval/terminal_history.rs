//! Evidence for a consumed tool checkpoint; mutable terminal labels are insufficient.
use crate::ApprovalError as Error;
use crate::{PhaseOperation, PhaseQuiescence, PhaseReceipt, PhaseState, ToolContinuationState};
use crabber_core::{EventKind, EventRecord, Run, ToolCallRecord};

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

pub(crate) fn result(
    run: &Run,
    next: &crate::phase::PhaseRecord,
    call: &ToolCallRecord,
    current: (&PhaseReceipt, &EventRecord),
    terminal: (&PhaseReceipt, &EventRecord),
    settled: &EventRecord,
) -> Result<(), Error> {
    for (receipt, event) in [current, terminal] {
        crate::phase::verify_receipt(receipt, event).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let (now, _) = current;
    let (done, _) = terminal;
    let logical = call.result.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    if now.phase != next.snapshot
        || done.phase.target != next.snapshot.target
        || done.phase.session_id != run.session_id
        || done.phase.run_id != run.id
        || done.phase.call_binding != next.snapshot.call_binding
        || done.phase.state != next.snapshot.state
        || !matches!(
            done.operation,
            PhaseOperation::Complete
                | PhaseOperation::ResolveSuccess
                | PhaseOperation::ResolveFailure
        )
        || done.event_cursor > now.event_cursor
        || settled
            .cursor
            .is_none_or(|cursor| cursor >= done.event_cursor)
        || settled.kind != EventKind::ToolCallSettled
        || settled.live_only
        || settled.session_id != run.session_id
        || settled.run_id != run.id
        || settled.payload.get("call_id") != Some(&serde_json::json!(call.id))
        || settled.correlation.as_deref() != Some(&call.id.0)
        || settled.payload.get("content") != Some(&serde_json::json!(logical.content))
        || settled.payload.get("status") != Some(&serde_json::json!(logical.status))
        || settled.payload.get("is_error")
            != Some(&serde_json::json!(
                next.output
                    .as_ref()
                    .ok_or(Error::UnknownStoreOutcome)?
                    .is_error()
            ))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
