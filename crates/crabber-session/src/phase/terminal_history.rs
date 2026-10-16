//! Immutable native terminal receipts and settlement authenticate private outputs.
use crate::PhaseError as Error;
use crate::{PhaseOperation, PhaseReceipt};
use crabber_core::{EventKind, EventRecord, Run, ToolCallRecord};

// ToolCallSettled repeats the logical call's name in both `name` and `tool`.
// Twice the 4 MiB persisted call budget also covers JSONB formatting overhead;
// the extra 16 KiB covers bounded scope/presentation IDs and the event envelope.
pub(crate) const MAX_SETTLED_EVENT_BYTES: usize = 2 * 4_194_304 + 16_384;

pub(crate) fn verify(
    run: &Run,
    next: &crate::phase::PhaseRecord,
    call: &ToolCallRecord,
    current: (&PhaseReceipt, &EventRecord),
    terminal: (&PhaseReceipt, &EventRecord),
    settled: &EventRecord,
) -> Result<(), Error> {
    verify_scope(
        &run.session_id,
        &run.id,
        next,
        call,
        current,
        terminal,
        settled,
    )
}
pub(crate) fn verify_scope(
    session: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    next: &crate::phase::PhaseRecord,
    call: &ToolCallRecord,
    current: (&PhaseReceipt, &EventRecord),
    terminal: (&PhaseReceipt, &EventRecord),
    settled: &EventRecord,
) -> Result<(), Error> {
    let quiescence = verify_history_scope(session, run_id, next, call, current, terminal, settled)?;
    if quiescence != crate::PhaseQuiescence::Complete {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
/// Authenticate the immutable terminal result and report truthful cleanup state.
/// Unlike retirement validation, a history reader may classify retained cleanup
/// as an unsafe copy boundary after proving the result itself is coherent.
pub(crate) fn verify_history_scope(
    session: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    next: &crate::phase::PhaseRecord,
    call: &ToolCallRecord,
    current: (&PhaseReceipt, &EventRecord),
    terminal: (&PhaseReceipt, &EventRecord),
    settled: &EventRecord,
) -> Result<crate::PhaseQuiescence, Error> {
    let quiescence =
        crate::attempt_control::validate_phase_history_scope(session, run_id, call, next)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    for (receipt, event) in [current, terminal] {
        crate::phase::verify_receipt(receipt, event).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let (now, _) = current;
    let (done, _) = terminal;
    let logical = call.result.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    if now.phase != next.snapshot
        || done.phase.target != next.snapshot.target
        || done.phase.session_id != *session
        || done.phase.run_id != *run_id
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
        || settled.session_id != *session
        || settled.run_id != *run_id
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
    Ok(quiescence)
}
