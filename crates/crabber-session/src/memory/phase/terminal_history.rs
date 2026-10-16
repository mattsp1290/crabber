//! Bounded immutable result evidence for an exact native terminal phase.
use super::{super::State, Error};
use crate::{PhaseReceipt, phase::PhaseRecord};
use crabber_core::{EventKind, EventRecord, Run, ToolCallRecord};
pub(in super::super) fn validate(
    state: &State,
    run: &Run,
    next: &PhaseRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    let current = source(state, run, call, false)?;
    let terminal = source(state, run, call, true)?;
    let settled = state
        .events
        .iter()
        .rev()
        .find(|e| {
            e.session_id == run.session_id && e.run_id == run.id && e.cursor < terminal.1.cursor
        })
        .ok_or(Error::UnknownStoreOutcome)?;
    if crate::snapshot::encoded_bytes(
        settled,
        crate::phase::terminal_history::MAX_SETTLED_EVENT_BYTES,
    )
    .is_none()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::phase::terminal_history::verify(run, next, call, current, terminal, settled)
}
fn source<'a>(
    state: &'a State,
    run: &Run,
    call: &ToolCallRecord,
    terminal: bool,
) -> Result<(&'a PhaseReceipt, &'a EventRecord), Error> {
    let event = state
        .events
        .iter()
        .rev()
        .find(|e| {
            e.session_id == run.session_id
                && e.run_id == run.id
                && e.kind == EventKind::ToolPhaseChanged
                && e.payload
                    .pointer("/phase/target/call_id")
                    .and_then(|v| v.as_str())
                    == Some(&call.id.0)
                && (!terminal
                    || matches!(
                        e.payload.get("operation").and_then(|v| v.as_str()),
                        Some("complete" | "resolve_success" | "resolve_failure")
                    ))
        })
        .ok_or(Error::UnknownStoreOutcome)?;
    if crate::snapshot::encoded_bytes(event, 65536).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    let id = event
        .correlation
        .as_deref()
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::attempt::validate_identifier(id).map_err(|_| Error::UnknownStoreOutcome)?;
    let saved = state
        .operation_receipts
        .get(&(run.session_id.clone(), crabber_core::ControlId(id.into())))
        .ok_or(Error::UnknownStoreOutcome)?;
    let super::super::OperationReceipt::Phase(receipt) = saved else {
        return Err(Error::UnknownStoreOutcome);
    };
    if crate::snapshot::encoded_bytes(receipt, 8192).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((receipt, event))
}
