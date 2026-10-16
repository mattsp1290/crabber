//! Fresh termination proof only; public aggregate reads do not load these carriers.
use super::{super::State, Error};
use crate::{PhaseReceipt, phase::PhaseRecord};
use crabber_core::{EventKind, EventRecord, Run, ToolCallRecord};

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
    if crate::snapshot::encoded_bytes(settled, 4_198_400).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::approval::terminal_history::result(run, next, call, current, terminal, settled)
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
