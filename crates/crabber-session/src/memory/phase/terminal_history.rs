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
    validate_with_proof(
        state,
        run,
        next,
        call,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
}
pub(in super::super) fn validate_with_proof(
    state: &State,
    run: &Run,
    next: &PhaseRecord,
    call: &ToolCallRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let current = source(state, run, call, false, proof)?;
    let terminal = source(state, run, call, true, proof)?;
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
    proof
        .measure(settled)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::phase::terminal_history::verify(run, next, call, current, terminal, settled)
}
fn source<'a>(
    state: &'a State,
    run: &Run,
    call: &ToolCallRecord,
    terminal: bool,
    proof: &mut crate::public_session::v2::proof::Proof,
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
    proof
        .measure(event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
    proof
        .measure(receipt)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok((receipt, event))
}
