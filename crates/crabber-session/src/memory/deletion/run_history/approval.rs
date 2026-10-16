//! Exact approval terminal source, frozen original batch, and indexed settlement.
use super::super::super::{State, approval};
use crate::approval::terminal;
use crate::attempt::terminal::TerminalFacts;
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crabber_core::{EventKind, Run};
pub(super) fn verify(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let Some((id, record)) = state
        .approvals
        .iter()
        .filter(|(_, record)| record.run_id == run.id)
        .max_by_key(|(_, record)| record.batch.continuation.generation)
    else {
        return Ok(false);
    };
    proof.measure(&(id, record))?;
    if record.session_id != run.session_id || *id != record.batch.continuation.id {
        return Err(unknown("approval terminal routing"));
    }
    approval::origin::verify_with_proof(state, run, record, proof).map_err(ReadError::public)?;
    approval::current::verify_with_proof(state, run, record, false, proof)
        .map_err(ReadError::public)?;
    let selected = approval::current::selected(state, record, false).map_err(unknown)?;
    let event = selected
        .first()
        .ok_or_else(|| unknown("latest approval terminal source"))?;
    proof.measure(event)?;
    let source =
        approval::current::source_with_proof(state, event, proof).map_err(ReadError::public)?;
    if !terminal::candidate(&source.event) {
        return Ok(false);
    }
    let saved = terminal::settlement(&source.receipt, &source.event).map_err(unknown)?;
    let cursor = saved
        .cursor
        .ok_or_else(|| unknown("approval terminal cursor"))?;
    let original = state
        .events
        .get(cursor)
        .ok_or_else(|| unknown("original approval settlement"))?;
    proof.measure(original)?;
    let latest = state.events.iter().rev().find(|event| {
        event.session_id == run.session_id
            && event.run_id == run.id
            && event.kind == EventKind::RunSettled
            && event
                .cursor
                .is_some_and(|cursor| cursor < source.receipt.event_cursor)
    });
    if latest != Some(original) {
        return Err(unknown("independent latest approval settlement"));
    }
    proof.measure(&(&run.error, &run.usage, run.status))?;
    terminal::verify_scope(
        TerminalFacts {
            session: &run.session_id,
            run: &run.id,
            status: run.status,
            error: run.error.as_deref(),
            usage: &run.usage,
        },
        &source.receipt,
        &source.event,
        original,
        &record.batch,
    )
    .map_err(unknown)?;
    Ok(true)
}

#[cfg(test)]
#[path = "approval_tests.rs"]
mod tests;
