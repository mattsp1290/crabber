//! Actual original input and terminal domains on the readiness allowance.
//! These authenticate history; no provider declaration becomes resource coverage.
#[path = "run_history/abandonment.rs"]
mod abandonment;
use super::super::{OperationReceipt, State};
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crabber_core::{EventKind, Run, SessionId};

/// True means at least one initial/terminal source package is unproven.
/// Even false supplies no physical resource or noninitial claim coverage.
pub(super) fn verify(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    // owned::verify has authenticated all witness/Init/Begin membership before
    // these validators canonicalize the retained original input.
    let mut unproven = false;
    for run in state.runs.values().filter(|run| &run.session_id == session) {
        input(state, run, proof)?;
        let terminal = terminal(state, run, proof)?;
        let abandoned = abandonment::verify(state, run, proof)?;
        unproven |= !(state.preparation_admissions.contains_key(&run.id)
            && state.admission_execution_protocols.contains(&run.id)
            && (terminal || abandoned));
    }
    Ok(unproven)
}
fn input(state: &State, run: &Run, proof: &mut Proof) -> Result<(), Failure> {
    super::super::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
        .map_err(ReadError::public)?;
    let Some(record) = state.admission_executions.get(&run.id) else {
        return Ok(());
    };
    proof.measure(record)?;
    proof.measure(&(&run.config_hash, &run.plan_fingerprint, &run.owner))?;
    let original = state
        .receipts
        .get(&(run.session_id.clone(), record.key.clone()))
        .ok_or_else(|| unknown("original admission receipt"))?;
    proof.measure(original)?;
    if original != &record.receipt {
        return Err(unknown("original execution receipt"));
    }
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let selected = || {
        state
            .messages
            .iter()
            .filter(|message| {
                message.run_id.as_ref() == Some(&run.id) && message.session_id == run.session_id
            })
            .take(count)
    };
    for message in selected() {
        proof.measure(message)?;
    }
    let messages = selected().cloned().collect::<Vec<_>>();
    record.validate_messages(run, &messages).map_err(unknown)
}
fn terminal(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let Some(record) = state
        .attempts
        .values()
        .filter(|record| record.snapshot.run_id == run.id)
        .max_by_key(|record| record.snapshot.target.generation)
    else {
        return Ok(false);
    };
    proof.measure(&record.snapshot)?;
    let event = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == EventKind::ExecutionAttemptChanged
                && event
                    .payload
                    .pointer("/attempt/target/id")
                    .and_then(serde_json::Value::as_str)
                    == Some(&record.snapshot.target.id.0)
        })
        .ok_or_else(|| unknown("latest terminal attempt source"))?;
    proof.measure(event)?;
    if !crate::attempt::terminal::candidate(event) {
        return Ok(false);
    }
    let operation = event
        .correlation
        .as_ref()
        .ok_or_else(|| unknown("terminal operation"))?;
    let Some(OperationReceipt::Attempt(receipt)) = state.operation_receipts.get(&(
        run.session_id.clone(),
        crabber_core::ControlId(operation.clone()),
    )) else {
        return Err(unknown("original terminal receipt"));
    };
    proof.measure(receipt)?;
    if receipt.attempt != record.snapshot || event.cursor != Some(receipt.event_cursor) {
        return Err(unknown("current terminal attempt"));
    }
    let original = crate::attempt::terminal::settlement(receipt, event).map_err(unknown)?;
    let source = state
        .events
        .get(original.cursor.ok_or_else(|| unknown("terminal cursor"))?)
        .ok_or_else(|| unknown("original terminal settlement"))?;
    proof.measure(source)?;
    // This verifier compares actual private error/usage and independently latest
    // same-run settlement before the original audit, without fabricating a Run.
    proof.measure(&(&run.error, &run.usage, run.status))?;
    super::super::attempt::verify(state, receipt).map_err(unknown)?;
    Ok(true)
}

#[cfg(test)]
#[path = "run_history_tests.rs"]
mod tests;
