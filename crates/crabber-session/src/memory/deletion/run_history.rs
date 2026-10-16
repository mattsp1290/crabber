//! Actual original input and terminal domains on the readiness allowance.
//! These authenticate history; no provider declaration becomes resource coverage.
#[path = "run_history/abandonment.rs"]
mod abandonment;
#[path = "run_history/approval.rs"]
mod approval;
#[path = "run_history/claims.rs"]
mod claims;
#[path = "run_history/waiting.rs"]
mod waiting;
use super::super::{OperationReceipt, State};
use crate::deletion::history::RunHistoryReport;
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
        unproven |= !report(state, run, proof)?.complete();
    }
    Ok(unproven)
}
fn report(state: &State, run: &Run, proof: &mut Proof) -> Result<RunHistoryReport, Failure> {
    let unkeyed_input = input(state, run, proof)?;
    let (initial_present, claims) = claims::read(state, &run.session_id, &run.id, proof)?;
    let terminal = terminal(state, run, proof)?;
    let approved = approval::verify(state, run, proof)?;
    let cancelled = waiting::verify(state, run, proof)?;
    let abandoned = abandonment::verify(state, run, proof)?;
    Ok(RunHistoryReport::sources(
        initial_present
            && state.preparation_admissions.contains_key(&run.id)
            && (state.admission_execution_protocols.contains(&run.id) || unkeyed_input),
        claims,
        terminal || approved || abandoned || cancelled,
    ))
}

fn input(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let binding =
        super::super::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
            .map_err(ReadError::public)?;
    if let Some(original) = state
        .preparation_admissions
        .get(&run.id)
        .filter(|w| w.is_unkeyed())
    {
        return unkeyed_input(state, run, original, binding.as_ref(), proof);
    }
    let Some(record) = state.admission_executions.get(&run.id) else {
        return Ok(false);
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
    record.validate_messages(run, &messages).map_err(unknown)?;
    Ok(false)
}
fn unkeyed_input(
    state: &State,
    run: &Run,
    original: &crate::session_owner::preparation::admission::Witness,
    binding: Option<&crate::RetainedExecutionBinding>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    proof.measure(original)?;
    if original.session_id() != &run.session_id || original.run_id() != &run.id {
        return Err(unknown("original unkeyed input scope"));
    }
    super::owned::original_admission(state, original, proof)?;
    proof.charge(1)?;
    if state.admission_executions.contains_key(&run.id)
        || state.admission_execution_protocols.contains(&run.id)
        || state
            .admission_execution_sources
            .keys()
            .any(|(id, _)| id == &run.id)
        || state
            .receipts
            .values()
            .any(|receipt| receipt.run_id == run.id)
    {
        return Err(unknown("mixed unkeyed/keyed original input"));
    }
    let binding = binding.ok_or_else(|| unknown("original unkeyed binding missing"))?;
    original
        .verify_unkeyed_binding(&binding.capsule)
        .map_err(unknown)?;
    Ok(true)
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
