//! Exact immutable native Begin sources and private issuance witnesses.
use super::{OperationReceipt, State};
use crate::native_parent::{Error, RunIssuance, RunState, Witness};
use crabber_core::{EventKind, EventRecord, Run};

pub(super) fn record_attempt(
    state: &mut State,
    run: &Run,
    command: &crate::AttemptCommand,
    attempt: &crate::attempt::AttemptRecord,
    receipt: &crate::AttemptReceipt,
) -> Result<(), crate::AttemptError> {
    if matches!(command, crate::AttemptCommand::Begin { .. }) {
        let Some(issuance) =
            latest_run(state, run).map_err(|_| crate::AttemptError::UnknownStoreOutcome)?
        else {
            return Ok(());
        };
        let witness = Witness::attempt(run, attempt, receipt, issuance)
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
        insert(state, witness).map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    }
    Ok(())
}
pub(super) fn record_phase(
    state: &mut State,
    run: &Run,
    phase: &crate::phase::PhaseRecord,
    receipt: &crate::PhaseReceipt,
) -> Result<(), crate::PhaseError> {
    if receipt.operation == crate::PhaseOperation::Begin {
        let attempt = state
            .attempts
            .get(&phase.snapshot.target.attempt.id)
            .ok_or(crate::PhaseError::UnknownStoreOutcome)?;
        let Some(issuance) =
            latest_run(state, run).map_err(|_| crate::PhaseError::UnknownStoreOutcome)?
        else {
            return Ok(());
        };
        let witness = Witness::phase(run, attempt, phase, receipt, issuance)
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?;
        insert(state, witness).map_err(|_| crate::PhaseError::UnknownStoreOutcome)?;
    }
    Ok(())
}
fn insert(state: &mut State, witness: Witness) -> Result<(), Error> {
    witness.validate()?;
    if state
        .native_parent_witnesses
        .contains_key(&witness.source.event_cursor)
    {
        return Err(Error);
    }
    state
        .native_parent_witnesses
        .insert(witness.source.event_cursor, witness);
    Ok(())
}
// Owner-ledger integration consumes these proofs; metadata never grants tokens.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "owner-ledger handoff integration is a separate slice"
    )
)]
pub(super) fn verify_attempt(
    state: &State,
    run: &Run,
    attempt: &crate::attempt::AttemptRecord,
) -> Result<Witness, Error> {
    let event = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == EventKind::ExecutionAttemptChanged
        })
        .ok_or(Error)?;
    let key = source_key(event)?;
    let Some(OperationReceipt::Attempt(receipt)) = state.operation_receipts.get(&key) else {
        return Err(Error);
    };
    let witness = state
        .native_parent_witnesses
        .get(&receipt.event_cursor)
        .ok_or(Error)?;
    let issuance = latest_run(state, run)?.ok_or(Error)?;
    witness.verify_attempt(run, attempt, receipt, event, issuance)?;
    Ok(witness.clone())
}
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "owner-ledger handoff integration is a separate slice"
    )
)]
pub(super) fn verify_phase(
    state: &State,
    run: &Run,
    attempt: &crate::attempt::AttemptRecord,
    phase: &crate::phase::PhaseRecord,
) -> Result<Witness, Error> {
    // Authenticate the live parent too; its token issuance may have changed
    // without changing the public target across an Unstarted reclaim.
    verify_attempt(state, run, attempt)?;
    let event = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == EventKind::ToolPhaseChanged
                && event
                    .payload
                    .pointer("/phase/target/call_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(phase.snapshot.target.call_id.0.as_str())
        })
        .ok_or(Error)?;
    let key = source_key(event)?;
    let Some(OperationReceipt::Phase(receipt)) = state.operation_receipts.get(&key) else {
        return Err(Error);
    };
    let witness = state
        .native_parent_witnesses
        .get(&receipt.event_cursor)
        .ok_or(Error)?;
    let issuance = latest_run(state, run)?.ok_or(Error)?;
    witness.verify_phase(run, attempt, phase, receipt, event, issuance)?;
    Ok(witness.clone())
}
fn source_key(
    event: &EventRecord,
) -> Result<(crabber_core::SessionId, crabber_core::ControlId), Error> {
    if crate::snapshot::encoded_bytes(event, 65536).is_none() {
        return Err(Error);
    }
    let id = event.correlation.as_deref().ok_or(Error)?;
    crate::attempt::validate_identifier(id).map_err(|_| Error)?;
    Ok((event.session_id.clone(), crabber_core::ControlId(id.into())))
}

/// Real Run creation/claim/activation/status writes use this before publication.
pub(super) fn record_run(
    state: &mut State,
    previous: Option<&Run>,
    updated: &Run,
) -> Result<(), Error> {
    let previous = previous.map(RunState::from_run).transpose()?;
    record_run_states(state, previous.as_ref(), RunState::from_run(updated)?)
}
pub(super) fn record_run_states(
    state: &mut State,
    previous: Option<&RunState>,
    next: RunState,
) -> Result<(), Error> {
    let latest = state
        .native_run_issuances
        .range(
            (next.session_id.clone(), next.run_id.clone(), 0)
                ..=(
                    next.session_id.clone(),
                    next.run_id.clone(),
                    i64::MAX as u64,
                ),
        )
        .next_back()
        .map(|(_, value)| value);
    if let Some(issuance) = crate::native_parent::advance(previous, next, latest)? {
        let key = (
            issuance.state.session_id.clone(),
            issuance.state.run_id.clone(),
            issuance.sequence,
        );
        if state.native_run_issuances.contains_key(&key) {
            return Err(Error);
        }
        state.native_run_issuances.insert(key, issuance);
    }
    Ok(())
}
fn latest_run<'a>(state: &'a State, run: &Run) -> Result<Option<&'a RunIssuance>, Error> {
    let scope = RunState::from_run(run)?;
    let latest = state
        .native_run_issuances
        .range(
            (scope.session_id.clone(), scope.run_id.clone(), 0)
                ..=(scope.session_id, scope.run_id, i64::MAX as u64),
        )
        .next_back()
        .map(|(_, value)| value);
    if let Some(latest) = latest {
        latest.verify(run)?;
    }
    Ok(latest)
}

#[cfg(test)]
#[path = "native_parent/tests.rs"]
mod tests;
