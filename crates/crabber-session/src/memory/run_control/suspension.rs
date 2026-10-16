use super::super::{State, tool_result};
use crate::{RunControlError as Error, RunControlRequest, run_control};
use crabber_core::{Run, ToolCallStatus};
pub(super) fn cancel(
    state: &mut State,
    run: &Run,
    request: &RunControlRequest,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let calls = verify(state, run)?;
    apply_verified(
        state,
        run,
        request,
        calls,
        run_control::source_turn(run)?,
        now,
    )
}
pub(in super::super) fn apply_verified(
    state: &mut State,
    run: &Run,
    request: &RunControlRequest,
    calls: Vec<crabber_core::ToolCallId>,
    source: Option<crabber_core::TurnId>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let turn =
        super::super::attempt::host_turn(state, run, source).map_err(run_control::unknown)?;
    for id in calls {
        let call = state.calls.get(&id).ok_or(Error::UnknownStoreOutcome)?;
        let mut records = run_control::cancel_call(request, run, call, now)?;
        records.event.turn_id.clone_from(&turn.source);
        tool_result::publish_result(state, run, &id, records).map_err(run_control::unknown)?;
    }
    let terminal = run_control::cancelled_run(run, now);
    if let Some(turn) = turn.close {
        super::super::insert_event(
            state,
            run,
            crate::attempt_control::close_turn(run, turn, now),
        )
        .map_err(run_control::unknown)?;
    }
    let mut event = run_control::terminal_event(&terminal, now)?;
    event.turn_id = turn.source;
    super::super::insert_event(state, run, event).map_err(run_control::unknown)?;
    super::super::native_parent::record_run(state, Some(run), &terminal)
        .map_err(run_control::unknown)?;
    state.runs.insert(run.id.clone(), terminal.clone());
    super::finalize(state, &terminal, now).map_err(run_control::unknown)?;
    Ok(())
}
fn verify(state: &State, run: &Run) -> Result<Vec<crabber_core::ToolCallId>, Error> {
    let proof = state.suspension_proofs.get(&run.id).ok_or(Error::Busy)?;
    let latest = state
        .attempts
        .values()
        .filter(|r| r.snapshot.run_id == run.id)
        .max_by_key(|r| r.snapshot.target.generation)
        .ok_or(Error::Busy)?;
    latest.validate().map_err(run_control::unknown)?;
    let calls = proof.verify(run, &latest.snapshot)?;
    let Some(super::super::OperationReceipt::Attempt(saved)) = state
        .operation_receipts
        .get(&(run.session_id.clone(), proof.receipt.operation_id.clone()))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    if saved != &proof.receipt {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::attempt::verify_receipt(
        saved,
        state
            .events
            .get(saved.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(run_control::unknown)?;
    let mut pending = std::collections::BTreeSet::new();
    for call in state.calls.values().filter(|call| {
        call.run_id == run.id
            && matches!(
                call.status,
                ToolCallStatus::Pending | ToolCallStatus::Running
            )
    }) {
        if pending.len() == 64 || call.id.0.len() > 512 {
            return Err(Error::Busy);
        }
        pending.insert(call.id.clone());
    }
    if pending != calls.iter().cloned().collect() {
        return Err(Error::UnknownStoreOutcome);
    }
    for phase in state
        .phases
        .values()
        .filter(|p| p.snapshot.run_id == run.id)
    {
        let call = state
            .calls
            .get(&phase.snapshot.target.call_id)
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::attempt_control::validate_phase(run, call, phase).map_err(|_| Error::Busy)?;
        let parent = state
            .attempts
            .get(&phase.snapshot.target.attempt.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        parent.validate().map_err(run_control::unknown)?;
        if parent.snapshot.target != phase.snapshot.target.attempt
            || parent.snapshot.run_id != run.id
            || parent.snapshot.session_id != run.session_id
        {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    for id in &calls {
        let call = state.calls.get(id).ok_or(Error::UnknownStoreOutcome)?;
        if call.run_id != run.id
            || call.status != ToolCallStatus::Pending
            || call.result.is_some()
            || state
                .phases
                .values()
                .any(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == *id)
        {
            return Err(Error::Busy);
        }
    }
    Ok(calls)
}

pub(in super::super) fn save(
    state: &mut State,
    run: &Run,
    conclusion: Option<&crate::AttemptConclusion>,
    receipt: &crate::AttemptReceipt,
) {
    if let Some(conclusion) = conclusion
        && matches!(conclusion.boundary, crate::RunBoundary::Suspended { .. })
    {
        state.suspension_proofs.insert(
            run.id.clone(),
            run_control::SuspensionProof {
                conclusion: conclusion.clone(),
                receipt: receipt.clone(),
            },
        );
    }
}
