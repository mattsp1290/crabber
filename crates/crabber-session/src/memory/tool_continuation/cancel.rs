use super::{
    Error, State, ToolContinuationOutcome, ToolContinuationRecord, ToolContinuationState, domain,
};
use crate::public_session::v2::proof::{Proof, ToolReadError};
use crabber_core::{InteractionState, Run};

pub(in super::super) fn cancel_waiting(
    state: &mut State,
    run: &Run,
    request: &crate::RunControlRequest,
    accepted: crate::RunControlReceipt,
    now: time::OffsetDateTime,
) -> Result<bool, Error> {
    let Some(mut record) = super::waiting_context(state, run)? else {
        return Ok(false);
    };
    let proof = domain::ToolContinuationTermination::new(&record, request.clone(), accepted, now)?;
    if state
        .operation_receipts
        .contains_key(&(run.session_id.clone(), proof.id.clone()))
    {
        return Err(Error::SemanticConflict);
    }
    record.state = ToolContinuationState::Released;
    for question in &mut record.questions {
        if question.answer.is_none() {
            question.view.state = InteractionState::Cancelled;
        }
        question.view.consumed = true;
    }
    record.termination = Some(proof.clone());
    record.validate()?;
    super::publish(
        state,
        run,
        proof.id.clone(),
        proof.fingerprint()?,
        ToolContinuationOutcome::Terminated {
            continuation: record.batch.continuation.clone(),
            original_cancel_id: request.id.clone(),
        },
        now,
    )?;
    state.tool_barriers.remove(&run.session_id);
    let source = record.batch.source_turn_id.clone();
    let calls = record
        .batch
        .calls
        .iter()
        .map(|p| p.target.call_id.clone())
        .collect();
    state
        .tool_continuations
        .insert(record.batch.continuation.id.clone(), record);
    super::super::run_control::suspension::apply_verified(state, run, request, calls, source, now)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(true)
}
pub(super) fn verify_with_proof(
    state: &State,
    record: &ToolContinuationRecord,
    budget: &mut Proof,
) -> Result<(), ToolReadError> {
    let Some(proof) = &record.termination else {
        return Ok(());
    };
    let Some(super::OperationReceipt::RunControl(accepted)) = state
        .operation_receipts
        .get(&(record.session_id.clone(), proof.original.id.clone()))
    else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    budget.measure(accepted).map_err(ToolReadError::Budget)?;
    if accepted != &proof.accepted {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let event = state
        .events
        .get(accepted.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    budget.measure(event).map_err(ToolReadError::Budget)?;
    crate::run_control::verify_event(&crate::run_control::accepted(accepted), event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let run = state
        .runs
        .get(&record.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let latest = state
        .attempts
        .values()
        .filter(|a| a.snapshot.run_id == run.id)
        .max_by_key(|a| a.snapshot.target.generation)
        .ok_or(Error::UnknownStoreOutcome)?;
    budget
        .measure(&(
            &latest.snapshot,
            &latest.owner,
            &latest.token,
            &latest.run_token,
            latest.lease_until,
        ))
        .map_err(ToolReadError::Budget)?;
    budget
        .measure(&(
            &run.id,
            &run.session_id,
            run.status,
            &run.plan_fingerprint,
            &run.config_hash,
        ))
        .map_err(ToolReadError::Budget)?;
    latest.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    proof.verify_run(record, run, &latest.snapshot)?;
    Ok(())
}
