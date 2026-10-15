//! Atomic approval controls, sharing `MemoryStore`'s session mutation lock.
#[path = "approval/answer.rs"]
mod answer;
#[path = "approval/claim.rs"]
mod claim;
#[path = "approval/origin.rs"]
mod origin;
#[path = "approval/recover.rs"]
mod recover;
#[path = "approval/suspend.rs"]
mod suspend;
#[path = "approval/terminate.rs"]
mod terminate;
use super::{MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::{ApprovalError as Error, ApprovalOutcome, ApprovalReceipt};
use crabber_core::{ControlId, EventCursor, InputFingerprint, Run, SessionId};
pub(super) use origin::verify as verify_record;
use time::OffsetDateTime;

fn receipt(
    state: &State,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<ApprovalReceipt>, Error> {
    crate::public_state::validate_id(id).map_err(|_| Error::InvalidRequest)?;
    let Some(saved) = state.operation_receipts.get(&(session.clone(), id.clone())) else {
        return Ok(None);
    };
    let OperationReceipt::Approval(saved) = saved else {
        return Err(Error::SemanticConflict);
    };
    if saved.session_id != *session || saved.operation_id != *id {
        return Err(Error::UnknownStoreOutcome);
    }
    let index = saved
        .event_cursor
        .0
        .checked_sub(1)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(Error::UnknownStoreOutcome)?;
    let event = state.events.get(index).ok_or(Error::UnknownStoreOutcome)?;
    crate::approval::verify_receipt(saved, event)?;
    Ok(Some(saved.clone()))
}
fn replay(
    state: &State,
    session: &SessionId,
    run: &crabber_core::RunId,
    id: &ControlId,
    digest: &InputFingerprint,
) -> Result<Option<ApprovalReceipt>, Error> {
    let saved = receipt(state, session, id)?;
    if saved
        .as_ref()
        .is_some_and(|r| r.run_id != *run || r.semantic_digest != *digest)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(saved)
}
fn publish(
    state: &mut State,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: ApprovalOutcome,
    now: OffsetDateTime,
) -> Result<ApprovalReceipt, Error> {
    bump_public_revision(state, &run.session_id).map_err(|_| Error::UnknownStoreOutcome)?;
    let receipt = ApprovalReceipt {
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        operation_id: id.clone(),
        semantic_digest: digest,
        revision: revision(state, &run.session_id),
        event_cursor: EventCursor(state.events.len() as u64 + 1),
        outcome,
    };
    let mut event = crate::approval::event(
        run,
        &id,
        &receipt.semantic_digest,
        receipt.revision,
        &receipt.outcome,
        now,
    )?;
    event.cursor = Some(receipt.event_cursor);
    state.events.push(event);
    state.operation_receipts.insert(
        (run.session_id.clone(), id),
        OperationReceipt::Approval(receipt.clone()),
    );
    Ok(receipt)
}
fn revision(state: &State, session: &SessionId) -> crabber_core::SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(crabber_core::SessionRevision::INITIAL)
}
pub(super) fn guard_attempt(
    state: &State,
    run: &Run,
    request: &crate::AttemptMutation,
) -> Result<(), crate::AttemptError> {
    let Some(id) = state.approval_barriers.get(&run.session_id) else {
        return Ok(());
    };
    let record = state
        .approvals
        .get(id)
        .ok_or(crate::AttemptError::UnknownStoreOutcome)?;
    verify_record(state, run, record).map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    if record.run_id != run.id
        || record.state != crate::ApprovalContinuationState::Claimed
        || *request.command.target() != record.batch.next_attempt
    {
        return Err(crate::AttemptError::Busy);
    }
    Ok(())
}
pub(super) fn release_terminal(
    state: &mut State,
    run: &Run,
    request: &crate::AttemptMutation,
) -> Result<(), crate::AttemptError> {
    let Some(id) = state.approval_barriers.get(&run.session_id).cloned() else {
        return Ok(());
    };
    let record = state
        .approvals
        .get(&id)
        .ok_or(crate::AttemptError::UnknownStoreOutcome)?;
    verify_record(state, run, record).map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    let record = state
        .approvals
        .get_mut(&id)
        .ok_or(crate::AttemptError::UnknownStoreOutcome)?;
    if record.state != crate::ApprovalContinuationState::Claimed
        || record.batch.next_attempt != *request.command.target()
        || record.questions.iter().any(|q| !q.view.consumed)
    {
        return Err(crate::AttemptError::CleanupBlocked);
    }
    record.state = crate::ApprovalContinuationState::Released;
    state.approval_barriers.remove(&run.session_id);
    Ok(())
}
impl MemoryStore {
    pub(super) fn read_approval(
        &self,
        session: &SessionId,
        run: &crabber_core::RunId,
    ) -> Result<Option<crate::ApprovalSnapshot>, Error> {
        let state = self.state.lock().unwrap();
        if state.runs.get(run).is_none_or(|r| r.session_id != *session) {
            return Err(Error::NotFound);
        }
        let record = state
            .approvals
            .values()
            .filter(|record| record.session_id == *session && record.run_id == *run)
            .max_by_key(|record| record.batch.continuation.generation);
        record
            .map(|record| {
                verify_record(&state, state.runs.get(run).ok_or(Error::NotFound)?, record)?;
                Ok(record.snapshot(revision(&state, session)))
            })
            .transpose()
    }
    pub(super) fn read_approval_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ApprovalReceipt>, Error> {
        receipt(&self.state.lock().unwrap(), session, id)
    }
}

fn after_commit<T>(result: Result<(T, bool), Error>, fault: u8) -> Result<T, Error> {
    let (value, changed) = result?;
    if changed && fault == 2 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(value)
}
fn before_commit(fault: u8) -> Result<(), Error> {
    if fault == 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(super) fn retirement_context(
    state: &State,
    run: &Run,
) -> Result<crate::attempt_control::RetirementContext, crate::AttemptError> {
    let Some(id) = state.approval_barriers.get(&run.session_id) else {
        return Ok(crate::attempt_control::RetirementContext::default());
    };
    let record = state
        .approvals
        .get(id)
        .ok_or(crate::AttemptError::UnknownStoreOutcome)?;
    recover::validate_provenance(state, record)
        .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    for prepared in &record.batch.calls {
        let call = state
            .calls
            .get(&prepared.target.call_id)
            .ok_or(crate::AttemptError::UnknownStoreOutcome)?;
        prepared
            .verify_call(call)
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    }
    Ok(crate::attempt_control::RetirementContext {
        source: record.batch.source_turn_id.clone(),
        order: Some(
            record
                .batch
                .calls
                .iter()
                .map(|call| call.target.call_id.clone())
                .collect(),
        ),
    })
}

#[cfg(test)]
#[path = "approval/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "approval/termination_tests.rs"]
mod termination_tests;
