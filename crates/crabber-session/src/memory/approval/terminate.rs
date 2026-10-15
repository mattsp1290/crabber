//! Administrative termination of a fully joined waiting batch.
use super::{Error, publish, receipt, replay, revision};
use crate::{ApprovalReceipt, ApprovalTermination, AttemptCleanup, AttemptState, approval};
use crabber_core::{Run, ToolCallStatus};

impl super::super::MemoryStore {
    pub(in super::super) fn end_approval(
        &self,
        request: &ApprovalTermination,
    ) -> Result<ApprovalReceipt, Error> {
        let digest = request.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let result = self.transact(|state| {
            if let Some(saved) = replay(
                state,
                &request.session_id,
                &request.run_id,
                &request.id,
                &digest,
            )? {
                request.verify_receipt(&saved)?;
                return Ok((saved, false));
            }
            let mut run = state
                .runs
                .get(&request.run_id)
                .filter(|run| run.session_id == request.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if revision(state, &run.session_id) != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let id = state
                .approval_barriers
                .get(&run.session_id)
                .cloned()
                .ok_or(Error::InvalidTransition)?;
            let mut record = state
                .approvals
                .get(&id)
                .cloned()
                .ok_or(Error::UnknownStoreOutcome)?;
            super::verify_record(state, &run, &record)?;
            validate_native(state, &run, &record)?;
            validate_calls(state, &run, &record)?;
            let now = self.clock.now();
            let outcome = record.terminate(request, &run, now)?;
            for prepared in &record.batch.calls {
                let call = state
                    .calls
                    .get(&prepared.target.call_id)
                    .ok_or(Error::NotFound)?;
                let records = request.materialize_call(
                    &run,
                    call,
                    &prepared.target,
                    now,
                    record.batch.source_turn_id.clone(),
                )?;
                super::super::tool_result::publish_result(
                    state,
                    &run,
                    &prepared.target.call_id,
                    records,
                )
                .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            if let Some(event) =
                request.turn_event(&run, record.batch.source_turn_id.clone(), now)?
            {
                super::super::insert_event(state, &run, event)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            let event = request.run_event(&run, record.batch.source_turn_id.clone(), now)?;
            super::super::insert_event(state, &run, event)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            request.apply_run(&mut run, now)?;
            state.runs.insert(run.id.clone(), run.clone());
            state.approvals.insert(id, record);
            state.approval_barriers.remove(&run.session_id);
            let saved = publish(
                state,
                &run,
                request.id.clone(),
                digest.clone(),
                outcome,
                now,
            )?;
            request.verify_receipt(&saved)?;
            super::before_commit(fault)?;
            Ok((saved, true))
        });
        super::after_commit(result, fault)
    }
}
fn validate_native(
    state: &super::super::State,
    run: &Run,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let saved =
        receipt(state, &run.session_id, &record.batch.id)?.ok_or(Error::UnknownStoreOutcome)?;
    record.verify_recovery_batch(&saved)?;
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            let saved =
                receipt(state, &run.session_id, &answer.id)?.ok_or(Error::UnknownStoreOutcome)?;
            approval::verify_recovery_answer(answer, &saved)?;
        }
    }
    let latest = state
        .attempts
        .values()
        .filter(|attempt| attempt.snapshot.run_id == run.id)
        .max_by_key(|attempt| attempt.snapshot.target.generation)
        .ok_or(Error::NotFound)?;
    latest.validate().map_err(approval::attempt_error)?;
    if latest.snapshot.session_id != run.session_id
        || latest.snapshot.target != record.batch.attempt
        || latest.snapshot.state != AttemptState::Suspended
        || latest.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    for phase in state
        .phases
        .values()
        .filter(|phase| phase.snapshot.run_id == run.id)
    {
        let owner = state
            .attempts
            .get(&phase.snapshot.target.attempt.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        owner.validate().map_err(approval::attempt_error)?;
        if owner.snapshot.target != phase.snapshot.target.attempt
            || owner.snapshot.session_id != run.session_id
            || owner.snapshot.run_id != run.id
            || owner.snapshot.target.generation > latest.snapshot.target.generation
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let call = state
            .calls
            .get(&phase.snapshot.target.call_id)
            .ok_or(Error::UnknownStoreOutcome)?;
        approval::validate_termination_phase(run, phase, call)?;
    }
    Ok(())
}
fn validate_calls(
    state: &super::super::State,
    run: &Run,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let pending = state
        .calls
        .values()
        .filter(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running
                )
        })
        .count();
    if pending != record.batch.calls.len() {
        return Err(Error::InvalidTransition);
    }
    for prepared in &record.batch.calls {
        let call = state
            .calls
            .get(&prepared.target.call_id)
            .ok_or(Error::NotFound)?;
        if call.run_id != run.id
            || call.status != ToolCallStatus::Pending
            || call.result.is_some()
            || crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
                != prepared.call_binding
        {
            return Err(Error::SemanticConflict);
        }
        if state.phases.values().any(|phase| {
            phase.snapshot.run_id == run.id && phase.snapshot.target.call_id == call.id
        }) {
            return Err(Error::InvalidTransition);
        }
    }
    Ok(())
}
