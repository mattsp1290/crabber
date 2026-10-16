//! Whole-batch suspension and attempt conclusion are one clone-and-publish commit.
use super::{Error, replay};
use crate::{
    ApprovalContinuationState, ApprovalOutcome, ApprovalSuspension, AttemptAuthority,
    AttemptMutationOutcome, approval,
};
use crabber_core::{ContinuationGeneration, RunStatus, ToolCallStatus};

impl super::super::MemoryExecution {
    pub(in super::super) fn suspend_approval(
        &self,
        batch: ApprovalSuspension,
        authority: &AttemptAuthority,
    ) -> Result<crate::ApprovalReceipt, Error> {
        #[cfg(test)]
        let fault = self
            .store
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let result = self.store.transact(|state| {
            let run = state
                .runs
                .get(&self.fence.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            let digest = batch.fingerprint(&run.session_id, &run.id)?;
            if let Some(saved) = replay(state, &run.session_id, &run.id, &batch.id, &digest)? {
                return Ok((saved, false));
            }
            validate_batch(state, &run, &batch)?;
            let now = self.store.clock.now();
            let mut record = approval::ApprovalRecord::new(
                batch.clone(),
                run.session_id.clone(),
                run.id.clone(),
                state
                    .sessions
                    .get(&run.session_id)
                    .ok_or(Error::NotFound)?
                    .workspace_id
                    .clone(),
                now,
            )?;
            record.handoff = super::super::continuation_handoff::prepare(
                state,
                &run,
                crate::continuation_handoff::Domain::Approval,
                &batch.continuation,
                &batch.id,
                &digest,
                &batch.attempt,
            )?;
            record.validate(&run.session_id, &run.id)?;
            for question in &record.questions {
                if state
                    .approvals
                    .values()
                    .any(|old| old.questions.iter().any(|q| q.view.id == question.view.id))
                {
                    return Err(Error::SemanticConflict);
                }
            }
            let conclusion = batch.conclusion(&run, now)?;
            let applied = self
                .apply_attempt(
                    state,
                    conclusion.finish.clone(),
                    Some(authority),
                    Some(&conclusion),
                )
                .map_err(approval::attempt_error)?;
            if !matches!(applied, AttemptMutationOutcome::Applied { .. }) {
                return Err(Error::SemanticConflict);
            }
            if let Some(previous) = state.approval_barriers.get(&run.session_id) {
                state
                    .approvals
                    .get_mut(previous)
                    .ok_or(Error::UnknownStoreOutcome)?
                    .state = ApprovalContinuationState::Released;
            }
            let handoff = record.handoff.clone();
            if let Some(proof) = &handoff {
                super::super::continuation_handoff::release(state, &run, proof)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            state
                .approval_barriers
                .insert(run.session_id.clone(), batch.continuation.id.clone());
            state
                .approvals
                .insert(batch.continuation.id.clone(), record);
            let receipt = super::publish_handoff(
                state,
                &run,
                batch.id.clone(),
                digest,
                ApprovalOutcome::Suspended {
                    continuation: batch.continuation,
                },
                now,
                handoff.as_ref(),
            )?;
            super::before_commit(fault)?;
            Ok((receipt, true))
        });
        super::after_commit(result, fault)
    }
}

fn validate_predecessor(
    state: &super::super::State,
    run: &crabber_core::Run,
    batch: &ApprovalSuspension,
) -> Result<(), Error> {
    if let Some(id) = state.approval_barriers.get(&run.session_id) {
        let previous = state.approvals.get(id).ok_or(Error::UnknownStoreOutcome)?;
        super::verify_record(state, run, previous)?;
        if previous.run_id != run.id
            || previous.state != ApprovalContinuationState::Claimed
            || previous.batch.next_attempt != batch.attempt
            || previous.batch.continuation.generation.next().ok()
                != Some(batch.continuation.generation)
        {
            return Err(Error::GenerationConflict);
        }
    } else {
        let previous = state
            .approvals
            .values()
            .filter(|r| r.session_id == run.session_id && r.run_id == run.id)
            .max_by_key(|r| r.batch.continuation.generation);
        let generation = if let Some(previous) = previous {
            super::verify_record(state, run, previous)?;
            if previous.state != ApprovalContinuationState::Released {
                return Err(Error::UnknownStoreOutcome);
            }
            previous
                .batch
                .continuation
                .generation
                .next()
                .map_err(|_| Error::GenerationConflict)?
        } else {
            ContinuationGeneration::INITIAL
        };
        if batch.continuation.generation != generation {
            return Err(Error::GenerationConflict);
        }
    }
    Ok(())
}

fn validate_batch(
    state: &super::super::State,
    run: &crabber_core::Run,
    batch: &ApprovalSuspension,
) -> Result<(), Error> {
    if batch.expected_plan.as_str() != run.plan_fingerprint || run.status != RunStatus::Running {
        return Err(Error::InvalidTransition);
    }
    if state.approvals.contains_key(&batch.continuation.id) {
        return Err(Error::SemanticConflict);
    }
    validate_predecessor(state, run, batch)?;
    let pending: Vec<_> = state
        .calls
        .values()
        .filter(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
                )
        })
        .collect();
    if pending.len() != batch.calls.len()
        || pending
            .iter()
            .any(|call| call.status != ToolCallStatus::Pending || call.result.is_some())
    {
        return Err(Error::InvalidTransition);
    }
    for approved in &batch.calls {
        let call = pending
            .iter()
            .find(|call| call.id == approved.target.call_id)
            .ok_or(Error::InvalidTransition)?;
        if crate::phase::call_binding(call).map_err(|_| Error::InvalidRequest)?
            != approved.call_binding
            || state.phases.values().any(|phase| {
                phase.snapshot.run_id == run.id && phase.snapshot.target.call_id == call.id
            })
        {
            return Err(Error::SemanticConflict);
        }
    }
    Ok(())
}
