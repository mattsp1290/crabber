//! Whole-batch suspension and attempt conclusion are one clone-and-publish commit.
use super::{Error, publish, replay};
use crate::{
    ApprovalContinuationState, ApprovalOutcome, ApprovalSuspension, AttemptAuthority,
    AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation, AttemptMutationOutcome,
    RunBoundary, approval,
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
            let record = approval::ApprovalRecord::new(
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
            let conclusion = AttemptConclusion {
                finish: AttemptMutation {
                    id: approval::derived_id("suspend-attempt", &batch.id),
                    expected_revision: batch.expected_revision,
                    command: AttemptCommand::Finish {
                        target: batch.attempt.clone(),
                        outcome: AttemptFinish::Suspended,
                    },
                },
                boundary: RunBoundary::Suspended {
                    checkpoint: serde_json::json!({"version":3,"approval":batch.continuation}),
                    usage: batch.usage.clone(),
                },
                created_at: now,
                turn_id: None,
                elapsed: (now - run.created_at).try_into().unwrap_or_default(),
            };
            conclusion.validate().map_err(approval::attempt_error)?;
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
            state
                .approval_barriers
                .insert(run.session_id.clone(), batch.continuation.id.clone());
            state
                .approvals
                .insert(batch.continuation.id.clone(), record);
            let receipt = publish(
                state,
                &run,
                batch.id.clone(),
                digest,
                ApprovalOutcome::Suspended {
                    continuation: batch.continuation,
                },
                now,
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
        previous.validate(&run.session_id, &run.id)?;
        if previous.run_id != run.id
            || previous.state != ApprovalContinuationState::Claimed
            || previous.batch.next_attempt != batch.attempt
            || previous.batch.continuation.generation.next().ok()
                != Some(batch.continuation.generation)
        {
            return Err(Error::GenerationConflict);
        }
    } else if batch.continuation.generation != ContinuationGeneration::INITIAL {
        return Err(Error::GenerationConflict);
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
                    ToolCallStatus::Pending | ToolCallStatus::Running
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
