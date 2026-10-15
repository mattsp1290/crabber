//! Host assertions and Boolean choices are persisted independently of dispatch.
use super::{Error, publish, replay, revision};
use crate::{
    ApprovalAnswer, ApprovalContinuationState, ApprovalOutcome, ApprovalReply, CallApproval,
};
use crabber_core::InteractionState;

impl super::super::MemoryStore {
    pub(in super::super) fn answer_approval_inner(
        &self,
        answer: &ApprovalAnswer,
    ) -> Result<crate::ApprovalReceipt, Error> {
        let digest = answer.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let result = self.transact(|state| {
            if let Some(saved) = replay(
                state,
                &answer.session_id,
                &answer.run_id,
                &answer.id,
                &digest,
            )? {
                return Ok((saved, false));
            }
            let run = state
                .runs
                .get(&answer.run_id)
                .filter(|run| run.session_id == answer.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if revision(state, &run.session_id) != answer.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let current = state
                .approval_barriers
                .get(&run.session_id)
                .ok_or(Error::InvalidTransition)?;
            if *current != answer.continuation.id {
                return Err(Error::GenerationConflict);
            }
            let mut record = state
                .approvals
                .get(current)
                .cloned()
                .ok_or(Error::UnknownStoreOutcome)?;
            record.validate(&run.session_id, &run.id)?;
            if record.batch.continuation != answer.continuation || record.run_id != run.id {
                return Err(Error::GenerationConflict);
            }
            if record.state != ApprovalContinuationState::Waiting
                || run.status != crabber_core::RunStatus::Paused
            {
                return Err(Error::InvalidTransition);
            }
            let description = record
                .batch
                .calls
                .iter()
                .find_map(|call| match &call.decision {
                    CallApproval::Ask { id, description } if *id == answer.interaction_id => {
                        Some(description)
                    }
                    _ => None,
                })
                .ok_or(Error::NotFound)?;
            if *description.authority() != answer.resolver.authority {
                return Err(Error::AuthorityMismatch);
            }
            let question = record
                .questions
                .iter_mut()
                .find(|q| q.view.id == answer.interaction_id)
                .ok_or(Error::UnknownStoreOutcome)?;
            if question.view.state == InteractionState::Expired
                || question
                    .view
                    .expires_at
                    .is_some_and(|at| at <= self.clock.now())
            {
                return Err(Error::Expired);
            }
            if question.view.state != InteractionState::Pending || question.view.consumed {
                return Err(Error::InvalidTransition);
            }
            question.view.state = match answer.reply {
                ApprovalReply::Grant => InteractionState::Answered,
                ApprovalReply::Deny => InteractionState::Denied,
            };
            question.answer = Some(answer.clone());
            let outcome = ApprovalOutcome::Answered {
                continuation: answer.continuation.clone(),
                interaction_id: answer.interaction_id.clone(),
                state: question.view.state,
            };
            state.approvals.insert(current.clone(), record);
            let receipt = publish(
                state,
                &run,
                answer.id.clone(),
                digest.clone(),
                outcome,
                self.clock.now(),
            )?;
            super::before_commit(fault)?;
            Ok((receipt, true))
        });
        super::after_commit(result, fault)
    }
}
