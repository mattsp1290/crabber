//! Backend-independent validation of a host-asserted approval response.
use crate::approval::ApprovalRecord;
use crate::{
    ApprovalAnswer, ApprovalContinuationState, ApprovalError as Error, ApprovalOutcome,
    ApprovalReply, CallApproval,
};
use crabber_core::InteractionState;
use time::OffsetDateTime;

impl ApprovalRecord {
    pub(crate) fn accept_answer(
        &mut self,
        answer: &ApprovalAnswer,
        now: OffsetDateTime,
    ) -> Result<ApprovalOutcome, Error> {
        self.validate(&answer.session_id, &answer.run_id)?;
        if self.batch.continuation != answer.continuation {
            return Err(Error::GenerationConflict);
        }
        if self.state != ApprovalContinuationState::Waiting {
            return Err(Error::InvalidTransition);
        }
        let description = self
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
        let question = self
            .questions
            .iter_mut()
            .find(|q| q.view.id == answer.interaction_id)
            .ok_or(Error::UnknownStoreOutcome)?;
        if question.view.state == InteractionState::Expired
            || question.view.expires_at.is_some_and(|at| at <= now)
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
        Ok(ApprovalOutcome::Answered {
            continuation: answer.continuation.clone(),
            interaction_id: answer.interaction_id.clone(),
            state: question.view.state,
        })
    }
}
