//! Validate persisted records before they can become continuation authority.
use crate::approval::{ApprovalQuestionRecord, ApprovalRecord};
use crate::{ApprovalContinuationState, ApprovalError as Error, ApprovalReply, CallApproval};
use crabber_core::{InteractionState, RunId, SessionId};

impl ApprovalRecord {
    pub(crate) fn validate(&self, session: &SessionId, run: &RunId) -> Result<(), Error> {
        self.validate_inner(session, run)
            .map_err(|_| Error::UnknownStoreOutcome)
    }
    fn validate_inner(&self, session: &SessionId, run: &RunId) -> Result<(), Error> {
        if self.version != 1 || self.session_id != *session || self.run_id != *run {
            return Err(Error::InvalidRequest);
        }
        self.batch.fingerprint(session, run)?;
        let expected: Vec<_> = self
            .batch
            .calls
            .iter()
            .filter_map(|call| {
                if let CallApproval::Ask { id, description } = &call.decision {
                    Some((id, description))
                } else {
                    None
                }
            })
            .collect();
        if expected.len() != self.questions.len() {
            return Err(Error::InvalidRequest);
        }
        let consumed = self.state != ApprovalContinuationState::Waiting;
        for ((id, description), question) in expected.iter().zip(&self.questions) {
            let expires_at = description
                .expires_after_ms()
                .map(|ms| {
                    let ms = i64::try_from(ms).map_err(|_| Error::InvalidRequest)?;
                    self.created_at
                        .checked_add(time::Duration::milliseconds(ms))
                        .ok_or(Error::InvalidRequest)
                })
                .transpose()?;
            if question.view.id != **id
                || question.view.expires_at != expires_at
                || question.view.consumed != consumed
            {
                return Err(Error::InvalidRequest);
            }
            self.validate_answer(question, description.authority())?;
        }
        if consumed {
            self.grants()?;
        }
        Ok(())
    }
    fn validate_answer(
        &self,
        question: &ApprovalQuestionRecord,
        authority: &crabber_core::AuthorityRequirement,
    ) -> Result<(), Error> {
        let Some(answer) = &question.answer else {
            if !matches!(
                question.view.state,
                InteractionState::Pending | InteractionState::Expired
            ) || question.view.consumed
                || question.view.state == InteractionState::Expired
                    && question.view.expires_at.is_none()
            {
                return Err(Error::InvalidRequest);
            }
            return Ok(());
        };
        answer.fingerprint()?;
        let state = match answer.reply {
            ApprovalReply::Grant => InteractionState::Answered,
            ApprovalReply::Deny => InteractionState::Denied,
        };
        if answer.session_id != self.session_id
            || answer.run_id != self.run_id
            || answer.continuation != self.batch.continuation
            || answer.interaction_id != question.view.id
            || answer.resolver.authority != *authority
            || answer.expected_revision <= self.batch.expected_revision
            || question.view.state != state
        {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
}
