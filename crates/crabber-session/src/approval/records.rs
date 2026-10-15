//! Private persisted approval state. Public snapshots contain no worker tokens.
#[path = "answer.rs"]
mod answer;
#[path = "integrity.rs"]
mod integrity;
#[path = "termination.rs"]
mod termination;
use super::{
    ApprovalAnswer, ApprovalContinuationState, ApprovalError as Error, ApprovalInteraction,
    ApprovalReply, ApprovalSnapshot, ApprovalSuspension, CallApproval, GrantedApproval,
    GrantedDecision,
};
use crabber_core::{InteractionState, RunId, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
pub(crate) use termination::validate_termination_phase;
use time::OffsetDateTime;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovalRecord {
    pub version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub workspace_id: String,
    pub created_at: OffsetDateTime,
    pub batch: ApprovalSuspension,
    pub state: ApprovalContinuationState,
    pub questions: Vec<ApprovalQuestionRecord>,
    #[serde(default)]
    pub termination: Option<ApprovalTerminationRecord>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovalTerminationRecord {
    pub request: crate::ApprovalTermination,
    pub ended_at: OffsetDateTime,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovalQuestionRecord {
    pub view: ApprovalInteraction,
    pub answer: Option<ApprovalAnswer>,
}
impl ApprovalRecord {
    pub fn snapshot(&self, revision: SessionRevision) -> ApprovalSnapshot {
        ApprovalSnapshot {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            workspace_id: self.workspace_id.clone(),
            revision,
            continuation: self.batch.continuation.clone(),
            attempt: self.batch.attempt.clone(),
            next_attempt: self.batch.next_attempt.clone(),
            state: self.state,
            termination: self
                .termination
                .as_ref()
                .map(|ended| ended.request.kind.clone()),
            calls: self.batch.calls.clone(),
            interactions: self.questions.iter().map(|q| q.view.clone()).collect(),
        }
    }
    pub fn grants(&self) -> Result<Vec<GrantedApproval>, Error> {
        self.batch
            .calls
            .iter()
            .map(|call| {
                let decision = match &call.decision {
                    CallApproval::Allow => GrantedDecision::Allow,
                    CallApproval::Deny => GrantedDecision::Deny,
                    CallApproval::NotRequired => GrantedDecision::NotRequired,
                    CallApproval::Ask { id, .. } => {
                        let question = self
                            .questions
                            .iter()
                            .find(|q| q.view.id == *id)
                            .ok_or(Error::UnknownStoreOutcome)?;
                        match question.answer.as_ref().map(|a| a.reply) {
                            Some(ApprovalReply::Grant)
                                if question.view.state == InteractionState::Answered =>
                            {
                                GrantedDecision::Allow
                            }
                            Some(ApprovalReply::Deny)
                                if question.view.state == InteractionState::Denied =>
                            {
                                GrantedDecision::Deny
                            }
                            _ => return Err(Error::Pending),
                        }
                    }
                };
                Ok(GrantedApproval {
                    target: call.target.clone(),
                    call_binding: call.call_binding.clone(),
                    decision,
                })
            })
            .collect()
    }
    pub fn new(
        batch: ApprovalSuspension,
        session: SessionId,
        run: RunId,
        workspace: String,
        now: OffsetDateTime,
    ) -> Result<Self, Error> {
        let mut questions = Vec::new();
        for call in &batch.calls {
            if let CallApproval::Ask { id, description } = &call.decision {
                let expires_at = description
                    .expires_after_ms()
                    .map(|ms| {
                        let duration = time::Duration::milliseconds(
                            i64::try_from(ms).map_err(|_| Error::InvalidRequest)?,
                        );
                        now.checked_add(duration).ok_or(Error::InvalidRequest)
                    })
                    .transpose()?;
                questions.push(ApprovalQuestionRecord {
                    view: ApprovalInteraction {
                        id: id.clone(),
                        state: if expires_at.is_some_and(|at| at <= now) {
                            InteractionState::Expired
                        } else {
                            InteractionState::Pending
                        },
                        expires_at,
                        consumed: false,
                    },
                    answer: None,
                });
            }
        }
        Ok(Self {
            version: 1,
            session_id: session,
            run_id: run,
            workspace_id: workspace,
            created_at: now,
            batch,
            state: ApprovalContinuationState::Waiting,
            questions,
            termination: None,
        })
    }
}

pub(crate) fn derived_id(
    domain: &str,
    original: &crabber_core::ControlId,
) -> crabber_core::ControlId {
    use sha2::{Digest, Sha256};
    crabber_core::ControlId(format!(
        "{:x}",
        Sha256::digest(format!("crabber.approval.{domain}:{}", original.0).as_bytes())
    ))
}
pub(crate) fn attempt_error(error: crate::AttemptError) -> Error {
    match error {
        crate::AttemptError::Unsupported => Error::Unsupported,
        crate::AttemptError::NotFound => Error::NotFound,
        crate::AttemptError::RevisionConflict => Error::RevisionConflict,
        crate::AttemptError::GenerationConflict | crate::AttemptError::GenerationExhausted => {
            Error::GenerationConflict
        }
        crate::AttemptError::StaleOwner | crate::AttemptError::LiveOwner => Error::StaleOwner,
        crate::AttemptError::CleanupBlocked | crate::AttemptError::Busy => Error::CleanupBlocked,
        crate::AttemptError::SemanticConflict => Error::SemanticConflict,
        crate::AttemptError::InvalidRequest => Error::InvalidRequest,
        crate::AttemptError::InvalidTransition | crate::AttemptError::PendingInput => {
            Error::InvalidTransition
        }
        crate::AttemptError::UnknownStoreOutcome | crate::AttemptError::RevisionExhausted => {
            Error::UnknownStoreOutcome
        }
    }
}
