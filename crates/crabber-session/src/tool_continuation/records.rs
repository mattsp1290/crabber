use crate::tool_continuation::canonical::identity;
use crate::tool_continuation::{
    FrozenToolPhase, ToolContinuationClaim, ToolContinuationError, ToolContinuationSnapshot,
    ToolContinuationState, ToolContinuationSuspension, ToolInteractionAnswer, ToolInteractionReply,
    ToolInteractionView,
};
use crabber_core::{InteractionDescription, InteractionState, RunId, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
type Error = ToolContinuationError;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolQuestion {
    pub view: ToolInteractionView,
    pub answer: Option<ToolInteractionAnswer>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolContinuationRecord {
    pub version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub workspace_id: String,
    pub batch: ToolContinuationSuspension,
    pub created_at: OffsetDateTime,
    pub state: ToolContinuationState,
    pub questions: Vec<ToolQuestion>,
    pub claim: Option<ToolContinuationClaim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontend_claim: Option<Box<crate::FrontendContinuationClaim>>,
    #[serde(default)]
    pub recovery: Option<crate::ToolContinuationRecovery>,
    #[serde(default)]
    pub termination: Option<crate::tool_continuation::ToolContinuationTermination>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<Box<crate::continuation_handoff::Handoff>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontend_recovery: Option<Box<crate::frontend_continuation::FrontendRecoveryRecord>>,
}
impl ToolContinuationRecord {
    pub(crate) fn new(
        session: SessionId,
        run: RunId,
        workspace: String,
        batch: ToolContinuationSuspension,
        now: OffsetDateTime,
    ) -> Result<Self, Error> {
        batch.fingerprint(&session, &run)?;
        let mut questions = Vec::new();
        for phase in &batch.phases {
            for question in &phase.interactions {
                let expires_at = question
                    .description
                    .expires_after_ms()
                    .map(|ms| {
                        now.checked_add(time::Duration::milliseconds(
                            i64::try_from(ms).map_err(|_| Error::InvalidRequest)?,
                        ))
                        .ok_or(Error::InvalidRequest)
                    })
                    .transpose()?;
                questions.push(ToolQuestion {
                    view: ToolInteractionView {
                        id: question.id.clone(),
                        call_id: phase.previous.call_id.clone(),
                        phase: phase.previous.clone(),
                        description: question.description.clone(),
                        state: InteractionState::Pending,
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
            batch,
            created_at: now,
            state: ToolContinuationState::Waiting,
            questions,
            claim: None,
            frontend_claim: None,
            recovery: None,
            termination: None,
            handoff: None,
            frontend_recovery: None,
        })
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.version != 1 {
            return Err(Error::UnknownStoreOutcome);
        }
        identity(&self.session_id.0, 512)?;
        identity(&self.run_id.0, 128)?;
        let original = Self::new(
            self.session_id.clone(),
            self.run_id.clone(),
            self.workspace_id.clone(),
            self.batch.clone(),
            self.created_at,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
        if original.questions.len() != self.questions.len() {
            return Err(Error::UnknownStoreOutcome);
        }
        for (expected, question) in original.questions.iter().zip(&self.questions) {
            if expected.view.id != question.view.id
                || expected.view.call_id != question.view.call_id
                || expected.view.phase != question.view.phase
                || expected.view.description != question.view.description
                || expected.view.expires_at != question.view.expires_at
                || question.view.consumed != (self.state != ToolContinuationState::Waiting)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            match (&question.answer, question.view.state) {
                (None, InteractionState::Pending) if self.termination.is_none() => (),
                (None, InteractionState::Cancelled) if self.termination.is_some() => (),
                (Some(answer), state) if state != InteractionState::Pending => {
                    answer
                        .fingerprint()
                        .map_err(|_| Error::UnknownStoreOutcome)?;
                    if answer.session_id != self.session_id
                        || answer.run_id != self.run_id
                        || answer.continuation != self.batch.continuation
                        || answer.interaction_id != question.view.id
                        || answer.resolver.authority != *question.view.description.authority()
                        || reply_state(&answer.reply) != state
                    {
                        return Err(Error::UnknownStoreOutcome);
                    }
                    validate_value(&question.view.description, &answer.reply)?;
                }
                _ => return Err(Error::UnknownStoreOutcome),
            }
        }
        if let Some(termination) = &self.termination {
            termination.verify(self)?;
            if self.state != ToolContinuationState::Released
                || self.claim.is_some()
                || self.recovery.is_some()
            {
                return Err(Error::UnknownStoreOutcome);
            }
        } else if (self.state == ToolContinuationState::Waiting) != self.claim.is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(claim) = &self.claim {
            if claim.session_id != self.session_id
                || claim.run_id != self.run_id
                || claim.continuation != self.batch.continuation
                || claim.expected_plan != self.batch.expected_plan
            {
                return Err(Error::UnknownStoreOutcome);
            }
            claim.fingerprint()?;
        }
        if let Some(recovery) = &self.recovery {
            let claim = self.claim.as_ref().ok_or(Error::UnknownStoreOutcome)?;
            if recovery.original.fingerprint()? != claim.fingerprint()?
                || recovery.original.id != claim.id
                || recovery.attempt != self.batch.next_attempt
            {
                return Err(Error::UnknownStoreOutcome);
            }
            recovery.fingerprint()?;
        }
        self.validate_frontend()?;
        if crate::snapshot::encoded_bytes(self, 4 * 1024 * 1024).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub(crate) fn snapshot(&self, revision: SessionRevision) -> ToolContinuationSnapshot {
        ToolContinuationSnapshot {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            workspace_id: self.workspace_id.clone(),
            continuation: self.batch.continuation.clone(),
            attempt: self.batch.attempt.clone(),
            next_attempt: self.batch.next_attempt.clone(),
            revision,
            state: self.state,
            interactions: self.questions.iter().map(|q| q.view.clone()).collect(),
            frontend_calls: self
                .batch
                .frontend
                .iter()
                .map(|call| crate::FrontendCallView {
                    call_id: call.target.call_id.clone(),
                    descriptor: call.descriptor.clone(),
                    descriptor_fingerprint: call.descriptor_fingerprint.clone(),
                    arguments: call.arguments.clone(),
                    consumed: self.state != ToolContinuationState::Waiting,
                })
                .collect(),
        }
    }
    pub(crate) fn answer(
        &mut self,
        answer: &ToolInteractionAnswer,
        now: OffsetDateTime,
    ) -> Result<(), Error> {
        self.validate()?;
        if answer.continuation != self.batch.continuation {
            return Err(Error::GenerationConflict);
        }
        if self.state != ToolContinuationState::Waiting {
            return Err(Error::InvalidTransition);
        }
        let question = self
            .questions
            .iter_mut()
            .find(|q| q.view.id == answer.interaction_id)
            .ok_or(Error::NotFound)?;
        if question.view.state != InteractionState::Pending {
            return Err(Error::SemanticConflict);
        }
        if answer.resolver.authority != *question.view.description.authority() {
            return Err(Error::AuthorityMismatch);
        }
        let expired = question.view.expires_at.is_some_and(|end| end <= now);
        if matches!(answer.reply, ToolInteractionReply::Expire) != expired {
            return Err(if expired {
                Error::Expired
            } else {
                Error::InvalidRequest
            });
        }
        validate_value(&question.view.description, &answer.reply)?;
        question.view.state = reply_state(&answer.reply);
        question.answer = Some(answer.clone());
        self.validate()
    }
    pub(crate) fn responses(
        &self,
        phase: &FrozenToolPhase,
    ) -> Result<Vec<crabber_core::AcceptedToolResponse>, Error> {
        phase
            .interactions
            .iter()
            .map(|required| {
                let question = self
                    .questions
                    .iter()
                    .find(|q| q.view.id == required.id)
                    .ok_or(Error::UnknownStoreOutcome)?;
                let answer = question.answer.as_ref().ok_or(Error::Pending)?;
                Ok(crabber_core::AcceptedToolResponse {
                    name: required.description.name().into(),
                    state: question.view.state,
                    value: match &answer.reply {
                        ToolInteractionReply::Answer { value } => Some(value.clone()),
                        _ => None,
                    },
                })
            })
            .collect()
    }
}
pub(crate) fn reply_state(reply: &ToolInteractionReply) -> InteractionState {
    match reply {
        ToolInteractionReply::Answer { .. } => InteractionState::Answered,
        ToolInteractionReply::Deny => InteractionState::Denied,
        ToolInteractionReply::Cancel => InteractionState::Cancelled,
        ToolInteractionReply::Expire => InteractionState::Expired,
    }
}
fn validate_value(
    description: &InteractionDescription,
    reply: &ToolInteractionReply,
) -> Result<(), Error> {
    if let ToolInteractionReply::Answer { value } = reply {
        let validator = jsonschema::validator_for(description.response_schema())
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if !validator.is_valid(value) {
            return Err(Error::SchemaMismatch);
        }
    }
    Ok(())
}
impl ToolContinuationRecord {
    pub(crate) fn claimed_outcome(
        &self,
        attempt: crate::AttemptSnapshot,
    ) -> crate::ToolContinuationOutcome {
        if self.frontend_claim.is_some() {
            crate::ToolContinuationOutcome::FrontendClaimed {
                continuation: self.batch.continuation.clone(),
                attempt,
            }
        } else {
            crate::ToolContinuationOutcome::Claimed {
                continuation: self.batch.continuation.clone(),
                attempt,
            }
        }
    }
    pub(crate) fn recovered_outcome(
        &self,
        attempt: crate::AttemptSnapshot,
    ) -> Result<crate::ToolContinuationOutcome, Error> {
        let original_claim_id = self
            .claim
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .id
            .clone();
        let continuation = self.batch.continuation.clone();
        Ok(if self.frontend_recovery.is_some() {
            crate::ToolContinuationOutcome::FrontendReclaimedUnstarted {
                continuation,
                original_claim_id,
                attempt,
            }
        } else {
            crate::ToolContinuationOutcome::ReclaimedUnstarted {
                continuation,
                original_claim_id,
                attempt,
            }
        })
    }
    pub(crate) fn into_grant(
        self,
        fence: crabber_core::RunFence,
        authority: crate::AttemptAuthority,
        phases: Vec<crate::ToolPhaseResume>,
        frontend: Vec<crate::FrontendPhaseResume>,
    ) -> Result<Box<crate::ToolContinuationGrant>, Error> {
        Ok(Box::new(crate::ToolContinuationGrant {
            calls: self.grants()?,
            fence,
            authority,
            phases,
            frontend,
            source_turn_id: self.batch.source_turn_id,
            usage: self.batch.usage,
            turns_used: self.batch.turns_used,
        }))
    }
    pub(crate) fn grants(&self) -> Result<Vec<crate::GrantedApproval>, Error> {
        self.batch
            .calls
            .iter()
            .map(|call| {
                Ok(crate::GrantedApproval {
                    target: call.target.clone(),
                    call_binding: call.call_binding.clone(),
                    decision: match call.decision {
                        crate::CallApproval::Allow => crate::GrantedDecision::Allow,
                        crate::CallApproval::Deny => crate::GrantedDecision::Deny,
                        crate::CallApproval::NotRequired => crate::GrantedDecision::NotRequired,
                        crate::CallApproval::Ask { .. } => return Err(Error::UnknownStoreOutcome),
                    },
                })
            })
            .collect()
    }
}
