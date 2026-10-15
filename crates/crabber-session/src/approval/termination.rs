//! Termination is independent of permission and never produces a dispatch grant.
use super::ApprovalRecord;
use crate::{
    ApprovalContinuationState, ApprovalError as Error, ApprovalOutcome, ApprovalTermination,
    ApprovalTerminationKind,
};
use crabber_core::{InteractionState, Run, RunStatus};
use time::OffsetDateTime;

impl ApprovalRecord {
    pub(crate) fn terminate(
        &mut self,
        request: &ApprovalTermination,
        run: &Run,
        now: OffsetDateTime,
    ) -> Result<ApprovalOutcome, Error> {
        self.validate(&request.session_id, &request.run_id)?;
        request.fingerprint()?;
        if self.batch.continuation != request.continuation {
            return Err(Error::GenerationConflict);
        }
        if self.state != ApprovalContinuationState::Waiting
            || self.termination.is_some()
            || run.status != RunStatus::Paused
        {
            return Err(Error::InvalidTransition);
        }
        if run.session_id != self.session_id
            || run.id != self.run_id
            || run.plan_fingerprint != self.batch.expected_plan.as_str()
            || run.usage != self.batch.usage
            || run.checkpoint.as_ref()
                != Some(&serde_json::json!({
                    "version":4,"approval":self.batch.continuation,"turns_used":self.batch.turns_used
                }))
        {
            return Err(Error::SemanticConflict);
        }
        self.validate_expiry(request, now)?;
        for question in &mut self.questions {
            if question.answer.is_none() {
                question.view.state = if question.view.expires_at.is_some_and(|at| at <= now) {
                    InteractionState::Expired
                } else {
                    InteractionState::Cancelled
                };
            }
        }
        self.state = ApprovalContinuationState::Released;
        self.termination = Some(super::ApprovalTerminationRecord {
            request: request.clone(),
            ended_at: now,
        });
        self.validate(&request.session_id, &request.run_id)?;
        Ok(ApprovalOutcome::Terminated {
            continuation: request.continuation.clone(),
            kind: request.kind.clone(),
            resolver: request.resolver.clone(),
            status: request.run_status(),
        })
    }
    fn validate_expiry(
        &self,
        request: &ApprovalTermination,
        now: OffsetDateTime,
    ) -> Result<(), Error> {
        if let ApprovalTerminationKind::Expire { interaction_id } = &request.kind {
            let question = self
                .questions
                .iter()
                .find(|q| q.view.id == *interaction_id)
                .ok_or(Error::NotFound)?;
            if question.answer.is_some() || question.view.consumed {
                return Err(Error::InvalidTransition);
            }
            if question.view.expires_at.is_none_or(|at| at > now) {
                return Err(Error::NotExpired);
            }
        }
        Ok(())
    }
    pub(super) fn validate_termination(&self) -> Result<bool, Error> {
        let Some(ended) = &self.termination else {
            return Ok(false);
        };
        ended.request.fingerprint()?;
        if self.state != ApprovalContinuationState::Released
            || ended.request.session_id != self.session_id
            || ended.request.run_id != self.run_id
            || ended.request.continuation != self.batch.continuation
            || ended.request.expected_revision <= self.batch.expected_revision
            || ended.ended_at < self.created_at
        {
            return Err(Error::InvalidRequest);
        }
        self.validate_expiry(&ended.request, ended.ended_at)?;
        Ok(true)
    }
}

/// Historical phases may coexist with the waiting batch only when their
/// protected outputs and logical results agree and their resources have joined.
pub(crate) fn validate_termination_phase(
    run: &Run,
    phase: &crate::phase::PhaseRecord,
    call: &crabber_core::ToolCallRecord,
) -> Result<(), Error> {
    phase.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    let saved = &phase.snapshot;
    if saved.session_id != run.session_id
        || saved.run_id != run.id
        || saved.quiescence != crate::PhaseQuiescence::Complete
        || !matches!(
            saved.state,
            crate::PhaseState::Completed | crate::PhaseState::Failed
        )
    {
        return Err(Error::CleanupBlocked);
    }
    let output = phase.output.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    let result = call.result.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    let valid_status = match saved.state {
        crate::PhaseState::Completed => {
            result.status == crabber_core::ToolResultStatus::Completed
                && call.status == crabber_core::ToolCallStatus::Completed
        }
        crate::PhaseState::Failed => match result.status {
            crabber_core::ToolResultStatus::Failed => {
                call.status == crabber_core::ToolCallStatus::Failed
            }
            crabber_core::ToolResultStatus::Interrupted => {
                call.status == crabber_core::ToolCallStatus::Interrupted
            }
            crabber_core::ToolResultStatus::Completed => false,
        },
        _ => false,
    };
    if call.run_id != run.id
        || call.id != saved.target.call_id
        || crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
            != saved.call_binding
        || !valid_status
        || result.content != output.to_content_blocks()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
