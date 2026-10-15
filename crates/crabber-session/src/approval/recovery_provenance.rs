//! Recovery provenance is shared by adapters and their private fault fixtures.
use crate::approval::ApprovalRecord;
use crate::{
    ApprovalContinuationRecovery, ApprovalContinuationState, ApprovalError as Error,
    ApprovalOutcome, ApprovalReceipt, AttemptCleanup, AttemptState, GrantedApproval,
};
use crabber_core::{Run, RunStatus};
use time::OffsetDateTime;
pub(crate) fn activate_recovery(
    request: &ApprovalContinuationRecovery,
    run: &Run,
    now: OffsetDateTime,
) -> Result<Run, Error> {
    let mut activated = run.clone();
    activated.owner.clone_from(&request.owner);
    activated.claim_token = uuid::Uuid::new_v4().to_string();
    activated.lease_until = now
        .checked_add(time::Duration::milliseconds(
            i64::try_from(request.lease_ms).map_err(|_| Error::InvalidRequest)?,
        ))
        .ok_or(Error::InvalidRequest)?;
    activated.updated_at = now;
    Ok(activated)
}
pub(crate) fn verify_recovery_original(
    request: &ApprovalContinuationRecovery,
    receipt: &ApprovalReceipt,
) -> Result<(), Error> {
    if receipt.operation_id != request.original.id
        || receipt.session_id != request.original.session_id
        || receipt.run_id != request.original.run_id
        || receipt.semantic_digest != request.original.fingerprint()?
    {
        return Err(Error::SemanticConflict);
    }
    let ApprovalOutcome::Claimed {
        continuation,
        attempt,
    } = &receipt.outcome
    else {
        return Err(Error::SemanticConflict);
    };
    if *continuation != request.original.continuation
        || attempt.target != request.attempt
        || attempt.session_id != request.original.session_id
        || attempt.run_id != request.original.run_id
        || attempt.state != AttemptState::Unstarted
        || attempt.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
impl ApprovalRecord {
    pub(crate) fn verify_recovery_batch(&self, receipt: &ApprovalReceipt) -> Result<(), Error> {
        if receipt.session_id != self.session_id
            || receipt.run_id != self.run_id
            || receipt.operation_id != self.batch.id
            || receipt.semantic_digest != self.batch.fingerprint(&self.session_id, &self.run_id)?
            || receipt.revision <= self.batch.expected_revision
            || receipt.outcome
                != (ApprovalOutcome::Suspended {
                    continuation: self.batch.continuation.clone(),
                })
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub(crate) fn recovery_grants(
        &self,
        run: &Run,
        request: &ApprovalContinuationRecovery,
        original: &ApprovalReceipt,
        now: OffsetDateTime,
    ) -> Result<Vec<GrantedApproval>, Error> {
        self.validate(&request.original.session_id, &request.original.run_id)?;
        verify_recovery_original(request, original)?;
        if self.state != ApprovalContinuationState::Claimed || run.status != RunStatus::Running {
            return Err(Error::InvalidTransition);
        }
        if self.batch.continuation != request.original.continuation
            || self.batch.next_attempt != request.attempt
        {
            return Err(Error::GenerationConflict);
        }
        if run.plan_fingerprint != request.original.expected_plan.as_str()
            || self.batch.expected_plan != request.original.expected_plan
            || run.usage != self.batch.usage
            || run.checkpoint.as_ref()
                != Some(
                    &serde_json::json!({"version":4,"approval":self.batch.continuation,"turns_used":self.batch.turns_used}),
                )
        {
            return Err(Error::SemanticConflict);
        }
        if run.lease_until > now {
            return Err(Error::LiveOwner);
        }
        self.grants()
    }
}

pub(crate) fn verify_recovery_answer(
    answer: &crate::ApprovalAnswer,
    receipt: &ApprovalReceipt,
) -> Result<(), Error> {
    let state = match answer.reply {
        crate::ApprovalReply::Grant => crabber_core::InteractionState::Answered,
        crate::ApprovalReply::Deny => crabber_core::InteractionState::Denied,
    };
    if receipt.session_id != answer.session_id
        || receipt.run_id != answer.run_id
        || receipt.operation_id != answer.id
        || receipt.semantic_digest != answer.fingerprint()?
        || receipt.revision <= answer.expected_revision
        || receipt.outcome
            != (ApprovalOutcome::Answered {
                continuation: answer.continuation.clone(),
                interaction_id: answer.interaction_id.clone(),
                state,
            })
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
