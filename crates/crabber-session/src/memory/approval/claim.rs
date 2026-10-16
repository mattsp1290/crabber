//! Consumes a complete accepted response set together with its single next owner.
use super::{Error, publish, replay, revision};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalGrant,
    ApprovalOutcome, AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome,
    AttemptState, approval,
};
use crabber_core::{RunFence, RunStatus};

impl super::super::MemoryStore {
    pub(in super::super) fn claim_approval(
        &self,
        claim: &ApprovalContinuationClaim,
    ) -> Result<ApprovalClaimOutcome, Error> {
        let fault = super::fault(self);
        let result = self.transact(|state| self.claim_approval_guarded(state, claim, None));
        super::after_commit(result, fault)
    }
    pub(in crate::memory) fn claim_approval_guarded(
        &self,
        state: &mut super::super::State,
        claim: &ApprovalContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest = claim.fingerprint()?;
        let fault = super::fault(self);

        if let Some(saved) = replay(state, &claim.session_id, &claim.run_id, &claim.id, &digest)? {
            return Ok((ApprovalClaimOutcome::Replayed(saved), false));
        }
        let run = state
            .runs
            .get(&claim.run_id)
            .filter(|run| run.session_id == claim.session_id)
            .cloned()
            .ok_or(Error::NotFound)?;
        let expected_revision = preparation
            .map_or(Ok(claim.expected_revision), |proof| {
                proof.control_revision(
                    &crate::session_owner::preparation::PreparationKey::Approval(claim.id.clone()),
                    &claim.session_id,
                    &claim.run_id,
                    claim.expected_revision,
                )
            })
            .map_err(|_| Error::SemanticConflict)?;
        if revision(state, &run.session_id) != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let (mut record, calls) = ready(state, &run, claim)?;
        let now = self.clock.now();
        let mut activated = run.clone();
        activated.owner.clone_from(&claim.owner);
        activated.claim_token = uuid::Uuid::new_v4().to_string();
        activated.lease_until = now
            .checked_add(time::Duration::milliseconds(
                i64::try_from(claim.lease_ms).map_err(|_| Error::InvalidRequest)?,
            ))
            .ok_or(Error::InvalidRequest)?;
        activated.updated_at = now;
        let fence = RunFence {
            run_id: run.id.clone(),
            claim_token: activated.claim_token.clone(),
        };
        super::super::native_parent::record_run(state, Some(&run), &activated)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        state.runs.insert(run.id.clone(), activated);
        let execution = super::super::MemoryExecution {
            store: self.clone(),
            fence: fence.clone(),
        };
        let reserve = AttemptMutation {
            id: approval::derived_id("claim-attempt", &claim.id),
            expected_revision,
            command: AttemptCommand::Reserve {
                target: record.batch.next_attempt.clone(),
            },
        };
        let applied = execution
            .apply_claim_attempt(state, reserve)
            .map_err(approval::attempt_error)?;
        let AttemptMutationOutcome::Applied {
            receipt,
            authority: Some(authority),
        } = applied
        else {
            return Err(Error::SemanticConflict);
        };
        record.state = ApprovalContinuationState::Claimed;
        for question in &mut record.questions {
            question.view.consumed = true;
        }
        state
            .approvals
            .insert(claim.continuation.id.clone(), record.clone());
        let receipt = publish(
            state,
            &run,
            claim.id.clone(),
            digest.clone(),
            ApprovalOutcome::Claimed {
                continuation: claim.continuation.clone(),
                attempt: receipt.attempt,
            },
            now,
        )?;
        super::before_commit(fault)?;
        Ok((
            ApprovalClaimOutcome::Applied {
                receipt,
                grant: Box::new(ApprovalGrant {
                    fence,
                    authority,
                    calls,
                    source_turn_id: record.batch.source_turn_id.clone(),
                    usage: record.batch.usage.clone(),
                    turns_used: record.batch.turns_used.ok_or(Error::UnknownStoreOutcome)?,
                }),
            },
            true,
        ))
    }
}

pub(in crate::memory) fn ready(
    state: &super::super::State,
    run: &crabber_core::Run,
    claim: &ApprovalContinuationClaim,
) -> Result<(approval::ApprovalRecord, Vec<crate::GrantedApproval>), Error> {
    let current = state
        .approval_barriers
        .get(&run.session_id)
        .ok_or(Error::InvalidTransition)?;
    let record = state
        .approvals
        .get(current)
        .cloned()
        .ok_or(Error::UnknownStoreOutcome)?;
    super::verify_record(state, run, &record)?;
    if record.batch.continuation != claim.continuation || record.run_id != run.id {
        return Err(Error::GenerationConflict);
    }
    if claim.expected_plan.as_str() != run.plan_fingerprint
        || record.batch.expected_plan != claim.expected_plan
    {
        return Err(Error::SemanticConflict);
    }
    if record.state != ApprovalContinuationState::Waiting || run.status != RunStatus::Paused {
        return Err(Error::InvalidTransition);
    }
    if run.checkpoint.as_ref()
        != Some(
            &serde_json::json!({"version":4,"approval":record.batch.continuation,"turns_used":record.batch.turns_used}),
        )
    {
        return Err(Error::SemanticConflict);
    }
    let old = state
        .attempts
        .get(&record.batch.attempt.id)
        .ok_or(Error::NotFound)?;
    if old.snapshot.state != AttemptState::Suspended
        || old.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    if run.usage != record.batch.usage {
        return Err(Error::SemanticConflict);
    }
    let calls = validate_calls(state, run, &record)?;
    Ok((record, calls))
}

pub(super) fn validate_calls(
    state: &super::super::State,
    run: &crabber_core::Run,
    record: &approval::ApprovalRecord,
) -> Result<Vec<crate::GrantedApproval>, Error> {
    let calls = record.grants()?;
    let unfinished = state
        .calls
        .values()
        .filter(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    crabber_core::ToolCallStatus::Pending | crabber_core::ToolCallStatus::Running
                )
        })
        .count();
    if unfinished != calls.len() {
        return Err(Error::InvalidTransition);
    }
    for call in &calls {
        let current = state
            .calls
            .get(&call.target.call_id)
            .ok_or(Error::NotFound)?;
        if current.run_id != run.id
            || current.status != crabber_core::ToolCallStatus::Pending
            || current.result.is_some()
            || crate::phase::call_binding(current).map_err(|_| Error::UnknownStoreOutcome)?
                != call.call_binding
        {
            return Err(Error::SemanticConflict);
        }
    }
    Ok(calls)
}
