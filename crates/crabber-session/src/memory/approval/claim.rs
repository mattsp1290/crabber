//! Consumes a complete accepted response set together with its single next owner.
use super::{Error, publish, revision};
use crate::native_enrollment::caller::{ApprovalCompletion, ApprovalInput, ApprovalResult, Caller};
use crate::public_session::v2::proof::Proof;
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalOutcome,
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState,
    approval,
};
use crabber_core::{RunFence, RunStatus};

impl super::super::MemoryStore {
    pub(in super::super) fn claim_approval(
        &self,
        claim: &ApprovalContinuationClaim,
    ) -> Result<ApprovalClaimOutcome, Error> {
        self.claim_approval_with_proof(claim, &mut Proof::bounded(32 << 20))
    }
    pub(in crate::memory) fn claim_approval_with_proof(
        &self,
        claim: &ApprovalContinuationClaim,
        proof: &mut Proof,
    ) -> Result<ApprovalClaimOutcome, Error> {
        let fault = super::fault(self);
        let result = self.transact_selected(
            proof,
            |state, proof| super::native_caller::before(state, ApprovalInput::Claim(claim), proof),
            |state, proof| self.claim_approval_guarded(state, claim, None, Caller::Raw, proof),
        );
        super::after_commit(result, fault)
    }
    pub(in crate::memory) fn claim_approval_guarded(
        &self,
        state: &mut super::super::State,
        claim: &ApprovalContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut Proof,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest = super::native_caller::digest(state, ApprovalInput::Claim(claim), read_proof)?;
        let fault = super::fault(self);

        if let Some(saved) = super::replay_with_proof(
            state,
            &claim.session_id,
            &claim.run_id,
            &claim.id,
            &digest,
            read_proof,
        )? {
            return Ok((ApprovalClaimOutcome::Replayed(saved), false));
        }
        let run =
            super::native_caller::selected_run(state, ApprovalInput::Claim(claim), read_proof)?;
        let (expected_revision, completion) = crate::native_parent::CompletionSource::control(
            preparation,
            crate::native_parent::CompletionOperation::ApprovalClaim,
            &crate::session_owner::preparation::PreparationKey::Approval(claim.id.clone()),
            (&claim.session_id, &claim.run_id),
            claim.expected_revision,
            &digest,
        )
        .map_err(|_| Error::SemanticConflict)?;
        if revision(state, &run.session_id) != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let (mut record, calls) = ready_with_proof(state, &run, claim, read_proof)?;
        let now = self.clock.now();
        let (activated, fence) = activate(
            state,
            &run,
            claim,
            now,
            completion.as_ref(),
            preparation,
            read_proof,
        )?;
        let execution = super::super::MemoryExecution {
            store: self.clone(),
            fence: fence.clone(),
        };
        let (reserve, native, authority) = reserve(
            state,
            &execution,
            claim,
            &record.batch.next_attempt,
            expected_revision,
            read_proof,
        )?;
        record.state = ApprovalContinuationState::Claimed;
        for question in &mut record.questions {
            question.view.consumed = true;
        }
        read_proof
            .measure(&record)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        state
            .approvals
            .insert(claim.continuation.id.clone(), record.clone());
        read_proof
            .measure(&native.attempt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let receipt = publish(
            state,
            &run,
            claim.id.clone(),
            digest.clone(),
            ApprovalOutcome::Claimed {
                continuation: claim.continuation.clone(),
                attempt: native.attempt.clone(),
            },
            now,
        )?;
        let outcome = super::native_caller::issue(
            state,
            caller,
            ApprovalCompletion {
                input: ApprovalInput::Claim(claim),
                previous: &run,
                activated: &activated,
                record: &record,
                native_request: &reserve,
                native_receipt: &native,
            },
            ApprovalResult {
                receipt,
                fence,
                authority,
                calls,
            },
            read_proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )?;
        super::before_commit(fault)?;
        Ok((outcome, true))
    }
}

pub(in crate::memory) fn ready_with_proof(
    state: &super::super::State,
    run: &crabber_core::Run,
    claim: &ApprovalContinuationClaim,
    proof: &mut Proof,
) -> Result<(approval::ApprovalRecord, Vec<crate::GrantedApproval>), Error> {
    let current = state
        .approval_barriers
        .get(&run.session_id)
        .ok_or(Error::InvalidTransition)?;
    let record = state
        .approvals
        .get(current)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let record = record.clone();
    super::verify_record_with_proof(state, run, &record, proof)?;
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
    let calls = validate_calls_with_proof(state, run, &record, proof)?;
    Ok((record, calls))
}

pub(super) fn validate_calls_with_proof(
    state: &super::super::State,
    run: &crabber_core::Run,
    record: &approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<Vec<crate::GrantedApproval>, Error> {
    proof
        .measure(record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
        proof
            .measure(current)
            .map_err(|_| Error::UnknownStoreOutcome)?;
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

fn activate(
    state: &mut super::super::State,
    run: &crabber_core::Run,
    claim: &ApprovalContinuationClaim,
    now: time::OffsetDateTime,
    completion: Option<&crate::native_parent::CompletionSource>,
    preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    proof: &mut Proof,
) -> Result<(crabber_core::Run, RunFence), Error> {
    proof.measure(run).map_err(|_| Error::UnknownStoreOutcome)?;
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

    super::activation::save(state, run, &activated, completion, preparation, proof)?;
    proof
        .measure(&activated)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state.runs.insert(run.id.clone(), activated.clone());

    Ok((activated, fence))
}

fn reserve(
    state: &mut super::super::State,
    execution: &super::super::MemoryExecution,

    claim: &ApprovalContinuationClaim,
    target: &crate::AttemptTarget,
    expected_revision: crabber_core::SessionRevision,
    proof: &mut Proof,
) -> Result<
    (
        AttemptMutation,
        crate::AttemptReceipt,
        crate::AttemptAuthority,
    ),
    Error,
> {
    let reserve = AttemptMutation {
        id: approval::derived_id("claim-attempt", &claim.id),
        expected_revision,
        command: AttemptCommand::Reserve {
            target: target.clone(),
        },
    };
    proof
        .measure(&reserve)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let applied = execution
        .apply_claim_attempt_with_proof(state, reserve.clone(), proof)
        .map_err(approval::attempt_error)?;
    let AttemptMutationOutcome::Applied {
        receipt: native,
        authority: Some(authority),
    } = applied
    else {
        return Err(Error::SemanticConflict);
    };

    Ok((reserve, native, authority))
}
