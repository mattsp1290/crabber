//! One fresh owner consumes the complete accepted response set atomically.
use super::super::{SqliteExecution, SqliteStore};
use super::{Error, approval, finish, sql, write};
use crate::native_enrollment::caller::{ApprovalCompletion, ApprovalInput, ApprovalResult, Caller};
use crate::public_session::v2::proof::Proof;
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalOutcome,
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState,
};
use crabber_core::{Run, RunFence, RunStatus};
use sqlx::{Sqlite, Transaction};

impl SqliteStore {
    pub(in super::super) async fn claim_approval(
        &self,
        claim: &ApprovalContinuationClaim,
    ) -> Result<ApprovalClaimOutcome, Error> {
        self.claim_approval_with_proof(claim, &mut Proof::bounded(32 << 20))
            .await
    }
    pub(in crate::sqlite) async fn claim_approval_with_proof(
        &self,
        claim: &ApprovalContinuationClaim,
        proof: &mut Proof,
    ) -> Result<ApprovalClaimOutcome, Error> {
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let (mut tx, run) = self
            .approval_transaction_with_proof(&claim.session_id, &claim.run_id, proof)
            .await?;
        let result = self
            .claim_approval_guarded(&mut tx, &run, claim, None, Caller::Raw, proof)
            .await;
        sql::after_commit(finish(tx, result).await, fault)
    }
    pub(in crate::sqlite) async fn claim_approval_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        claim: &ApprovalContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest =
            super::native_caller::digest(tx, ApprovalInput::Claim(claim), read_proof).await?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;

        if let Some(saved) =
            super::native_caller::replay(tx, run, &claim.id, &digest, read_proof).await?
        {
            return Ok((ApprovalClaimOutcome::Replayed(saved), false));
        }
        let (expected_revision, completion) = crate::native_parent::CompletionSource::control(
            preparation,
            crate::native_parent::CompletionOperation::ApprovalClaim,
            &crate::session_owner::preparation::PreparationKey::Approval(claim.id.clone()),
            (&claim.session_id, &claim.run_id),
            claim.expected_revision,
            &digest,
        )
        .map_err(|_| Error::SemanticConflict)?;
        if write::revision(tx, &run.session_id).await? != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let (mut record, calls) = ready_with_proof(tx, run, claim, read_proof).await?;
        let now = self.clock.now();
        let (activated, fence) = activate(
            tx,
            run,
            claim,
            now,
            completion.as_ref(),
            preparation,
            read_proof,
        )
        .await?;
        let execution = SqliteExecution {
            store: self.clone(),
            fence: fence.clone(),
        };
        let (reserve, native, authority) = reserve(
            tx,
            &execution,
            &activated,
            claim,
            &record.batch.next_attempt,
            expected_revision,
            read_proof,
        )
        .await?;
        record.state = ApprovalContinuationState::Claimed;
        for question in &mut record.questions {
            question.view.consumed = true;
        }
        write::save(tx, &record).await?;
        read_proof
            .measure(&native.attempt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let receipt = write::publish(
            tx,
            run,
            claim.id.clone(),
            digest,
            ApprovalOutcome::Claimed {
                continuation: claim.continuation.clone(),
                attempt: native.attempt.clone(),
            },
            now,
        )
        .await?;
        let outcome = super::native_caller::issue(
            tx,
            caller,
            ApprovalCompletion {
                input: ApprovalInput::Claim(claim),
                previous: run,
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
        )
        .await?;
        sql::before_commit(fault)?;
        Ok((outcome, true))
    }
}
pub(in crate::sqlite) async fn ready_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    claim: &ApprovalContinuationClaim,
    proof: &mut Proof,
) -> Result<(approval::ApprovalRecord, Vec<crate::GrantedApproval>), Error> {
    let (record, _) = super::load_with_proof(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
        .ok_or(Error::InvalidTransition)?;
    write::active(tx, &record).await?;
    if record.batch.continuation != claim.continuation {
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
    let (old, _) = super::super::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        Some(&record.batch.attempt.id),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::NotFound)?;
    if old.snapshot.target != record.batch.attempt
        || old.snapshot.state != AttemptState::Suspended
        || old.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    if run.usage != record.batch.usage {
        return Err(Error::SemanticConflict);
    }
    proof
        .measure(&record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let calls = record.grants()?;
    super::suspend::validate_calls_with_proof(tx, run, &record.batch, proof).await?;
    Ok((record, calls))
}

async fn activate(
    tx: &mut Transaction<'_, Sqlite>,
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

    super::activation::save(tx, &activated, completion, preparation, proof).await?;

    Ok((activated, fence))
}

async fn reserve(
    tx: &mut Transaction<'_, Sqlite>,
    execution: &SqliteExecution,
    activated: &Run,
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
        .apply_claim_attempt(tx, activated, &reserve, proof)
        .await
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
