//! Exact expired-Unstarted approval recovery in the session mutation transaction.
use super::super::{SqliteExecution, SqliteStore};
use super::{Error, approval, finish, sql, write};
use crate::native_enrollment::caller::{ApprovalCompletion, ApprovalInput, ApprovalResult, Caller};
use crate::public_session::v2::proof::Proof;
use crate::transaction::Transaction;
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationRecovery, ApprovalOutcome, AttemptCleanup,
    AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState,
};
use crabber_core::{Run, RunFence};
use sqlx::Sqlite;
impl SqliteStore {
    pub(in super::super) async fn recover_approval(
        &self,
        request: &ApprovalContinuationRecovery,
    ) -> Result<ApprovalClaimOutcome, Error> {
        self.recover_approval_with_proof(request, &mut Proof::bounded(32 << 20))
            .await
    }
    pub(in crate::sqlite) async fn recover_approval_with_proof(
        &self,
        request: &ApprovalContinuationRecovery,
        proof: &mut Proof,
    ) -> Result<ApprovalClaimOutcome, Error> {
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let original = &request.original;
        let (mut tx, run) = self
            .approval_transaction_with_proof(&original.session_id, &original.run_id, proof)
            .await?;
        let result = self
            .recover_approval_guarded(&mut tx, &run, request, None, Caller::Raw, proof)
            .await;
        sql::after_commit(finish(tx, result).await, fault)
    }

    pub(in crate::sqlite) async fn recover_approval_guarded(
        &self,
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &ApprovalContinuationRecovery,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest =
            super::native_caller::digest(tx, ApprovalInput::Recovery(request), read_proof).await?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;

        let original = &request.original;
        if let Some(saved) =
            super::native_caller::replay(tx, run, &request.id, &digest, read_proof).await?
        {
            request.verify_receipt(&saved)?;
            return Ok((ApprovalClaimOutcome::Replayed(saved), false));
        }
        let (expected_revision, completion) = crate::native_parent::CompletionSource::control(
            preparation,
            crate::native_parent::CompletionOperation::ApprovalRecovery,
            &crate::session_owner::preparation::PreparationKey::Approval(request.id.clone()),
            (&original.session_id, &original.run_id),
            request.expected_revision,
            &digest,
        )
        .map_err(|_| Error::SemanticConflict)?;
        if write::revision(tx, &run.session_id).await? != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let now = self.clock.now();
        let (record, calls) = ready(tx, run, request, now, read_proof).await?;
        read_proof
            .measure(run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let activated = approval::activate_recovery(request, run, now)?;
        let fence = RunFence {
            run_id: run.id.clone(),
            claim_token: activated.claim_token.clone(),
        };

        super::activation::save(tx, &activated, completion.as_ref(), preparation, read_proof)
            .await?;
        let execution = SqliteExecution {
            store: self.clone(),
            fence: fence.clone(),
        };
        let (mutation, native, authority) = reclaim(
            tx,
            &execution,
            &activated,
            request,
            expected_revision,
            read_proof,
        )
        .await?;
        read_proof
            .measure(&native.attempt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let saved = write::publish(
            tx,
            run,
            request.id.clone(),
            digest,
            ApprovalOutcome::ReclaimedUnstarted {
                continuation: original.continuation.clone(),
                original_claim_id: original.id.clone(),
                attempt: native.attempt.clone(),
            },
            now,
        )
        .await?;
        request.verify_receipt(&saved)?;
        let outcome = super::native_caller::issue(
            tx,
            caller,
            ApprovalCompletion {
                input: ApprovalInput::Recovery(request),
                previous: run,
                activated: &activated,
                record: &record,
                native_request: &mutation,
                native_receipt: &native,
            },
            ApprovalResult {
                receipt: saved,
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
async fn validate_native_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &ApprovalContinuationRecovery,
    record: &approval::ApprovalRecord,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
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
        || old.snapshot.session_id != run.session_id
        || old.snapshot.state != AttemptState::Suspended
        || old.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    let latest: Option<i64> =
        sqlx::query_scalar("SELECT MAX(generation) FROM execution_attempts WHERE run_id=$1")
            .bind(&run.id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(super::super::db)
            .map_err(sql::unknown)?;
    if latest
        != Some(i64::try_from(request.attempt.generation.get()).map_err(|_| Error::InvalidRequest)?)
    {
        return Err(Error::GenerationConflict);
    }
    let (current, _) = super::super::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        Some(&request.attempt.id),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::NotFound)?;
    if current.snapshot.target != request.attempt
        || current.snapshot.session_id != run.session_id
        || current.snapshot.state != AttemptState::Unstarted
        || current.snapshot.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::InvalidTransition);
    }
    if current.lease_until > now {
        return Err(Error::LiveOwner);
    }
    let phases: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND attempt_id=$2)",
    )
    .bind(&run.id.0)
    .bind(&request.attempt.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(super::super::db)
    .map_err(sql::unknown)?;
    if phases {
        return Err(Error::InvalidTransition);
    }
    Ok(())
}

pub(super) async fn validate_provenance(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    validate_provenance_with_proof(tx, record, &mut Proof::legacy()).await
}
pub(super) async fn validate_provenance_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    let saved = super::receipt_with_proof(tx, &record.session_id, &record.batch.id, proof)
        .await
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
        .ok_or(Error::UnknownStoreOutcome)?;
    record.verify_recovery_batch(&saved)?;
    for question in &record.questions {
        let answer = question.answer.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        let saved = super::receipt_with_proof(tx, &record.session_id, &answer.id, proof)
            .await
            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
            .ok_or(Error::UnknownStoreOutcome)?;
        approval::verify_recovery_answer(answer, &saved)?;
    }
    Ok(())
}

pub(in crate::sqlite) async fn preparation_context(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &ApprovalContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    ready(tx, run, request, now, proof).await?;
    Ok(())
}

async fn ready(
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    request: &ApprovalContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(approval::ApprovalRecord, Vec<crate::GrantedApproval>), Error> {
    let original = &request.original;
    let (record, _) = super::load_with_proof(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
        .ok_or(Error::InvalidTransition)?;
    write::active(tx, &record).await?;
    let parent = super::receipt_with_proof(tx, &run.session_id, &original.id, proof)
        .await
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
        .ok_or(Error::NotFound)?;
    proof
        .measure(&record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let calls = record.recovery_grants(run, request, &parent, now)?;
    validate_provenance_with_proof(tx, &record, proof).await?;
    validate_native_with_proof(tx, run, request, &record, now, proof).await?;
    super::suspend::validate_calls_with_proof(tx, run, &record.batch, proof).await?;

    Ok((record, calls))
}

async fn reclaim(
    tx: &mut Transaction<'_, Sqlite>,
    execution: &SqliteExecution,
    activated: &Run,
    request: &ApprovalContinuationRecovery,

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
    let mutation = AttemptMutation {
        id: approval::derived_id("reclaim-attempt", &request.id),
        expected_revision,
        command: AttemptCommand::ReclaimUnstarted {
            target: request.attempt.clone(),
        },
    };
    proof
        .measure(&mutation)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let AttemptMutationOutcome::Applied {
        receipt: native,
        authority: Some(authority),
    } = execution
        .apply_attempt_with_proof(tx, activated, &mutation, None, None, proof)
        .await
        .map_err(approval::attempt_error)?
    else {
        return Err(Error::UnknownStoreOutcome);
    };

    Ok((mutation, native, authority))
}
