//! Exact expired-Unstarted approval recovery in the session mutation transaction.
use super::super::{PostgresExecution, PostgresStore};
use super::{Error, approval, finish, load, receipt, sql, write};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationRecovery, ApprovalGrant, ApprovalOutcome,
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState,
};
use crabber_core::{Run, RunFence};
use sqlx::{Postgres, Transaction};
impl PostgresStore {
    pub(in super::super) async fn recover_approval(
        &self,
        request: &ApprovalContinuationRecovery,
    ) -> Result<ApprovalClaimOutcome, Error> {
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let original = &request.original;
        let (mut tx, run) = self
            .approval_transaction(&original.session_id, &original.run_id)
            .await?;
        let result = self
            .recover_approval_guarded(&mut tx, &run, request, None)
            .await;
        sql::after_commit(finish(tx, result).await, fault)
    }

    pub(in crate::postgres) async fn recover_approval_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        run: &Run,
        request: &ApprovalContinuationRecovery,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest = request.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;

        let original = &request.original;
        if let Some(saved) = write::replay(tx, run, &request.id, &digest).await? {
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
        let (record, _) = load(tx, &run.session_id, &run.id)
            .await?
            .ok_or(Error::InvalidTransition)?;
        write::active(tx, &record).await?;
        let parent = receipt(tx, &run.session_id, &original.id)
            .await?
            .ok_or(Error::NotFound)?;
        let now = self.clock.now();
        let calls = record.recovery_grants(run, request, &parent, now)?;
        validate_provenance(tx, &record).await?;
        validate_native(tx, run, request, &record, now).await?;
        super::suspend::validate_calls(tx, run, &record.batch).await?;
        let activated = approval::activate_recovery(request, run, now)?;
        let fence = RunFence {
            run_id: run.id.clone(),
            claim_token: activated.claim_token.clone(),
        };

        super::super::save_run_with_completion(tx, &activated, completion.as_ref())
            .await
            .map_err(sql::unknown)?;
        let execution = PostgresExecution {
            store: self.clone(),
            fence: fence.clone(),
        };
        let mutation = AttemptMutation {
            id: approval::derived_id("reclaim-attempt", &request.id),
            expected_revision,
            command: AttemptCommand::ReclaimUnstarted {
                target: request.attempt.clone(),
            },
        };
        let AttemptMutationOutcome::Applied {
            receipt: native,
            authority: Some(authority),
        } = execution
            .apply_attempt(tx, &activated, &mutation, None, None)
            .await
            .map_err(approval::attempt_error)?
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let saved = write::publish(
            tx,
            run,
            request.id.clone(),
            digest,
            ApprovalOutcome::ReclaimedUnstarted {
                continuation: original.continuation.clone(),
                original_claim_id: original.id.clone(),
                attempt: native.attempt,
            },
            now,
        )
        .await?;
        request.verify_receipt(&saved)?;
        sql::before_commit(fault)?;
        Ok((
            ApprovalClaimOutcome::Applied {
                receipt: saved,
                grant: Box::new(ApprovalGrant {
                    fence,
                    authority,
                    calls,
                    source_turn_id: record.batch.source_turn_id,
                    usage: record.batch.usage,
                    turns_used: record.batch.turns_used.ok_or(Error::UnknownStoreOutcome)?,
                }),
            },
            true,
        ))
    }
}
async fn validate_native(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &ApprovalContinuationRecovery,
    record: &approval::ApprovalRecord,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let old = super::super::attempt::load_owned(tx, &run.id, &record.batch.attempt.id)
        .await
        .map_err(approval::attempt_error)?
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
    let current = super::super::attempt::load_owned(tx, &run.id, &request.attempt.id)
        .await
        .map_err(approval::attempt_error)?
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
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let saved = receipt(tx, &record.session_id, &record.batch.id)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    record.verify_recovery_batch(&saved)?;
    for question in &record.questions {
        let answer = question.answer.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        let saved = receipt(tx, &record.session_id, &answer.id)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        approval::verify_recovery_answer(answer, &saved)?;
    }
    Ok(())
}

pub(in crate::postgres) async fn preparation_context(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &ApprovalContinuationRecovery,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let original = &request.original;
    let (record, _) = load(tx, &run.session_id, &run.id)
        .await?
        .ok_or(Error::InvalidTransition)?;
    write::active(tx, &record).await?;
    let parent = receipt(tx, &run.session_id, &original.id)
        .await?
        .ok_or(Error::NotFound)?;

    let _calls = record.recovery_grants(run, request, &parent, now)?;
    validate_provenance(tx, &record).await?;
    validate_native(tx, run, request, &record, now).await?;
    super::suspend::validate_calls(tx, run, &record.batch).await?;

    Ok(())
}
