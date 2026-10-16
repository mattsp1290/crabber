//! One fresh owner consumes the complete accepted response set atomically.
use super::super::{SqliteExecution, SqliteStore};
use super::{Error, approval, finish, load, sql, write};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalGrant,
    ApprovalOutcome, AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome,
    AttemptState,
};
use crabber_core::{Run, RunFence, RunStatus};
use sqlx::{Sqlite, Transaction};

impl SqliteStore {
    pub(in super::super) async fn claim_approval(
        &self,
        claim: &ApprovalContinuationClaim,
    ) -> Result<ApprovalClaimOutcome, Error> {
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let (mut tx, run) = self
            .approval_transaction(&claim.session_id, &claim.run_id)
            .await?;
        let result = self
            .claim_approval_guarded(&mut tx, &run, claim, None)
            .await;
        sql::after_commit(finish(tx, result).await, fault)
    }
    pub(in crate::sqlite) async fn claim_approval_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        claim: &ApprovalContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest = claim.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;

        if let Some(saved) = write::replay(tx, run, &claim.id, &digest).await? {
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
        let (mut record, calls) = ready(tx, run, claim).await?;
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

        super::super::records::save_run_with_completion(tx, &activated, completion.as_ref())
            .await
            .map_err(sql::unknown)?;
        let execution = SqliteExecution {
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
            .apply_claim_attempt(tx, &activated, &reserve)
            .await
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
        write::save(tx, &record).await?;
        let receipt = write::publish(
            tx,
            run,
            claim.id.clone(),
            digest,
            ApprovalOutcome::Claimed {
                continuation: claim.continuation.clone(),
                attempt: receipt.attempt,
            },
            now,
        )
        .await?;
        sql::before_commit(fault)?;
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
pub(in crate::sqlite) async fn ready(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    claim: &ApprovalContinuationClaim,
) -> Result<(approval::ApprovalRecord, Vec<crate::GrantedApproval>), Error> {
    let (record, _) = load(tx, &run.session_id, &run.id)
        .await?
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
    let old = super::super::attempt::load_owned(tx, &run.id, &record.batch.attempt.id)
        .await
        .map_err(approval::attempt_error)?
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
    let calls = record.grants()?;
    super::suspend::validate_calls(tx, run, &record.batch).await?;
    Ok((record, calls))
}
