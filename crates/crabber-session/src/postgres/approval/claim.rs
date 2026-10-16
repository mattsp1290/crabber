//! One fresh owner consumes the complete accepted response set atomically.
use super::super::{PostgresExecution, PostgresStore};
use super::{Error, approval, finish, load, sql, write};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalGrant,
    ApprovalOutcome, AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome,
    AttemptState,
};
use crabber_core::{Run, RunFence, RunStatus};
use sqlx::{Postgres, Transaction};

impl PostgresStore {
    pub(in super::super) async fn claim_approval(
        &self,
        claim: &ApprovalContinuationClaim,
    ) -> Result<ApprovalClaimOutcome, Error> {
        let digest = claim.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let (mut tx, run) = self
            .approval_transaction(&claim.session_id, &claim.run_id)
            .await?;
        let result = async {
            if let Some(saved) = write::replay(&mut tx, &run, &claim.id, &digest).await? {
                return Ok((ApprovalClaimOutcome::Replayed(saved), false));
            }
            if write::revision(&mut tx, &run.session_id).await? != claim.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let (mut record, calls) = ready(&mut tx, &run, claim).await?;
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
            super::super::save_run(&mut tx, &activated)
                .await
                .map_err(sql::unknown)?;
            let execution = PostgresExecution {
                store: self.clone(),
                fence: fence.clone(),
            };
            let reserve = AttemptMutation {
                id: approval::derived_id("claim-attempt", &claim.id),
                expected_revision: claim.expected_revision,
                command: AttemptCommand::Reserve {
                    target: record.batch.next_attempt.clone(),
                },
            };
            let applied = execution
                .apply_claim_attempt(&mut tx, &activated, &reserve)
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
            write::save(&mut tx, &record).await?;
            let receipt = write::publish(
                &mut tx,
                &run,
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
        .await;
        sql::after_commit(finish(tx, result).await, fault)
    }
}
async fn ready(
    tx: &mut Transaction<'_, Postgres>,
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
