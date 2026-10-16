use super::{
    Error, SqliteStore, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationOutcome, ToolContinuationRecord, ToolContinuationState, after, before, domain,
    evidence, fault, finish, revision, unknown, write,
};
use crate::{AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, ToolContinuationGrant};
use crabber_core::{Run, RunFence, RunStatus};
impl SqliteStore {
    pub(in super::super) async fn claim_tools(
        &self,
        claim: ToolContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let digest = claim.fingerprint()?;
        let fault = fault(self);
        let (mut tx, run) = self
            .tool_transaction(&claim.session_id, &claim.run_id)
            .await?;
        let result = async {
            if let Some(saved) = write::replay(&mut tx, &run, &claim.id, &digest).await? {
                return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
            }
            if revision(&mut tx, &run.session_id).await? != claim.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let mut record = ready(&mut tx, &run, &claim).await?;
            let now = self.clock.now();
            let activated =
                domain::canonical::activated_run(&run, &claim.owner, claim.lease_ms, now)?;
            let (previous, _) =
                super::super::attempt::load_host_attempt(&mut tx, &run.id, None, true)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::NotFound)?;
            let reserve = AttemptMutation {
                id: domain::derived_id("tool-continuation-claim-attempt", &claim.id),
                expected_revision: claim.expected_revision,
                command: AttemptCommand::Reserve {
                    target: record.batch.next_attempt.clone(),
                },
            };
            let parent = crate::attempt::AttemptRecord::reserve(
                &activated,
                &reserve,
                Some(&previous.snapshot),
            )
            .map_err(|_| Error::CleanupBlocked)?;
            let authority = parent.authority().map_err(unknown)?;
            super::super::records::save_run(&mut tx, &activated)
                .await
                .map_err(unknown)?;
            super::super::attempt::save_host_attempt(&mut tx, &parent, true)
                .await
                .map_err(unknown)?;
            let phases =
                super::phases::claim(&mut tx, &run, &activated, &parent, &record, &authority)
                    .await?;
            let calls = record.grants()?;
            record.state = ToolContinuationState::Claimed;
            record.claim = Some(claim.clone());
            for question in &mut record.questions {
                question.view.consumed = true;
            }
            write::save(&mut tx, &record, false).await?;
            let receipt = write::publish(
                &mut tx,
                &run,
                claim.id.clone(),
                digest,
                ToolContinuationOutcome::Claimed {
                    continuation: record.batch.continuation.clone(),
                    attempt: parent.snapshot,
                },
                now,
            )
            .await?;
            before(fault)?;
            Ok((
                ToolContinuationClaimOutcome::Applied {
                    receipt,
                    grant: Box::new(ToolContinuationGrant {
                        fence: RunFence {
                            run_id: run.id.clone(),
                            claim_token: activated.claim_token,
                        },
                        authority,
                        phases,
                        calls,
                        source_turn_id: record.batch.source_turn_id,
                        usage: record.batch.usage,
                        turns_used: record.batch.turns_used,
                    }),
                },
                true,
            ))
        }
        .await;
        after(finish(tx, result).await, fault)
    }
}
async fn ready(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    claim: &ToolContinuationClaim,
) -> Result<ToolContinuationRecord, Error> {
    let record = super::waiting_context(tx, run)
        .await?
        .ok_or(Error::InvalidTransition)?;
    if record.batch.continuation != claim.continuation {
        return Err(Error::GenerationConflict);
    }
    if record.batch.expected_plan != claim.expected_plan {
        return Err(Error::SemanticConflict);
    }
    if record.questions.iter().any(|q| {
        q.answer.is_none()
            || q.view.state == crabber_core::InteractionState::Pending
            || q.view.consumed
    }) {
        return Err(Error::Pending);
    }
    Ok(record)
}
pub(in super::super) async fn waiting_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let Some(record) = write::active(tx, run).await? else {
        return Ok(None);
    };
    if record.state != ToolContinuationState::Waiting || run.status != RunStatus::Paused {
        return Err(Error::InvalidTransition);
    }
    if record.batch.expected_plan.as_str() != run.plan_fingerprint
        || run.checkpoint.as_ref() != Some(&domain::checkpoint(&record.batch))
        || run.usage != record.batch.usage
    {
        return Err(Error::SemanticConflict);
    }
    let (previous, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if previous.snapshot.target != record.batch.attempt
        || previous.snapshot.state != AttemptState::Suspended
        || previous.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    evidence::ready(tx, run, &record).await?;
    Ok(Some(record))
}
