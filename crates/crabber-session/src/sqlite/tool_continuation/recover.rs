use super::{
    Error, SqliteStore, ToolContinuationClaimOutcome, ToolContinuationOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, evidence, fault, finish,
    read, revision, unknown, write,
};
use crate::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, ToolContinuationGrant,
    ToolContinuationRecovery,
};
use crabber_core::{Run, RunFence, RunStatus};
impl SqliteStore {
    pub(in super::super) async fn recover_tools(
        &self,
        request: ToolContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let digest = request.fingerprint()?;
        let fault = fault(self);
        let original = &request.original;
        let (mut tx, run) = self
            .tool_transaction(&original.session_id, &original.run_id)
            .await?;
        let result = async {
            if let Some(saved) = write::replay(&mut tx, &run, &request.id, &digest).await? {
                request.verify_receipt(&saved)?;
                return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
            }
            if revision(&mut tx, &run.session_id).await? != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let mut record = eligible(&mut tx, &run, &request).await?;
            let now = self.clock.now();
            let activated =
                domain::canonical::activated_run(&run, &request.owner, request.lease_ms, now)?;
            let (mut parent, _) =
                super::super::attempt::load_host_attempt(&mut tx, &run.id, None, true)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::NotFound)?;
            if parent.lease_until > now {
                return Err(Error::LiveOwner);
            }
            parent
                .change(
                    &activated,
                    &AttemptMutation {
                        id: domain::derived_id("tool-continuation-reclaim-attempt", &request.id),
                        expected_revision: request.expected_revision,
                        command: AttemptCommand::ReclaimUnstarted {
                            target: request.attempt.clone(),
                        },
                    },
                    None,
                    now,
                    true,
                )
                .map_err(|_| Error::CleanupBlocked)?;
            let authority = parent.authority().map_err(unknown)?;
            super::super::records::save_run(&mut tx, &activated)
                .await
                .map_err(unknown)?;
            super::super::attempt::save_host_attempt(&mut tx, &parent, false)
                .await
                .map_err(unknown)?;
            let phases = super::phases::recover(&mut tx, &run, &record, &authority).await?;
            let calls = record.grants()?;
            record.recovery = Some(request.clone());
            write::save(&mut tx, &record, false).await?;
            let receipt = write::publish(
                &mut tx,
                &run,
                request.id.clone(),
                digest,
                ToolContinuationOutcome::ReclaimedUnstarted {
                    continuation: original.continuation.clone(),
                    original_claim_id: original.id.clone(),
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
async fn eligible(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &ToolContinuationRecovery,
) -> Result<ToolContinuationRecord, Error> {
    let (original, _) = read::receipt(tx, &run.session_id, &request.original.id)
        .await?
        .ok_or(Error::NotFound)?;
    if original.run_id != run.id
        || original.semantic_digest != request.original.fingerprint()?
        || !matches!(original.outcome,ToolContinuationOutcome::Claimed{ref continuation,ref attempt} if continuation==&request.original.continuation && attempt.target==request.attempt)
    {
        return Err(Error::SemanticConflict);
    }
    let record = write::active(tx, run)
        .await?
        .ok_or(Error::InvalidTransition)?;
    if record.state != ToolContinuationState::Claimed
        || record.batch.continuation != request.original.continuation
        || record.batch.next_attempt != request.attempt
        || record.claim.as_ref().is_none_or(|claim| {
            claim.id != request.original.id
                || claim.fingerprint().ok() != request.original.fingerprint().ok()
        })
    {
        return Err(Error::SemanticConflict);
    }
    if run.status != RunStatus::Running
        || run.checkpoint.as_ref() != Some(&domain::checkpoint(&record.batch))
        || run.usage != record.batch.usage
        || record.questions.iter().any(|q| !q.view.consumed)
    {
        return Err(Error::InvalidTransition);
    }
    let (parent, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if parent.snapshot.target != request.attempt
        || parent.snapshot.state != AttemptState::Unstarted
        || parent.snapshot.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::CleanupBlocked);
    }
    evidence::recover(tx, run, &record).await?;
    Ok(record)
}
