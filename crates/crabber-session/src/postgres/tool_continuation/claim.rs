use super::{
    Error, PostgresStore, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, evidence, fault, finish,
    revision, unknown, write,
};
use crate::{AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState};
use crabber_core::{Run, RunFence, RunStatus};
impl PostgresStore {
    pub(in super::super) async fn claim_tools(
        &self,
        claim: ToolContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.claim_compound(&claim, None).await
    }
    pub(in super::super) async fn claim_frontend_tools(
        &self,
        request: crate::FrontendContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let digest = request.fingerprint()?;
        if let Some(saved) = self
            .tool_receipt(&request.claim.session_id, &request.claim.id)
            .await?
        {
            if saved.run_id != request.claim.run_id || saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(ToolContinuationClaimOutcome::Replayed(saved));
        }
        if crate::POSTGRES_SCHEMA_VERSION < 14 {
            return Err(Error::Unsupported);
        }
        self.claim_compound(&request.claim, Some(&request)).await
    }
    async fn claim_compound(
        &self,
        claim: &ToolContinuationClaim,
        frontend: Option<&crate::FrontendContinuationClaim>,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let digest = match frontend {
            Some(request) => request.fingerprint()?,
            None => claim.fingerprint()?,
        };
        let fault = fault(self);
        let (mut tx, run) = if frontend.is_some() {
            self.frontend_transaction(&claim.session_id, &claim.run_id)
                .await?
        } else {
            self.tool_transaction(&claim.session_id, &claim.run_id)
                .await?
        };
        let result = async {
            if let Some(saved) = write::replay(&mut tx, &run, &claim.id, &digest).await? {
                return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
            }
            if revision(&mut tx, &run.session_id).await? != claim.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let mut record = ready(&mut tx, &run, claim, frontend).await?;
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
            super::super::save_run(&mut tx, &activated)
                .await
                .map_err(unknown)?;
            super::super::attempt::save_host_attempt(&mut tx, &parent, true)
                .await
                .map_err(unknown)?;
            let phases =
                super::phases::claim(&mut tx, &run, &activated, &parent, &record, &authority)
                    .await?;
            let frontend_phases = match frontend {
                Some(request) => {
                    super::phases::claim_frontend(
                        &mut tx, &activated, &parent, &record, request, &authority,
                    )
                    .await?
                }
                None => Vec::new(),
            };
            record.state = ToolContinuationState::Claimed;
            record.claim = Some(claim.clone());
            record.frontend_claim = frontend.cloned().map(Box::new);
            for question in &mut record.questions {
                question.view.consumed = true;
            }
            write::save(&mut tx, &record, false).await?;
            let receipt = write::publish(
                &mut tx,
                &run,
                claim.id.clone(),
                digest,
                record.claimed_outcome(parent.snapshot),
                now,
            )
            .await?;
            before(fault)?;
            Ok((
                ToolContinuationClaimOutcome::Applied {
                    receipt,
                    grant: record.into_grant(
                        RunFence {
                            run_id: run.id.clone(),
                            claim_token: activated.claim_token,
                        },
                        authority,
                        phases,
                        frontend_phases,
                    )?,
                },
                true,
            ))
        }
        .await;
        after(finish(tx, result).await, fault)
    }
}
async fn ready(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: &Run,
    claim: &ToolContinuationClaim,
    frontend: Option<&crate::FrontendContinuationClaim>,
) -> Result<ToolContinuationRecord, Error> {
    let record = super::waiting_context(tx, run)
        .await?
        .ok_or(Error::InvalidTransition)?;
    match frontend {
        Some(request) => request.validate_for_batch(&record.batch)?,
        None if !record.batch.frontend.is_empty() => return Err(Error::InvalidTransition),
        None => (),
    }
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
