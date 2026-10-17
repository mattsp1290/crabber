use super::{
    Error, SqliteStore, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, evidence, fault, finish,
    revision, unknown, write,
};
use crate::{AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState};
use crabber_core::{Run, RunFence, RunStatus};
impl SqliteStore {
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
        if crate::SQLITE_SCHEMA_VERSION < 10 {
            return Err(Error::Unsupported);
        }
        self.claim_compound(&request.claim, Some(&request)).await
    }
    async fn claim_compound(
        &self,
        claim: &ToolContinuationClaim,
        frontend: Option<&crate::FrontendContinuationClaim>,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let fault = fault(self);
        let (mut tx, run) = if frontend.is_some() {
            self.frontend_transaction(&claim.session_id, &claim.run_id)
                .await?
        } else {
            self.tool_transaction(&claim.session_id, &claim.run_id)
                .await?
        };
        let result = self
            .claim_compound_guarded(
                &mut tx,
                &run,
                claim,
                frontend,
                None,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
            )
            .await;
        after(finish(tx, result).await, fault)
    }
    pub(in crate::sqlite) async fn claim_tools_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        claim: &ToolContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        self.claim_compound_guarded(tx, run, claim, None, preparation, read_proof)
            .await
    }
    pub(in crate::sqlite) async fn claim_frontend_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &crate::FrontendContinuationClaim,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        self.claim_compound_guarded(
            tx,
            run,
            &request.claim,
            Some(request),
            Some(preparation),
            read_proof,
        )
        .await
    }
    async fn claim_compound_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        claim: &ToolContinuationClaim,
        frontend: Option<&crate::FrontendContinuationClaim>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        let digest = match frontend {
            Some(request) => request.fingerprint()?,
            None => claim.fingerprint()?,
        };
        let fault = fault(self);
        if let Some(saved) = write::replay(tx, run, &claim.id, &digest).await? {
            return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
        }
        let (expected_revision, completion) = crate::native_parent::CompletionSource::control(
            preparation,
            if frontend.is_some() {
                crate::native_parent::CompletionOperation::FrontendClaim
            } else {
                crate::native_parent::CompletionOperation::ToolClaim
            },
            &crate::session_owner::preparation::PreparationKey::ToolContinuation(claim.id.clone()),
            (&claim.session_id, &claim.run_id),
            claim.expected_revision,
            &digest,
        )
        .map_err(|_| Error::SemanticConflict)?;
        if revision(tx, &run.session_id).await? != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = ready(tx, run, claim, frontend).await?;
        let now = self.clock.now();
        let activated = domain::canonical::activated_run(run, &claim.owner, claim.lease_ms, now)?;
        let (previous, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        let reserve = AttemptMutation {
            id: domain::derived_id("tool-continuation-claim-attempt", &claim.id),
            expected_revision,
            command: AttemptCommand::Reserve {
                target: record.batch.next_attempt.clone(),
            },
        };
        let parent =
            crate::attempt::AttemptRecord::reserve(&activated, &reserve, Some(&previous.snapshot))
                .map_err(|_| Error::CleanupBlocked)?;
        let authority = parent.authority().map_err(unknown)?;

        super::activation::save(tx, &activated, completion.as_ref(), preparation, read_proof)
            .await
            .map_err(unknown)?;
        super::super::attempt::save_host_attempt(tx, &parent, true)
            .await
            .map_err(unknown)?;
        let phases =
            super::phases::claim(tx, run, &activated, &parent, &record, &authority).await?;
        let frontend_phases = match frontend {
            Some(request) => {
                super::phases::claim_frontend(tx, &activated, &parent, &record, request, &authority)
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
        write::save(tx, &record, false).await?;
        let receipt = write::publish(
            tx,
            run,
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
}
async fn ready(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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

pub(in crate::sqlite) async fn preparation_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    claim: &ToolContinuationClaim,
) -> Result<ToolContinuationRecord, Error> {
    ready(tx, run, claim, None).await
}

pub(in crate::sqlite) async fn preparation_frontend_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &crate::FrontendContinuationClaim,
) -> Result<ToolContinuationRecord, Error> {
    ready(tx, run, &request.claim, Some(request)).await
}
