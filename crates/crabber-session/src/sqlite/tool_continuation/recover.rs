use super::{
    Error, SqliteStore, ToolContinuationClaimOutcome, ToolContinuationOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, evidence, fault, finish,
    read, revision, unknown, write,
};
use crate::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, ToolContinuationRecovery,
};
use crabber_core::{Run, RunFence, RunStatus};
impl SqliteStore {
    pub(in super::super) async fn recover_tools(
        &self,
        request: ToolContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.recover_compound(&request, None).await
    }
    pub(in super::super) async fn recover_frontend_tools(
        &self,
        request: crate::FrontendContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        if let Some(saved) = self
            .tool_receipt(&request.original.claim.session_id, &request.id)
            .await?
        {
            request.verify_receipt(&saved)?;
            return Ok(ToolContinuationClaimOutcome::Replayed(saved));
        }
        if crate::SQLITE_SCHEMA_VERSION < 10 {
            return Err(Error::Unsupported);
        }
        let common = domain::canonical::frontend_recovery(&request);
        self.recover_compound(&common, Some(&request)).await
    }
    async fn recover_compound(
        &self,
        request: &ToolContinuationRecovery,
        frontend: Option<&crate::FrontendContinuationRecovery>,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let fault = fault(self);
        let original = &request.original;
        let (mut tx, run) = if frontend.is_some() {
            self.frontend_transaction(&original.session_id, &original.run_id)
                .await?
        } else {
            self.tool_transaction(&original.session_id, &original.run_id)
                .await?
        };
        let result = self
            .recover_compound_guarded(
                &mut tx,
                &run,
                request,
                frontend,
                None,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
            )
            .await;
        after(finish(tx, result).await, fault)
    }
    pub(in crate::sqlite) async fn recover_tools_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &ToolContinuationRecovery,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        self.recover_compound_guarded(tx, run, request, None, preparation, read_proof)
            .await
    }
    pub(in crate::sqlite) async fn recover_frontend_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &crate::FrontendContinuationRecovery,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        self.recover_compound_guarded(
            tx,
            run,
            &domain::canonical::frontend_recovery(request),
            Some(request),
            Some(preparation),
            read_proof,
        )
        .await
    }
    async fn recover_compound_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &ToolContinuationRecovery,
        frontend: Option<&crate::FrontendContinuationRecovery>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        let digest = match frontend {
            Some(request) => request.fingerprint()?,
            None => request.fingerprint()?,
        };
        let fault = fault(self);
        let original = &request.original;
        if let Some(saved) = write::replay(tx, run, &request.id, &digest).await? {
            match frontend {
                Some(request) => request.verify_receipt(&saved)?,
                None => request.verify_receipt(&saved)?,
            }
            return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
        }
        let (expected_revision, completion) = crate::native_parent::CompletionSource::control(
            preparation,
            if frontend.is_some() {
                crate::native_parent::CompletionOperation::FrontendRecovery
            } else {
                crate::native_parent::CompletionOperation::ToolRecovery
            },
            &crate::session_owner::preparation::PreparationKey::ToolContinuation(
                request.id.clone(),
            ),
            (&original.session_id, &original.run_id),
            request.expected_revision,
            &digest,
        )
        .map_err(|_| Error::SemanticConflict)?;
        if revision(tx, &run.session_id).await? != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = eligible(tx, run, request, frontend).await?;
        let now = self.clock.now();
        let activated =
            domain::canonical::activated_run(run, &request.owner, request.lease_ms, now)?;
        let (mut parent, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
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
                    expected_revision,
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

        super::super::records::save_run_with_proof(tx, &activated, completion.as_ref(), read_proof)
            .await
            .map_err(unknown)?;
        super::super::attempt::save_host_attempt(tx, &parent, false)
            .await
            .map_err(unknown)?;
        let phases = super::phases::recover(tx, run, &record, &authority).await?;
        let frontend_phases = if frontend.is_some() {
            super::phases::recover_frontend(tx, run, &record, &authority).await?
        } else {
            Vec::new()
        };
        record.capture_recovery(request, frontend)?;
        write::save(tx, &record, false).await?;
        let receipt = write::publish(
            tx,
            run,
            request.id.clone(),
            digest,
            record.recovered_outcome(parent.snapshot)?,
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
async fn eligible(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &ToolContinuationRecovery,
    frontend: Option<&crate::FrontendContinuationRecovery>,
) -> Result<ToolContinuationRecord, Error> {
    let (original, _) = read::receipt(tx, &run.session_id, &request.original.id)
        .await?
        .ok_or(Error::NotFound)?;
    let expected_digest = match frontend {
        Some(request) => request.original.fingerprint()?,
        None => request.original.fingerprint()?,
    };
    let original_kind = match (&original.outcome, frontend) {
        (
            ToolContinuationOutcome::Claimed {
                continuation,
                attempt,
            },
            None,
        )
        | (
            ToolContinuationOutcome::FrontendClaimed {
                continuation,
                attempt,
            },
            Some(_),
        ) => continuation == &request.original.continuation && attempt.target == request.attempt,
        _ => false,
    };
    if original.run_id != run.id || original.semantic_digest != expected_digest || !original_kind {
        return Err(Error::SemanticConflict);
    }
    let record = write::active(tx, run)
        .await?
        .ok_or(Error::InvalidTransition)?;
    match frontend {
        Some(request) => {
            request.original.validate_for_batch(&record.batch)?;
            if record.frontend_claim.as_ref().is_none_or(|claim| {
                claim.claim.id != request.original.claim.id
                    || claim.fingerprint().ok() != request.original.fingerprint().ok()
            }) {
                return Err(Error::SemanticConflict);
            }
        }
        None if !record.batch.frontend.is_empty() => return Err(Error::InvalidTransition),
        None => (),
    }
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

pub(in crate::sqlite) async fn preparation_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &ToolContinuationRecovery,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    eligible(tx, run, request, None).await?;
    let (parent, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if parent.lease_until > now {
        return Err(Error::LiveOwner);
    }
    Ok(())
}

pub(in crate::sqlite) async fn preparation_frontend_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &crate::FrontendContinuationRecovery,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    eligible(
        tx,
        run,
        &domain::canonical::frontend_recovery(request),
        Some(request),
    )
    .await?;
    let (parent, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if parent.lease_until > now {
        return Err(Error::LiveOwner);
    }
    Ok(())
}
