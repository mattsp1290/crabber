use super::{
    Error, SqliteStore, ToolContinuationClaimOutcome, ToolContinuationOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, evidence, fault, finish,
    read, revision, unknown, write,
};
use crate::native_enrollment::caller::{
    Caller,
    tool::{IssuedPhases, ToolCompletion, ToolGrantOutput, ToolInput},
};
use crate::public_session::v2::proof::Proof;
use crate::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, ToolContinuationRecovery,
};
use crabber_core::{Run, RunStatus};
impl SqliteStore {
    pub(in super::super) async fn recover_tools(
        &self,
        request: ToolContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(ToolInput::Recovery(&request), &mut Proof::bounded(32 << 20))
            .await
    }
    pub(in super::super) async fn recover_frontend_tools(
        &self,
        request: crate::FrontendContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(
            ToolInput::FrontendRecovery(&request),
            &mut Proof::bounded(32 << 20),
        )
        .await
    }

    pub(in crate::sqlite) async fn recover_compound_with_proof(
        &self,
        input: ToolInput<'_>,
        proof: &mut Proof,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        if let Some(saved) = super::native_caller::outer_replay(self, input, proof).await? {
            return Ok(ToolContinuationClaimOutcome::Replayed(saved));
        }
        if input.frontend().is_some() && crate::SQLITE_SCHEMA_VERSION < 10 {
            return Err(Error::Unsupported);
        }
        let fault = fault(self);
        let original = input.original();
        let (mut tx, run) = self
            .tool_transaction_with_proof(&original.session_id, &original.run_id, proof)
            .await?;
        let result = self
            .recover_compound_guarded(&mut tx, &run, input, None, Caller::Raw, proof)
            .await;
        after(finish(tx, result).await, fault).map(super::native_caller::ToolMutation::into_outcome)
    }
    pub(in crate::sqlite) async fn recover_tools_guarded(
        &self,
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &ToolContinuationRecovery,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.recover_compound_guarded(
            tx,
            run,
            ToolInput::Recovery(request),
            preparation,
            caller,
            read_proof,
        )
        .await
    }
    pub(in crate::sqlite) async fn recover_frontend_guarded(
        &self,
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &crate::FrontendContinuationRecovery,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.recover_compound_guarded(
            tx,
            run,
            ToolInput::FrontendRecovery(request),
            Some(preparation),
            caller,
            read_proof,
        )
        .await
    }
    async fn recover_compound_guarded(
        &self,
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        input: ToolInput<'_>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        let digest = super::native_caller::digest(tx, input, read_proof).await?;
        let fault = fault(self);
        if let Some(saved) = super::native_caller::replay(tx, input, &digest, read_proof).await? {
            return Ok((super::native_caller::ToolMutation::Replayed(saved), false));
        }
        let (common, frontend) = input.recovering(read_proof)?;
        let request = common.as_ref();
        let (expected_revision, completion) = input.completion(preparation, &digest, read_proof)?;
        if revision(tx, &run.session_id).await? != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = eligible_with_proof(tx, run, request, frontend, read_proof).await?;
        read_proof.measure(run).map_err(unknown)?;
        let now = self.clock.now();
        let activated =
            domain::canonical::activated_run(run, &request.owner, request.lease_ms, now)?;
        let (mut parent, _) =
            super::super::attempt::load_host_attempt_with_proof(tx, &run.id, None, read_proof)
                .await
                .map_err(unknown)?
                .ok_or(Error::NotFound)?;
        if parent.lease_until > now {
            return Err(Error::LiveOwner);
        }
        crate::native_enrollment::caller::measure_record(&parent, read_proof).map_err(unknown)?;
        let previous = parent.clone();
        read_proof.measure(&request.attempt).map_err(unknown)?;
        let native_request = AttemptMutation {
            id: domain::derived_id("tool-continuation-reclaim-attempt", &request.id),
            expected_revision,
            command: AttemptCommand::ReclaimUnstarted {
                target: request.attempt.clone(),
            },
        };
        parent
            .change(&activated, &native_request, None, now, true)
            .map_err(|_| Error::CleanupBlocked)?;
        crate::native_enrollment::caller::measure_record(&parent, read_proof).map_err(unknown)?;
        let authority = parent.authority().map_err(unknown)?;

        let publication =
            super::activation::save(tx, &activated, completion.as_ref(), preparation, read_proof)
                .await
                .map_err(unknown)?;
        super::super::attempt::save_host_attempt(tx, &parent, false)
            .await
            .map_err(unknown)?;
        let phases = super::phases::recover(tx, run, &record, &authority, read_proof).await?;
        let frontend_phases = if frontend.is_some() {
            super::phases::recover_frontend(tx, run, &record, &authority, read_proof).await?
        } else {
            IssuedPhases::empty()
        };
        input.measure(read_proof).map_err(unknown)?;
        record.capture_recovery(request, frontend)?;
        read_proof.measure(&record).map_err(unknown)?;
        write::save(tx, &record, false).await?;
        let native_phases = crate::native_enrollment::caller::tool::records(
            phases.records,
            frontend_phases.records,
            read_proof,
        )
        .map_err(unknown)?;
        let outcome = super::native_caller::finish(
            tx,
            caller,
            ToolCompletion {
                input,
                previous: run,
                current: &activated,
                record: &record,
                previous_parent: &previous,
                parent: &parent,
                native_request: &native_request,
                phases: &native_phases,
                publication: &publication,
                completion: completion.as_ref(),
            },
            ToolGrantOutput::new(
                &activated,
                authority,
                phases.grants,
                frontend_phases.grants,
                digest,
                read_proof,
            )?,
            preparation,
            read_proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )
        .await?;
        before(fault)?;
        Ok((outcome, true))
    }
}
async fn eligible_with_proof(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &ToolContinuationRecovery,
    frontend: Option<&crate::FrontendContinuationRecovery>,
    proof: &mut Proof,
) -> Result<ToolContinuationRecord, Error> {
    let (original, _) = read::receipt_with_proof(tx, &run.session_id, &request.original.id, proof)
        .await
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
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
    let record = super::native_caller::active(tx, run, proof)
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
    let (parent, _) = super::super::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if parent.snapshot.target != request.attempt
        || parent.snapshot.state != AttemptState::Unstarted
        || parent.snapshot.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::CleanupBlocked);
    }
    evidence::recover_with_proof(tx, run, &record, proof).await?;
    Ok(record)
}

pub(in crate::sqlite) async fn preparation_context(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &ToolContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    eligible_with_proof(tx, run, request, None, proof).await?;
    let (parent, _) = super::super::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if parent.lease_until > now {
        return Err(Error::LiveOwner);
    }
    Ok(())
}

pub(in crate::sqlite) async fn preparation_frontend_context(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &crate::FrontendContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    proof
        .measure(&(&request.original.claim, &request.attempt, &request.owner))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    eligible_with_proof(
        tx,
        run,
        &domain::canonical::frontend_recovery(request),
        Some(request),
        proof,
    )
    .await?;
    let (parent, _) = super::super::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if parent.lease_until > now {
        return Err(Error::LiveOwner);
    }
    Ok(())
}
