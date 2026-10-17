use super::{
    Error, SqliteStore, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, evidence, fault, finish,
    revision, unknown, write,
};
use crate::native_enrollment::caller::{
    Caller,
    tool::{IssuedPhases, ToolCompletion, ToolGrantOutput, ToolInput},
};
use crate::public_session::v2::proof::Proof;
use crate::{AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState};
use crabber_core::{Run, RunStatus};
impl SqliteStore {
    pub(in super::super) async fn claim_tools(
        &self,
        claim: ToolContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(ToolInput::Claim(&claim), &mut Proof::bounded(32 << 20))
            .await
    }
    pub(in super::super) async fn claim_frontend_tools(
        &self,
        request: crate::FrontendContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(
            ToolInput::FrontendClaim(&request),
            &mut Proof::bounded(32 << 20),
        )
        .await
    }

    pub(in crate::sqlite) async fn claim_compound_with_proof(
        &self,
        claim: &ToolContinuationClaim,
        frontend: Option<&crate::FrontendContinuationClaim>,
        proof: &mut Proof,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let input = frontend.map_or(ToolInput::Claim(claim), ToolInput::FrontendClaim);
        if let Some(saved) = super::native_caller::outer_replay(self, input, proof).await? {
            return Ok(ToolContinuationClaimOutcome::Replayed(saved));
        }
        if frontend.is_some() && crate::SQLITE_SCHEMA_VERSION < 10 {
            return Err(Error::Unsupported);
        }
        let fault = fault(self);
        let (mut tx, run) = self
            .tool_transaction_with_proof(&claim.session_id, &claim.run_id, proof)
            .await?;
        let result = self
            .claim_compound_guarded(&mut tx, &run, input, None, Caller::Raw, proof)
            .await;
        after(finish(tx, result).await, fault).map(super::native_caller::ToolMutation::into_outcome)
    }
    pub(in crate::sqlite) async fn claim_tools_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        claim: &ToolContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.claim_compound_guarded(
            tx,
            run,
            ToolInput::Claim(claim),
            preparation,
            caller,
            read_proof,
        )
        .await
    }
    pub(in crate::sqlite) async fn claim_frontend_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        request: &crate::FrontendContinuationClaim,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.claim_compound_guarded(
            tx,
            run,
            ToolInput::FrontendClaim(request),
            Some(preparation),
            caller,
            read_proof,
        )
        .await
    }
    async fn claim_compound_guarded(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &Run,
        input: ToolInput<'_>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        let (claim, frontend) = input.claiming()?;
        let digest = super::native_caller::digest(tx, input, read_proof).await?;
        let fault = fault(self);
        if let Some(saved) = super::native_caller::replay(tx, input, &digest, read_proof).await? {
            return Ok((super::native_caller::ToolMutation::Replayed(saved), false));
        }
        let (expected_revision, completion) = input.completion(preparation, &digest, read_proof)?;
        if revision(tx, &run.session_id).await? != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = ready_with_proof(tx, run, claim, frontend, read_proof).await?;
        read_proof.measure(run).map_err(unknown)?;
        let now = self.clock.now();
        let activated = domain::canonical::activated_run(run, &claim.owner, claim.lease_ms, now)?;
        let (previous, _) =
            super::super::attempt::load_host_attempt_with_proof(tx, &run.id, None, read_proof)
                .await
                .map_err(unknown)?
                .ok_or(Error::NotFound)?;
        read_proof
            .measure(&record.batch.next_attempt)
            .map_err(unknown)?;
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
        crate::native_enrollment::caller::measure_record(&parent, read_proof).map_err(unknown)?;
        let authority = parent.authority().map_err(unknown)?;

        let publication =
            super::activation::save(tx, &activated, completion.as_ref(), preparation, read_proof)
                .await
                .map_err(unknown)?;
        super::super::attempt::save_host_attempt(tx, &parent, true)
            .await
            .map_err(unknown)?;
        let phases = super::phases::claim(
            tx, run, &activated, &parent, &record, &authority, read_proof,
        )
        .await?;
        let frontend_phases = match frontend {
            Some(request) => {
                super::phases::claim_frontend(
                    tx, &activated, &parent, &record, request, &authority, read_proof,
                )
                .await?
            }
            None => IssuedPhases::empty(),
        };
        input.measure(read_proof).map_err(unknown)?;
        record.capture_claim(claim, frontend);
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
                native_request: &reserve,
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
async fn ready_with_proof(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    claim: &ToolContinuationClaim,
    frontend: Option<&crate::FrontendContinuationClaim>,
    proof: &mut Proof,
) -> Result<ToolContinuationRecord, Error> {
    let record = waiting_context_with_proof(tx, run, proof)
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
    waiting_context_with_proof(tx, run, &mut Proof::legacy()).await
}
pub(super) async fn waiting_context_with_proof(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    proof: &mut Proof,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let Some(record) = super::native_caller::active(tx, run, proof).await? else {
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
    let (previous, _) =
        super::super::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
    if previous.snapshot.target != record.batch.attempt
        || previous.snapshot.state != AttemptState::Suspended
        || previous.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    evidence::ready_with_proof(tx, run, &record, proof).await?;
    Ok(Some(record))
}

pub(in crate::sqlite) async fn preparation_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    claim: &ToolContinuationClaim,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ToolContinuationRecord, Error> {
    ready_with_proof(tx, run, claim, None, proof).await
}

pub(in crate::sqlite) async fn preparation_frontend_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &Run,
    request: &crate::FrontendContinuationClaim,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ToolContinuationRecord, Error> {
    ready_with_proof(tx, run, &request.claim, Some(request), proof).await
}
