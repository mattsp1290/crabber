use super::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, Error, MemoryStore,
    PhaseQuiescence, PhaseState, Run, State, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, fault, publish, replay,
    revision,
};
use crabber_core::{RunFence, RunStatus, ToolCallStatus};
impl MemoryStore {
    pub(in super::super) fn claim_tools(
        &self,
        claim: &ToolContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.claim_compound(claim, None)
    }
    pub(in super::super) fn claim_frontend(
        &self,
        request: &crate::FrontendContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.claim_compound(&request.claim, Some(request))
    }
    fn claim_compound(
        &self,
        claim: &ToolContinuationClaim,
        frontend: Option<&crate::FrontendContinuationClaim>,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let fault = fault(self);
        let result = self.transact(|state| {
            self.claim_compound_guarded(
                state,
                claim,
                frontend,
                None,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
            )
        });
        after(result, fault)
    }
    pub(in crate::memory) fn claim_tools_guarded(
        &self,
        state: &mut super::super::State,
        claim: &ToolContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        self.claim_compound_guarded(state, claim, None, preparation, read_proof)
    }
    pub(in crate::memory) fn claim_frontend_guarded(
        &self,
        state: &mut State,
        request: &crate::FrontendContinuationClaim,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(ToolContinuationClaimOutcome, bool), Error> {
        self.claim_compound_guarded(
            state,
            &request.claim,
            Some(request),
            Some(preparation),
            read_proof,
        )
    }
    fn claim_compound_guarded(
        &self,
        state: &mut super::super::State,
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
        if let Some(saved) = replay(state, &claim.session_id, &claim.run_id, &claim.id, &digest)? {
            return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
        }
        let run = state
            .runs
            .get(&claim.run_id)
            .filter(|r| r.session_id == claim.session_id)
            .cloned()
            .ok_or(Error::NotFound)?;
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
        if revision(state, &run.session_id) != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = ready(state, &run, claim, frontend)?;
        let now = self.clock.now();
        let activated = domain::canonical::activated_run(&run, &claim.owner, claim.lease_ms, now)?;
        let previous = state
            .attempts
            .get(&record.batch.attempt.id)
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
        let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
        let phases = super::phases::claim(state, &activated, &parent, &record)?;
        let frontend_phases = match frontend {
            Some(request) => {
                super::phases::claim_frontend(state, &activated, &parent, &record, request)?
            }
            None => Vec::new(),
        };
        let fence = RunFence {
            run_id: run.id.clone(),
            claim_token: activated.claim_token.clone(),
        };

        super::activation::save(
            state,
            &run,
            &activated,
            completion.as_ref(),
            preparation,
            read_proof,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
        state.runs.insert(run.id.clone(), activated);
        state
            .attempts
            .insert(parent.snapshot.target.id.clone(), parent.clone());
        record.capture_claim(claim, frontend);
        record.validate()?;
        state
            .tool_continuations
            .insert(record.batch.continuation.id.clone(), record.clone());
        let receipt = publish(
            state,
            &run,
            claim.id.clone(),
            digest,
            record.claimed_outcome(parent.snapshot),
            now,
        )?;
        before(fault)?;
        let outcome = record.into_outcome(
            fence,
            authority,
            phases,
            frontend_phases,
            crate::tool_continuation::ack::Completion::new(
                receipt,
                (&parent.token, &parent.run_token),
                read_proof,
            ),
        )?;
        Ok((outcome, true))
    }
}
fn ready(
    state: &State,
    run: &Run,
    claim: &ToolContinuationClaim,
    frontend: Option<&crate::FrontendContinuationClaim>,
) -> Result<ToolContinuationRecord, Error> {
    let record = super::waiting_context(state, run)?.ok_or(Error::InvalidTransition)?;
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
pub(in super::super) fn waiting_context(
    state: &State,
    run: &Run,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let Some(record) = super::active(state, run)? else {
        return Ok(None);
    };
    if record.run_id != run.id {
        return Err(Error::GenerationConflict);
    }
    if run.status != RunStatus::Paused || record.state != ToolContinuationState::Waiting {
        return Err(Error::InvalidTransition);
    }
    if record.batch.expected_plan.as_str() != run.plan_fingerprint
        || run.checkpoint.as_ref() != Some(&domain::checkpoint(&record.batch))
        || run.usage != record.batch.usage
    {
        return Err(Error::SemanticConflict);
    }
    let previous = state
        .attempts
        .values()
        .filter(|a| a.snapshot.run_id == run.id)
        .max_by_key(|a| a.snapshot.target.generation)
        .ok_or(Error::NotFound)?;
    previous
        .validate()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if previous.snapshot.target != record.batch.attempt
        || previous.snapshot.state != AttemptState::Suspended
        || previous.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    let unfinished = state
        .calls
        .values()
        .filter(|c| {
            c.run_id == run.id
                && matches!(
                    c.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
                )
        })
        .take(65)
        .collect::<Vec<_>>();
    if unfinished.len() != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    for prepared in &record.batch.calls {
        let call = unfinished
            .iter()
            .find(|c| c.id == prepared.target.call_id)
            .ok_or(Error::NotFound)?;
        prepared
            .verify_call(call)
            .map_err(|_| Error::SemanticConflict)?;
        let frozen = record
            .batch
            .phases
            .iter()
            .find(|p| p.previous.call_id == call.id);
        let latest = state
            .phases
            .values()
            .filter(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == call.id)
            .max_by_key(|p| p.snapshot.target.generation);
        let frontend = record
            .batch
            .frontend
            .iter()
            .find(|f| f.target.call_id == call.id);
        let origin = super::super::frontend::origin(state, run, &call.id)?;
        if let Some(frontend) = frontend {
            super::super::frontend::verify_frozen(state, run, frontend)?;
            if latest.is_some()
                || frozen.is_some()
                || call.status != ToolCallStatus::Waiting
                || call.result.is_some()
            {
                return Err(Error::CleanupBlocked);
            }
            continue;
        }
        if origin.is_some() {
            return Err(Error::SemanticConflict);
        }
        match (frozen, latest) {
            (Some(frozen), Some(phase))
                if call.status == ToolCallStatus::Waiting
                    && phase.snapshot.target == frozen.previous
                    && phase.snapshot.state == PhaseState::Suspended
                    && phase.snapshot.quiescence == PhaseQuiescence::Complete => {}
            (None, None) if call.status == ToolCallStatus::Pending => (),
            _ => return Err(Error::CleanupBlocked),
        }
        if call.result.is_some() {
            return Err(Error::SemanticConflict);
        }
    }
    super::evidence::validate(state, run, None)?;
    Ok(Some(record))
}

pub(in crate::memory) fn preparation_context(
    state: &State,
    run: &Run,
    claim: &ToolContinuationClaim,
) -> Result<ToolContinuationRecord, Error> {
    ready(state, run, claim, None)
}

pub(in crate::memory) fn preparation_frontend_context(
    state: &State,
    run: &Run,
    request: &crate::FrontendContinuationClaim,
) -> Result<ToolContinuationRecord, Error> {
    ready(state, run, &request.claim, Some(request))
}
