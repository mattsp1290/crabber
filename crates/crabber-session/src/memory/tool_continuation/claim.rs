use super::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, Error, MemoryStore,
    PhaseQuiescence, PhaseState, Run, State, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, fault, revision,
};
use crate::native_enrollment::caller::{
    Caller,
    tool::{IssuedPhases, ToolCompletion, ToolGrantOutput, ToolInput},
};
use crate::public_session::v2::proof::Proof;
use crabber_core::{RunStatus, ToolCallStatus};
impl MemoryStore {
    pub(in super::super) fn claim_tools(
        &self,
        claim: &ToolContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(ToolInput::Claim(claim), &mut Proof::bounded(32 << 20))
    }
    pub(in super::super) fn claim_frontend(
        &self,
        request: &crate::FrontendContinuationClaim,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(
            ToolInput::FrontendClaim(request),
            &mut Proof::bounded(32 << 20),
        )
    }

    pub(in crate::memory) fn claim_compound_with_proof(
        &self,
        claim: &ToolContinuationClaim,
        frontend: Option<&crate::FrontendContinuationClaim>,
        proof: &mut Proof,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let fault = fault(self);
        let input = frontend.map_or(ToolInput::Claim(claim), ToolInput::FrontendClaim);
        let result = self.transact_selected(
            proof,
            |state, proof| super::native_caller::before(state, input, proof),
            |state, proof| self.claim_compound_guarded(state, input, None, Caller::Raw, proof),
        );
        after(result, fault).map(super::native_caller::ToolMutation::into_outcome)
    }
    pub(in crate::memory) fn claim_tools_guarded(
        &self,
        state: &mut super::super::State,
        claim: &ToolContinuationClaim,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.claim_compound_guarded(
            state,
            ToolInput::Claim(claim),
            preparation,
            caller,
            read_proof,
        )
    }
    pub(in crate::memory) fn claim_frontend_guarded(
        &self,
        state: &mut State,
        request: &crate::FrontendContinuationClaim,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.claim_compound_guarded(
            state,
            ToolInput::FrontendClaim(request),
            Some(preparation),
            caller,
            read_proof,
        )
    }
    fn claim_compound_guarded(
        &self,
        state: &mut super::super::State,
        input: ToolInput<'_>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        let (claim, frontend) = input.claiming()?;
        let digest = super::native_caller::digest(state, input, read_proof)?;
        let fault = fault(self);
        if let Some(saved) = super::native_caller::replay(state, input, &digest, read_proof)? {
            return Ok((super::native_caller::ToolMutation::Replayed(saved), false));
        }
        let run = super::native_caller::selected_run(state, input, read_proof)?;
        let (expected_revision, completion) = input.completion(preparation, &digest, read_proof)?;
        if revision(state, &run.session_id) != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = ready_with_proof(state, &run, claim, frontend, read_proof)?;
        read_proof
            .measure(&run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let now = self.clock.now();
        let activated = domain::canonical::activated_run(&run, &claim.owner, claim.lease_ms, now)?;
        let crate::native_enrollment::caller::tool::ParentGrant {
            previous,
            parent,
            request: reserve,
            authority,
        } = reserve_parent(
            state,
            &activated,
            claim,
            &record,
            expected_revision,
            read_proof,
        )?;
        let phases =
            super::phases::claim(state, &activated, &parent, &record, &authority, read_proof)?;
        let frontend_phases = match frontend {
            Some(request) => super::phases::claim_frontend(
                state, &activated, &parent, &record, request, &authority, read_proof,
            )?,
            None => IssuedPhases::empty(),
        };

        let publication = super::activation::save(
            state,
            &run,
            &activated,
            completion.as_ref(),
            preparation,
            read_proof,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
        read_proof
            .measure(&activated)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        state.runs.insert(run.id.clone(), activated.clone());
        crate::native_enrollment::caller::measure_record(&parent, read_proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        state
            .attempts
            .insert(parent.snapshot.target.id.clone(), parent.clone());
        input
            .measure(read_proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        record.capture_claim(claim, frontend);
        super::native_caller::save_record(state, &record, read_proof)?;
        let native_phases = crate::native_enrollment::caller::tool::records(
            phases.records,
            frontend_phases.records,
            read_proof,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
        let outcome = super::native_caller::finish(
            state,
            caller,
            ToolCompletion {
                input,
                previous: &run,
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
        )?;
        before(fault)?;
        Ok((outcome, true))
    }
}
fn ready_with_proof(
    state: &State,
    run: &Run,
    claim: &ToolContinuationClaim,
    frontend: Option<&crate::FrontendContinuationClaim>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ToolContinuationRecord, Error> {
    let record = waiting_context_with_proof(state, run, proof)?.ok_or(Error::InvalidTransition)?;
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
    waiting_context_with_proof(
        state,
        run,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
}
pub(super) fn waiting_context_with_proof(
    state: &State,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let Some(record) = super::active_with_proof(state, run, proof)? else {
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
    crate::native_enrollment::caller::measure_record(previous, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    previous
        .validate()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if previous.snapshot.target != record.batch.attempt
        || previous.snapshot.state != AttemptState::Suspended
        || previous.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    waiting_calls(state, run, &record, proof)?;
    Ok(Some(record))
}

pub(in crate::memory) fn preparation_context(
    state: &State,
    run: &Run,
    claim: &ToolContinuationClaim,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ToolContinuationRecord, Error> {
    ready_with_proof(state, run, claim, None, proof)
}

pub(in crate::memory) fn preparation_frontend_context(
    state: &State,
    run: &Run,
    request: &crate::FrontendContinuationClaim,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ToolContinuationRecord, Error> {
    ready_with_proof(state, run, &request.claim, Some(request), proof)
}

fn waiting_calls(
    state: &State,
    run: &Run,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof
        .charge(65 * std::mem::size_of::<&crabber_core::ToolCallRecord>())
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
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
        if let Some(phase) = latest {
            crate::native_enrollment::caller::measure_phase_record(phase, proof)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        let frontend = record
            .batch
            .frontend
            .iter()
            .find(|f| f.target.call_id == call.id);
        let origin = super::super::frontend::origin_with_proof(state, run, &call.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if let Some(frontend) = frontend {
            super::super::frontend::verify_frozen_with_proof(state, run, frontend, proof)
                .map_err(|_| Error::UnknownStoreOutcome)?;
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
    super::evidence::validate_with_proof(state, run, None, proof)?;
    Ok(())
}

fn reserve_parent(
    state: &State,
    activated: &Run,
    claim: &ToolContinuationClaim,
    record: &ToolContinuationRecord,
    expected_revision: crabber_core::SessionRevision,
    read_proof: &mut Proof,
) -> Result<crate::native_enrollment::caller::tool::ParentGrant, Error> {
    let previous = state
        .attempts
        .get(&record.batch.attempt.id)
        .ok_or(Error::NotFound)?;
    crate::native_enrollment::caller::measure_record(previous, read_proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let previous = previous.clone();
    read_proof
        .measure(&record.batch.next_attempt)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let reserve = AttemptMutation {
        id: domain::derived_id("tool-continuation-claim-attempt", &claim.id),
        expected_revision,
        command: AttemptCommand::Reserve {
            target: record.batch.next_attempt.clone(),
        },
    };
    let parent =
        crate::attempt::AttemptRecord::reserve(activated, &reserve, Some(&previous.snapshot))
            .map_err(|_| Error::CleanupBlocked)?;
    crate::native_enrollment::caller::measure_record(&parent, read_proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(crate::native_enrollment::caller::tool::ParentGrant {
        previous,
        parent,
        request: reserve,
        authority,
    })
}
