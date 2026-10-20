use super::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, Error, MemoryStore,
    PhaseQuiescence, PhaseState, State, ToolContinuationClaimOutcome, ToolContinuationOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, fault, revision,
};
use crate::ToolContinuationRecovery;
use crate::native_enrollment::caller::{
    Caller,
    tool::{IssuedPhases, ToolCompletion, ToolGrantOutput, ToolInput},
};
use crate::public_session::v2::proof::Proof;
use crabber_core::{Run, RunStatus, ToolCallStatus};
impl MemoryStore {
    pub(in super::super) fn recover_tools(
        &self,
        request: &ToolContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(ToolInput::Recovery(request), &mut Proof::bounded(32 << 20))
    }
    pub(in super::super) fn recover_frontend(
        &self,
        request: &crate::FrontendContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.tool_with_proof(
            ToolInput::FrontendRecovery(request),
            &mut Proof::bounded(32 << 20),
        )
    }

    pub(in crate::memory) fn recover_compound_with_proof(
        &self,
        input: ToolInput<'_>,
        proof: &mut Proof,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let fault = fault(self);
        let result = self.transact_selected(
            proof,
            |state, proof| super::native_caller::before(state, input, proof),
            |state, proof| self.recover_compound_guarded(state, input, None, Caller::Raw, proof),
        );
        after(result, fault).map(super::native_caller::ToolMutation::into_outcome)
    }
    pub(in crate::memory) fn recover_tools_guarded(
        &self,
        state: &mut super::super::State,
        request: &ToolContinuationRecovery,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.recover_compound_guarded(
            state,
            ToolInput::Recovery(request),
            preparation,
            caller,
            read_proof,
        )
    }
    pub(in crate::memory) fn recover_frontend_guarded(
        &self,
        state: &mut State,
        request: &crate::FrontendContinuationRecovery,
        preparation: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        self.recover_compound_guarded(
            state,
            ToolInput::FrontendRecovery(request),
            Some(preparation),
            caller,
            read_proof,
        )
    }
    fn recover_compound_guarded(
        &self,
        state: &mut super::super::State,
        input: ToolInput<'_>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(super::native_caller::ToolMutation, bool), Error> {
        let digest = super::native_caller::digest(state, input, read_proof)?;
        let fault = fault(self);
        if let Some(saved) = super::native_caller::replay(state, input, &digest, read_proof)? {
            return Ok((super::native_caller::ToolMutation::Replayed(saved), false));
        }
        let run = super::native_caller::selected_run(state, input, read_proof)?;
        let (common, frontend) = input.recovering(read_proof)?;
        let request = common.as_ref();
        let (expected_revision, completion) = input.completion(preparation, &digest, read_proof)?;
        if revision(state, &run.session_id) != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let mut record = eligible_with_proof(state, &run, request, frontend, read_proof)?;
        read_proof
            .measure(&run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let now = self.clock.now();
        let activated =
            domain::canonical::activated_run(&run, &request.owner, request.lease_ms, now)?;
        let previous = state
            .attempts
            .get(&request.attempt.id)
            .ok_or(Error::NotFound)?;
        crate::native_enrollment::caller::measure_record(previous, read_proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let previous = previous.clone();
        read_proof
            .measure(&request.attempt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let native_request = AttemptMutation {
            id: domain::derived_id("tool-continuation-reclaim-attempt", &request.id),
            expected_revision,
            command: AttemptCommand::ReclaimUnstarted {
                target: request.attempt.clone(),
            },
        };
        let parent =
            reclaimed_parent_with_proof(&previous, &activated, &native_request, now, read_proof)?;
        crate::native_enrollment::caller::measure_record(&parent, read_proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
        let phases = super::phases::reclaim(state, &record, &authority, read_proof)?;
        let frontend_phases = if frontend.is_some() {
            super::phases::reclaim_frontend(state, &record, &authority, read_proof)?
        } else {
            IssuedPhases::empty()
        };
        let publication = save_recovered_owner(
            state,
            &run,
            &activated,
            &parent,
            completion.as_ref(),
            preparation,
            read_proof,
        )?;
        input
            .measure(read_proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        record.capture_recovery(request, frontend)?;
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
        )?;
        before(fault)?;
        Ok((outcome, true))
    }
}
fn eligible_with_proof(
    state: &State,
    run: &Run,
    request: &ToolContinuationRecovery,
    frontend: Option<&crate::FrontendContinuationRecovery>,
    proof: &mut Proof,
) -> Result<ToolContinuationRecord, Error> {
    let original = super::receipt_with_proof(state, &run.session_id, &request.original.id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?
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
    let record = super::active_with_proof(state, run, proof)?.ok_or(Error::InvalidTransition)?;
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
    if record.run_id != run.id
        || record.state != ToolContinuationState::Claimed
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
    let parent = state
        .attempts
        .values()
        .filter(|parent| parent.snapshot.run_id == run.id)
        .max_by_key(|parent| parent.snapshot.target.generation)
        .ok_or(Error::NotFound)?;
    crate::native_enrollment::caller::measure_record(parent, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    parent.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if parent.snapshot.target != request.attempt
        || parent.snapshot.state != AttemptState::Unstarted
        || parent.snapshot.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::CleanupBlocked);
    }
    validate_reserved_with_proof(state, run, &record, proof)?;
    Ok(record)
}
fn validate_reserved_with_proof(
    state: &State,
    run: &Run,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    for call in &record.batch.calls {
        let stored = state
            .calls
            .get(&call.target.call_id)
            .ok_or(Error::NotFound)?;
        proof
            .measure(stored)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        call.verify_call(stored)
            .map_err(|_| Error::SemanticConflict)?;
        if stored.run_id != run.id
            || stored.status != ToolCallStatus::Pending
            || stored.result.is_some()
        {
            return Err(Error::CleanupBlocked);
        }
    }
    let unfinished = state
        .calls
        .values()
        .filter(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
                )
        })
        .count();
    if unfinished != record.batch.calls.len() {
        return Err(Error::CleanupBlocked);
    }
    for frozen in &record.batch.phases {
        let phase = state
            .phases
            .get(&frozen.next.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::native_enrollment::caller::measure_phase_record(phase, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if phase.snapshot.target != frozen.next
            || phase.snapshot.call_binding != frozen.call_binding
            || phase.snapshot.session_id != run.session_id
            || phase.snapshot.run_id != run.id
            || phase.snapshot.state != PhaseState::Claimed
            || phase.snapshot.quiescence != PhaseQuiescence::Pending
            || phase.output.is_some()
        {
            return Err(Error::CleanupBlocked);
        }
    }
    for frozen in &record.batch.frontend {
        let phase = state
            .phases
            .get(&frozen.target.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::native_enrollment::caller::measure_phase_record(phase, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        phase.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        if phase.snapshot.target != frozen.target
            || phase.snapshot.call_binding != frozen.call_binding
            || phase.snapshot.session_id != run.session_id
            || phase.snapshot.run_id != run.id
            || phase.snapshot.state != PhaseState::Claimed
            || phase.snapshot.quiescence != PhaseQuiescence::Pending
            || phase.output.is_some()
        {
            return Err(Error::CleanupBlocked);
        }
    }
    proof
        .measure(&(&record.batch.phases, &record.batch.frontend))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::evidence::validate_excluding_with_proof(
        state,
        run,
        &record
            .batch
            .phases
            .iter()
            .map(|phase| phase.next.clone())
            .chain(record.batch.frontend.iter().map(|call| call.target.clone()))
            .collect::<Vec<_>>(),
        proof,
    )?;
    Ok(())
}

pub(in crate::memory) fn preparation_context(
    state: &State,
    run: &Run,
    request: &ToolContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    eligible_with_proof(state, run, request, None, proof)?;
    if state
        .attempts
        .get(&request.attempt.id)
        .ok_or(Error::NotFound)?
        .lease_until
        > now
    {
        return Err(Error::LiveOwner);
    }
    Ok(())
}

fn save_recovered_owner(
    state: &mut State,
    original: &Run,
    activated: &Run,
    parent: &crate::attempt::AttemptRecord,
    completion: Option<&crate::native_parent::CompletionSource>,
    preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crate::native_enrollment::caller::initial::RecoveryPublication, Error> {
    let publication = super::activation::save(
        state,
        original,
        activated,
        completion,
        preparation,
        read_proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    read_proof
        .measure(activated)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state.runs.insert(original.id.clone(), activated.clone());
    crate::native_enrollment::caller::measure_record(parent, read_proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state
        .attempts
        .insert(parent.snapshot.target.id.clone(), parent.clone());
    Ok(publication)
}

pub(in crate::memory) fn preparation_frontend_context(
    state: &State,
    run: &Run,
    request: &crate::FrontendContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    proof
        .measure(&(&request.original.claim, &request.attempt, &request.owner))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    eligible_with_proof(
        state,
        run,
        &domain::canonical::frontend_recovery(request),
        Some(request),
        proof,
    )?;
    if state
        .attempts
        .get(&request.attempt.id)
        .ok_or(Error::NotFound)?
        .lease_until
        > now
    {
        return Err(Error::LiveOwner);
    }
    Ok(())
}

fn reclaimed_parent_with_proof(
    previous: &crate::attempt::AttemptRecord,
    activated: &Run,
    request: &AttemptMutation,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<crate::attempt::AttemptRecord, Error> {
    if previous.lease_until > now {
        return Err(Error::LiveOwner);
    }
    crate::native_enrollment::caller::measure_record(previous, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut parent = previous.clone();
    parent
        .change(activated, request, None, now, true)
        .map_err(|_| Error::CleanupBlocked)?;
    Ok(parent)
}
