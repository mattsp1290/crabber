use super::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, Error, MemoryStore,
    PhaseQuiescence, PhaseState, State, ToolContinuationClaimOutcome, ToolContinuationOutcome,
    ToolContinuationRecord, ToolContinuationState, after, before, domain, fault, publish, receipt,
    replay, revision,
};
use crate::ToolContinuationRecovery;
use crabber_core::{Run, RunFence, RunStatus, ToolCallStatus};
impl MemoryStore {
    pub(in super::super) fn recover_tools(
        &self,
        request: &ToolContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        self.recover_compound(request, None)
    }
    pub(in super::super) fn recover_frontend(
        &self,
        request: &crate::FrontendContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let common = ToolContinuationRecovery {
            id: request.id.clone(),
            expected_revision: request.expected_revision,
            original: request.original.claim.clone(),
            attempt: request.attempt.clone(),
            owner: request.owner.clone(),
            lease_ms: request.lease_ms,
        };
        self.recover_compound(&common, Some(request))
    }
    fn recover_compound(
        &self,
        request: &ToolContinuationRecovery,
        frontend: Option<&crate::FrontendContinuationRecovery>,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let digest = match frontend {
            Some(request) => request.fingerprint()?,
            None => request.fingerprint()?,
        };
        let fault = fault(self);
        let result = self.transact(|state| {
            let original = &request.original;
            if let Some(saved) = replay(
                state,
                &original.session_id,
                &original.run_id,
                &request.id,
                &digest,
            )? {
                match frontend {
                    Some(request) => request.verify_receipt(&saved)?,
                    None => request.verify_receipt(&saved)?,
                }
                return Ok((ToolContinuationClaimOutcome::Replayed(saved), false));
            }
            let run = state
                .runs
                .get(&original.run_id)
                .filter(|run| run.session_id == original.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if revision(state, &run.session_id) != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let mut record = eligible(state, &run, request, frontend)?;
            let now = self.clock.now();
            let activated =
                domain::canonical::activated_run(&run, &request.owner, request.lease_ms, now)?;
            let mut parent = state
                .attempts
                .get(&request.attempt.id)
                .cloned()
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
            let authority = parent.authority().map_err(|_| Error::UnknownStoreOutcome)?;
            let phases = super::phases::reclaim(state, &parent, &record)?;
            let frontend_phases = if frontend.is_some() {
                super::phases::reclaim_frontend(state, &parent, &record)?
            } else {
                Vec::new()
            };
            let fence = RunFence {
                run_id: run.id.clone(),
                claim_token: activated.claim_token.clone(),
            };
            super::super::native_parent::record_run(state, Some(&run), &activated)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            state.runs.insert(run.id.clone(), activated);
            state
                .attempts
                .insert(request.attempt.id.clone(), parent.clone());
            record.capture_recovery(request, frontend)?;
            record.validate()?;
            state
                .tool_continuations
                .insert(record.batch.continuation.id.clone(), record.clone());
            let receipt = publish(
                state,
                &run,
                request.id.clone(),
                digest,
                record.recovered_outcome(parent.snapshot)?,
                now,
            )?;
            before(fault)?;
            Ok((
                ToolContinuationClaimOutcome::Applied {
                    receipt,
                    grant: record.into_grant(fence, authority, phases, frontend_phases)?,
                },
                true,
            ))
        });
        after(result, fault)
    }
}
fn eligible(
    state: &State,
    run: &Run,
    request: &ToolContinuationRecovery,
    frontend: Option<&crate::FrontendContinuationRecovery>,
) -> Result<ToolContinuationRecord, Error> {
    let original = receipt(state, &run.session_id, &request.original.id)?.ok_or(Error::NotFound)?;
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
    let record = super::active(state, run)?.ok_or(Error::InvalidTransition)?;
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
    parent.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if parent.snapshot.target != request.attempt
        || parent.snapshot.state != AttemptState::Unstarted
        || parent.snapshot.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::CleanupBlocked);
    }
    validate_reserved(state, run, &record)?;
    Ok(record)
}
fn validate_reserved(
    state: &State,
    run: &Run,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    for call in &record.batch.calls {
        let stored = state
            .calls
            .get(&call.target.call_id)
            .ok_or(Error::NotFound)?;
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
    super::evidence::validate_excluding(
        state,
        run,
        &record
            .batch
            .phases
            .iter()
            .map(|phase| phase.next.clone())
            .chain(record.batch.frontend.iter().map(|call| call.target.clone()))
            .collect::<Vec<_>>(),
    )?;
    Ok(())
}
