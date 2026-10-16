use super::{
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptState, Error, MemoryStore,
    PhaseQuiescence, PhaseState, State, ToolContinuationClaimOutcome, ToolContinuationGrant,
    ToolContinuationOutcome, ToolContinuationRecord, ToolContinuationState, after, before, domain,
    fault, publish, receipt, replay, revision,
};
use crate::ToolContinuationRecovery;
use crabber_core::{Run, RunFence, RunStatus, ToolCallStatus};
impl MemoryStore {
    pub(in super::super) fn recover_tools(
        &self,
        request: &ToolContinuationRecovery,
    ) -> Result<ToolContinuationClaimOutcome, Error> {
        let digest = request.fingerprint()?;
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
                request.verify_receipt(&saved)?;
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
            let mut record = eligible(state, &run, request)?;
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
            let calls = record.grants()?;
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
            record.recovery = Some(request.clone());
            state
                .tool_continuations
                .insert(record.batch.continuation.id.clone(), record.clone());
            let receipt = publish(
                state,
                &run,
                request.id.clone(),
                digest,
                ToolContinuationOutcome::ReclaimedUnstarted {
                    continuation: original.continuation.clone(),
                    original_claim_id: original.id.clone(),
                    attempt: parent.snapshot,
                },
                now,
            )?;
            before(fault)?;
            Ok((
                ToolContinuationClaimOutcome::Applied {
                    receipt,
                    grant: Box::new(ToolContinuationGrant {
                        fence,
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
        });
        after(result, fault)
    }
}
fn eligible(
    state: &State,
    run: &Run,
    request: &ToolContinuationRecovery,
) -> Result<ToolContinuationRecord, Error> {
    let original = receipt(state, &run.session_id, &request.original.id)?.ok_or(Error::NotFound)?;
    if original.run_id != run.id
        || original.semantic_digest != request.original.fingerprint()?
        || !matches!(original.outcome,ToolContinuationOutcome::Claimed{ref continuation,ref attempt} if continuation==&request.original.continuation && attempt.target==request.attempt)
    {
        return Err(Error::SemanticConflict);
    }
    let record = super::active(state, run)?.ok_or(Error::InvalidTransition)?;
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
    super::evidence::validate_excluding(
        state,
        run,
        &record
            .batch
            .phases
            .iter()
            .map(|phase| phase.next.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(record)
}
