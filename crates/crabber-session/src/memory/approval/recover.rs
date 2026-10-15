//! Fresh reclaim of a consumed approval whose planned attempt never began.
use super::{Error, publish, receipt, replay, revision};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationRecovery, ApprovalGrant, ApprovalOutcome,
    AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState,
    approval,
};
use crabber_core::RunFence;
impl super::super::MemoryStore {
    pub(in super::super) fn recover_approval(
        &self,
        request: &ApprovalContinuationRecovery,
    ) -> Result<ApprovalClaimOutcome, Error> {
        let digest = request.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let original = &request.original;
        let result = self.transact(|state| {
            if let Some(saved) = replay(
                state,
                &original.session_id,
                &original.run_id,
                &request.id,
                &digest,
            )? {
                request.verify_receipt(&saved)?;
                return Ok((ApprovalClaimOutcome::Replayed(saved), false));
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
            let current = state
                .approval_barriers
                .get(&run.session_id)
                .ok_or(Error::InvalidTransition)?;
            let record = state
                .approvals
                .get(current)
                .cloned()
                .ok_or(Error::UnknownStoreOutcome)?;
            super::verify_record(state, &run, &record)?;
            let parent = receipt(state, &run.session_id, &original.id)?.ok_or(Error::NotFound)?;
            let now = self.clock.now();
            let calls = record.recovery_grants(&run, request, &parent, now)?;
            validate_native(state, &run, request, &record, now)?;
            super::claim::validate_calls(state, &run, &record)?;
            let activated = approval::activate_recovery(request, &run, now)?;
            let fence = RunFence {
                run_id: run.id.clone(),
                claim_token: activated.claim_token.clone(),
            };
            state.runs.insert(run.id.clone(), activated);
            let execution = super::super::MemoryExecution {
                store: self.clone(),
                fence: fence.clone(),
            };
            let mutation = AttemptMutation {
                id: approval::derived_id("reclaim-attempt", &request.id),
                expected_revision: request.expected_revision,
                command: AttemptCommand::ReclaimUnstarted {
                    target: request.attempt.clone(),
                },
            };
            let AttemptMutationOutcome::Applied {
                receipt: native,
                authority: Some(authority),
            } = execution
                .apply_attempt(state, mutation, None, None)
                .map_err(approval::attempt_error)?
            else {
                return Err(Error::UnknownStoreOutcome);
            };
            let saved = publish(
                state,
                &run,
                request.id.clone(),
                digest.clone(),
                ApprovalOutcome::ReclaimedUnstarted {
                    continuation: original.continuation.clone(),
                    original_claim_id: original.id.clone(),
                    attempt: native.attempt,
                },
                now,
            )?;
            request.verify_receipt(&saved)?;
            super::before_commit(fault)?;
            Ok((
                ApprovalClaimOutcome::Applied {
                    receipt: saved,
                    grant: Box::new(ApprovalGrant {
                        fence,
                        authority,
                        calls,
                        source_turn_id: record.batch.source_turn_id,
                        usage: record.batch.usage,
                        turns_used: record.batch.turns_used.ok_or(Error::UnknownStoreOutcome)?,
                    }),
                },
                true,
            ))
        });
        super::after_commit(result, fault)
    }
}
fn validate_native(
    state: &super::super::State,
    run: &crabber_core::Run,
    request: &ApprovalContinuationRecovery,
    record: &approval::ApprovalRecord,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    validate_provenance(state, record)?;
    let old = state
        .attempts
        .get(&record.batch.attempt.id)
        .ok_or(Error::NotFound)?;
    old.validate().map_err(approval::attempt_error)?;
    if old.snapshot.target != record.batch.attempt
        || old.snapshot.session_id != run.session_id
        || old.snapshot.run_id != run.id
        || old.snapshot.state != AttemptState::Suspended
        || old.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    let latest = state
        .attempts
        .values()
        .filter(|record| record.snapshot.run_id == run.id)
        .max_by_key(|record| record.snapshot.target.generation)
        .ok_or(Error::NotFound)?;
    latest.validate().map_err(approval::attempt_error)?;
    if latest.snapshot.target != request.attempt
        || latest.snapshot.session_id != run.session_id
        || latest.snapshot.state != AttemptState::Unstarted
        || latest.snapshot.cleanup != AttemptCleanup::Pending
    {
        return Err(Error::InvalidTransition);
    }
    if latest.lease_until > now {
        return Err(Error::LiveOwner);
    }
    if state.phases.values().any(|phase| {
        phase.snapshot.run_id == run.id
            && (phase.snapshot.target.attempt == request.attempt
                || record
                    .batch
                    .calls
                    .iter()
                    .any(|call| call.target.call_id == phase.snapshot.target.call_id))
    }) {
        return Err(Error::InvalidTransition);
    }
    Ok(())
}

pub(super) fn validate_provenance(
    state: &super::super::State,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let saved =
        receipt(state, &record.session_id, &record.batch.id)?.ok_or(Error::UnknownStoreOutcome)?;
    record.verify_recovery_batch(&saved)?;
    for question in &record.questions {
        let answer = question.answer.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        let saved =
            receipt(state, &record.session_id, &answer.id)?.ok_or(Error::UnknownStoreOutcome)?;
        approval::verify_recovery_answer(answer, &saved)?;
    }
    Ok(())
}
