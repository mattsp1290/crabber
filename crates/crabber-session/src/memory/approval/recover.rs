//! Fresh reclaim of a consumed approval whose planned attempt never began.
use super::{Error, publish, revision};
use crate::native_enrollment::caller::{ApprovalCompletion, ApprovalInput, ApprovalResult, Caller};
use crate::public_session::v2::proof::Proof;
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationRecovery, ApprovalOutcome, AttemptCleanup,
    AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState, approval,
};
use crabber_core::RunFence;
impl super::super::MemoryStore {
    pub(in super::super) fn recover_approval(
        &self,
        request: &ApprovalContinuationRecovery,
    ) -> Result<ApprovalClaimOutcome, Error> {
        self.recover_approval_with_proof(request, &mut Proof::bounded(32 << 20))
    }
    pub(in crate::memory) fn recover_approval_with_proof(
        &self,
        request: &ApprovalContinuationRecovery,
        proof: &mut Proof,
    ) -> Result<ApprovalClaimOutcome, Error> {
        let fault = super::fault(self);
        let result = self.transact_selected(
            proof,
            |state, proof| {
                super::native_caller::before(state, ApprovalInput::Recovery(request), proof)
            },
            |state, proof| self.recover_approval_guarded(state, request, None, Caller::Raw, proof),
        );
        super::after_commit(result, fault)
    }
    pub(in crate::memory) fn recover_approval_guarded(
        &self,
        state: &mut super::super::State,
        request: &ApprovalContinuationRecovery,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        caller: Caller,
        read_proof: &mut Proof,
    ) -> Result<(ApprovalClaimOutcome, bool), Error> {
        let digest =
            super::native_caller::digest(state, ApprovalInput::Recovery(request), read_proof)?;
        let fault = super::fault(self);

        let original = &request.original;
        if let Some(saved) = super::replay_with_proof(
            state,
            &original.session_id,
            &original.run_id,
            &request.id,
            &digest,
            read_proof,
        )? {
            request.verify_receipt(&saved)?;
            return Ok((ApprovalClaimOutcome::Replayed(saved), false));
        }
        let run = super::native_caller::selected_run(
            state,
            ApprovalInput::Recovery(request),
            read_proof,
        )?;
        let (expected_revision, completion) = crate::native_parent::CompletionSource::control(
            preparation,
            crate::native_parent::CompletionOperation::ApprovalRecovery,
            &crate::session_owner::preparation::PreparationKey::Approval(request.id.clone()),
            (&original.session_id, &original.run_id),
            request.expected_revision,
            &digest,
        )
        .map_err(|_| Error::SemanticConflict)?;
        if revision(state, &run.session_id) != expected_revision {
            return Err(Error::RevisionConflict);
        }
        let now = self.clock.now();
        let (record, calls) = ready(state, &run, request, now, read_proof)?;
        let (fence, captured, activated) =
            activate(state, &run, request, now, completion.as_ref(), read_proof)?;
        if let Some(preparation) = preparation {
            preparation
                .record_continuation_claim(captured)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        let execution = super::super::MemoryExecution {
            store: self.clone(),
            fence: fence.clone(),
        };
        let (mutation, native, authority) =
            reclaim(state, &execution, request, expected_revision, read_proof)?;
        read_proof
            .measure(&native.attempt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let saved = publish(
            state,
            &run,
            request.id.clone(),
            digest.clone(),
            ApprovalOutcome::ReclaimedUnstarted {
                continuation: original.continuation.clone(),
                original_claim_id: original.id.clone(),
                attempt: native.attempt.clone(),
            },
            now,
        )?;
        request.verify_receipt(&saved)?;
        let outcome = super::native_caller::issue(
            state,
            caller,
            ApprovalCompletion {
                input: ApprovalInput::Recovery(request),
                previous: &run,
                activated: &activated,
                record: &record,
                native_request: &mutation,
                native_receipt: &native,
            },
            ApprovalResult {
                receipt: saved,
                fence,
                authority,
                calls,
            },
            read_proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )?;
        super::before_commit(fault)?;
        Ok((outcome, true))
    }
}
fn activate(
    state: &mut super::super::State,
    run: &crabber_core::Run,
    request: &ApprovalContinuationRecovery,
    now: time::OffsetDateTime,
    completion: Option<&crate::native_parent::CompletionSource>,
    proof: &mut Proof,
) -> Result<(RunFence, Option<u64>, crabber_core::Run), Error> {
    let activated = approval::activate_recovery(request, run, now)?;
    let fence = RunFence {
        run_id: run.id.clone(),
        claim_token: activated.claim_token.clone(),
    };
    let captured = super::super::native_parent::record_run_completed_captured_with_proof(
        state, run, &activated, completion, proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    proof
        .measure(&activated)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state.runs.insert(run.id.clone(), activated.clone());
    Ok((fence, captured, activated))
}

fn validate_native_with_proof(
    state: &super::super::State,
    run: &crabber_core::Run,
    request: &ApprovalContinuationRecovery,
    record: &approval::ApprovalRecord,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    validate_provenance_with_proof(state, record, proof)?;
    let old = state
        .attempts
        .get(&record.batch.attempt.id)
        .ok_or(Error::NotFound)?;
    crate::native_enrollment::caller::measure_record(old, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
    crate::native_enrollment::caller::measure_record(latest, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
    validate_provenance_with_proof(state, record, &mut Proof::legacy())
}
pub(super) fn validate_provenance_with_proof(
    state: &super::super::State,
    record: &approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    let run = state
        .runs
        .get(&record.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    super::verify_record_with_proof(state, run, record, proof)?;
    for question in &record.questions {
        let answer = question.answer.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        let saved = super::receipt_with_proof(state, &record.session_id, &answer.id, proof)
            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
            .ok_or(Error::UnknownStoreOutcome)?;
        approval::verify_recovery_answer(answer, &saved)?;
    }
    Ok(())
}

pub(in crate::memory) fn preparation_context(
    state: &super::super::State,
    run: &crabber_core::Run,
    request: &ApprovalContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    ready(state, run, request, now, proof)?;
    Ok(())
}

fn ready(
    state: &super::super::State,
    run: &crabber_core::Run,
    request: &ApprovalContinuationRecovery,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(approval::ApprovalRecord, Vec<crate::GrantedApproval>), Error> {
    let original = &request.original;
    let current = state
        .approval_barriers
        .get(&run.session_id)
        .ok_or(Error::InvalidTransition)?;
    let record = state
        .approvals
        .get(current)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let record = record.clone();
    super::verify_record_with_proof(state, run, &record, proof)?;
    let parent = super::receipt_with_proof(state, &run.session_id, &original.id, proof)
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
        .ok_or(Error::NotFound)?;
    proof
        .measure(&record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let calls = record.recovery_grants(run, request, &parent, now)?;
    validate_native_with_proof(state, run, request, &record, now, proof)?;
    super::claim::validate_calls_with_proof(state, run, &record, proof)?;

    Ok((record, calls))
}

fn reclaim(
    state: &mut super::super::State,
    execution: &super::super::MemoryExecution,

    request: &ApprovalContinuationRecovery,

    expected_revision: crabber_core::SessionRevision,
    proof: &mut Proof,
) -> Result<
    (
        AttemptMutation,
        crate::AttemptReceipt,
        crate::AttemptAuthority,
    ),
    Error,
> {
    let mutation = AttemptMutation {
        id: approval::derived_id("reclaim-attempt", &request.id),
        expected_revision,
        command: AttemptCommand::ReclaimUnstarted {
            target: request.attempt.clone(),
        },
    };
    proof
        .measure(&mutation)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let AttemptMutationOutcome::Applied {
        receipt: native,
        authority: Some(authority),
    } = execution
        .apply_attempt_with_proof(state, mutation.clone(), None, proof)
        .map_err(approval::attempt_error)?
    else {
        return Err(Error::UnknownStoreOutcome);
    };

    Ok((mutation, native, authority))
}
