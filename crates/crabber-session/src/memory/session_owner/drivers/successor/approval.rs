//! Fresh eligibility is current native truth, not original metadata or a holder proof.
use super::{Error, Proof, State};
use crabber_core::Run;

pub(super) fn fresh(
    state: &State,
    run: &Run,
    target: &crate::ContinuationTarget,
    parent: &crate::AttemptTarget,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    unstarted(state, run, parent, proof)?;
    let record = state
        .approvals
        .get(&target.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    super::super::measure(proof, record)?;
    if crate::snapshot::encoded_bytes(record, 4 << 20).is_none()
        || record.batch.continuation != *target
        || record.batch.next_attempt != *parent
        || record.state != crate::ApprovalContinuationState::Claimed
        || state.approval_barriers.get(&run.session_id) != Some(&target.id)
    {
        return Err(Error::StaleOwner);
    }
    crate::memory::approval::origin::verify_with_proof(state, run, record, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::memory::approval::current::verify_with_proof(state, run, record, false, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if state.tool_barriers.contains_key(&run.session_id) {
        return Err(Error::StaleOwner);
    }
    suspended(state, &record.batch, proof)?;
    let current = state
        .attempts
        .get(&parent.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let carrier = (
        &current.snapshot,
        &current.owner,
        &current.token,
        &current.run_token,
        current.lease_until,
    );
    super::super::measure(proof, &carrier)?;
    current.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if crate::snapshot::encoded_bytes(&carrier, 8192).is_none()
        || current.snapshot.session_id != run.session_id
        || current.snapshot.run_id != run.id
        || current.snapshot.target != *parent
        || current.snapshot.state != crate::AttemptState::Unstarted
        || current.snapshot.cleanup != crate::AttemptCleanup::Pending
        || current.run_token != run.claim_token
        || current.lease_until <= now
        || state.phases.values().any(|phase| {
            phase.snapshot.run_id == run.id && phase.snapshot.target.attempt == *parent
        })
    {
        return Err(Error::StaleOwner);
    }
    for attempt in state
        .attempts
        .values()
        .filter(|a| a.snapshot.run_id == run.id)
    {
        super::super::measure(proof, &attempt.snapshot)?;
        if attempt.snapshot.target.generation > parent.generation {
            return Err(Error::StaleOwner);
        }
    }
    let mut count = 0;
    for call in state.calls.values().filter(|c| {
        c.run_id == run.id
            && matches!(
                c.status,
                crabber_core::ToolCallStatus::Pending | crabber_core::ToolCallStatus::Running
            )
    }) {
        super::super::measure(proof, call)?;
        if crate::snapshot::encoded_bytes(call, 4 << 20).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        let frozen = record
            .batch
            .calls
            .iter()
            .find(|c| c.target.call_id == call.id)
            .ok_or(Error::SemanticConflict)?;
        if call.status != crabber_core::ToolCallStatus::Pending
            || call.result.is_some()
            || crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
                != frozen.call_binding
        {
            return Err(Error::SemanticConflict);
        }
        count += 1;
        if count > crate::MAX_APPROVAL_CALLS {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    if count != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}

fn unstarted(
    state: &State,
    run: &Run,
    parent: &crate::AttemptTarget,
    proof: &mut Proof,
) -> Result<(), Error> {
    for witness in state
        .native_parent_witnesses
        .values()
        .filter(|w| w.source.session_id == run.session_id && w.source.run_id == run.id)
    {
        super::super::measure(proof, witness)?;
        if crate::snapshot::encoded_bytes(witness, 8192).is_none() || witness.validate().is_err() {
            return Err(Error::UnknownStoreOutcome);
        }
        if witness.source.attempt == *parent {
            return Err(Error::StaleOwner);
        }
    }
    Ok(())
}

fn suspended(
    state: &State,
    batch: &crate::ApprovalSuspension,
    proof: &mut Proof,
) -> Result<(), Error> {
    let old = state
        .attempts
        .get(&batch.attempt.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let old_carrier = (
        &old.snapshot,
        &old.owner,
        &old.token,
        &old.run_token,
        old.lease_until,
    );
    super::super::measure(proof, &old_carrier)?;
    if crate::snapshot::encoded_bytes(&old_carrier, 8192).is_none()
        || old.snapshot.target != batch.attempt
        || old.snapshot.state != crate::AttemptState::Suspended
        || old.snapshot.cleanup != crate::AttemptCleanup::Complete
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
