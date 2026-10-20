//! Fresh current Tool/Frontend guards, repeated at registration and activation.
use super::{Error, Proof, State};
use crate::session_owner::driver::successor::tool_fresh as check;
use crabber_core::Run;

pub(super) fn fresh(
    state: &State,
    run: &Run,
    target: &crate::ContinuationTarget,
    parent: &crate::AttemptTarget,
    receipt: &crate::ToolContinuationReceipt,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::approval::unstarted(state, run, parent, proof)?;
    let record =
        crate::memory::tool_continuation::checked_record_with_proof(state, &target.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    if record.batch.continuation != *target
        || record.batch.next_attempt != *parent
        || record.state != crate::ToolContinuationState::Claimed
        || state.tool_barriers.get(&run.session_id) != Some(&target.id)
        || state.approval_barriers.contains_key(&run.session_id)
    {
        return Err(Error::StaleOwner);
    }
    for other in state
        .tool_continuations
        .values()
        .filter(|v| v.session_id == run.session_id && v.run_id == run.id)
    {
        super::super::measure(proof, &other.batch.continuation)?;
        if other.batch.continuation.generation > target.generation {
            return Err(Error::StaleOwner);
        }
    }
    check::source(&record, receipt)?;
    parents(state, run, &record.batch, now, proof)?;
    phases(state, run, &record.batch, proof)?;
    let mut count = 0;
    for call in state.calls.values().filter(|c| {
        c.run_id == run.id
            && matches!(
                c.status,
                crabber_core::ToolCallStatus::Waiting
                    | crabber_core::ToolCallStatus::Pending
                    | crabber_core::ToolCallStatus::Running
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
            .find(|v| v.target.call_id == call.id)
            .ok_or(Error::SemanticConflict)?;
        check::call(run, call, frozen)?;
        count += 1;
    }
    if count != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
fn parents(
    state: &State,
    run: &Run,
    batch: &crate::ToolContinuationSuspension,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), Error> {
    for (target, current) in [(&batch.attempt, false), (&batch.next_attempt, true)] {
        let record = state
            .attempts
            .get(&target.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        let carrier = (
            &record.snapshot,
            &record.owner,
            &record.token,
            &record.run_token,
            record.lease_until,
        );
        super::super::measure(proof, &carrier)?;
        if crate::snapshot::encoded_bytes(&carrier, 8192).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        check::parent(run, record, target, current, now)?;
    }
    for attempt in state
        .attempts
        .values()
        .filter(|v| v.snapshot.run_id == run.id)
    {
        super::super::measure(proof, &attempt.snapshot)?;
        if attempt.snapshot.target.generation > batch.next_attempt.generation {
            return Err(Error::StaleOwner);
        }
    }
    Ok(())
}
fn phases(
    state: &State,
    run: &Run,
    batch: &crate::ToolContinuationSuspension,
    proof: &mut Proof,
) -> Result<(), Error> {
    let mut count = 0;
    for (key, phase) in state.phases.iter().filter(|(_, v)| {
        v.snapshot.run_id == run.id && v.snapshot.target.attempt == batch.next_attempt
    }) {
        super::super::measure(proof, &(&key, &phase.snapshot))?;
        if key != &phase.snapshot.target.id {
            return Err(Error::UnknownStoreOutcome);
        }
        count += 1;
    }
    if count != batch.phases.len() + batch.frontend.len() {
        return Err(Error::StaleOwner);
    }
    for (target, binding) in check::expected_phases(batch) {
        let phase = state
            .phases
            .get(&target.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        super::super::measure(proof, &(&phase.snapshot, &phase.token, &phase.output))?;
        if crate::snapshot::encoded_bytes(&(&phase.snapshot, &phase.token, &phase.output), 4 << 20)
            .is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        check::phase(run, phase, target, binding, true)?;
    }
    for frozen in &batch.phases {
        let old = state
            .phases
            .get(&frozen.previous.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        super::super::measure(proof, &(&old.snapshot, &old.token, &old.output))?;
        if crate::snapshot::encoded_bytes(&(&old.snapshot, &old.token, &old.output), 4 << 20)
            .is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        check::phase(run, old, &frozen.previous, &frozen.call_binding, false)?;
    }
    for phase in state
        .phases
        .values()
        .filter(|v| v.snapshot.run_id == run.id)
    {
        super::super::measure(proof, &phase.snapshot)?;
        if let Some(frozen) = batch
            .calls
            .iter()
            .find(|v| v.target.call_id == phase.snapshot.target.call_id)
            && phase.snapshot.target.generation > frozen.target.generation
        {
            return Err(Error::StaleOwner);
        }
    }
    Ok(())
}
