//! Borrow and charge current call/phase carriers before validation or copying.
use super::{Error, Proof, State, bounded};
use crate::session_owner::driver::successor::tool_fresh as validate;
use crate::session_owner::preparation::pending_tool as choice;
use crabber_core::Run;
pub(super) fn check(
    state: &State,
    run: &Run,
    record: &crate::tool_continuation::ToolContinuationRecord,
    recovered: bool,
    proof: &mut Proof,
) -> Result<(), Error> {
    let mut count = 0;
    for (id, call) in state.calls.iter().filter(|(_, v)| {
        v.run_id == run.id
            && matches!(
                v.status,
                crabber_core::ToolCallStatus::Waiting
                    | crabber_core::ToolCallStatus::Pending
                    | crabber_core::ToolCallStatus::Running
            )
    }) {
        bounded(proof, &(id, &call.id, &call.run_id, call.status), 2048)?;
        if id != &call.id {
            return Err(Error::UnknownStoreOutcome);
        }
        count += 1;
        if count > 64 {
            return Err(Error::Capacity);
        }
    }
    if count != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    for frozen in &record.batch.calls {
        let call = state
            .calls
            .get(&frozen.target.call_id)
            .ok_or(Error::NotFound)?;
        bounded(proof, call, 4 << 20)?;
        if call.id != frozen.target.call_id {
            return Err(Error::UnknownStoreOutcome);
        }
        choice::call(record, call, recovered)?;
        let expected = if recovered {
            validate::expected_phases(&record.batch)
                .find(|(t, _)| t.call_id == call.id)
                .map(|(t, b)| (t, b, true))
        } else {
            record
                .batch
                .phases
                .iter()
                .find(|v| v.previous.call_id == call.id)
                .map(|v| (&v.previous, &v.call_binding, false))
        };
        let mut seen = 0;
        for (id, phase) in state
            .phases
            .iter()
            .filter(|(_, v)| v.snapshot.run_id == run.id && v.snapshot.target.call_id == call.id)
        {
            bounded(proof, &(id, &phase.snapshot.target), 4096)?;
            if id != &phase.snapshot.target.id {
                return Err(Error::UnknownStoreOutcome);
            }
            if let Some((target, binding, current)) = expected {
                if phase.snapshot.target.generation > target.generation {
                    return Err(Error::Busy);
                }
                if phase.snapshot.target.id == target.id {
                    bounded(
                        proof,
                        &(&phase.snapshot, &phase.token, &phase.output),
                        4 << 20,
                    )?;
                    validate::phase(run, phase, target, binding, current)?;
                    seen += 1;
                }
            } else {
                return Err(Error::Busy);
            }
        }
        if seen != usize::from(expected.is_some()) {
            return Err(Error::NotFound);
        }
    }
    originals_and_set(state, run, record, recovered, proof)
}
fn originals_and_set(
    state: &State,
    run: &Run,
    record: &crate::tool_continuation::ToolContinuationRecord,
    recovered: bool,
    proof: &mut Proof,
) -> Result<(), Error> {
    for frozen in &record.batch.phases {
        let phase = state
            .phases
            .get(&frozen.previous.id)
            .ok_or(Error::NotFound)?;
        bounded(
            proof,
            &(&phase.snapshot, &phase.token, &phase.output),
            4 << 20,
        )?;
        validate::phase(run, phase, &frozen.previous, &frozen.call_binding, false)?;
    }
    let mut count = 0;
    for (id, phase) in state.phases.iter().filter(|(_, v)| {
        v.snapshot.run_id == run.id && v.snapshot.target.attempt == record.batch.next_attempt
    }) {
        bounded(proof, &(id, &phase.snapshot.target), 4096)?;
        if id != &phase.snapshot.target.id {
            return Err(Error::UnknownStoreOutcome);
        }
        count += 1;
    }
    let expected = if recovered {
        record.batch.phases.len() + record.batch.frontend.len()
    } else {
        0
    };
    if count != expected {
        return Err(Error::Busy);
    }
    Ok(())
}
