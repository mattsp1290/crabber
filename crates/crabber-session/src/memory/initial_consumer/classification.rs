//! Historical source classification on the caller's original lock and allowance.
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::{Status, initial_consumer::InitialConsumption};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::Run;

pub(in crate::memory) fn classify_run(
    state: &State,
    run: &Run,
    proof: &mut Proof,
) -> Result<InitialConsumption, Failure> {
    let result = read(state, run, proof);
    // Legacy native Ledger readers preserve their domain errors, sometimes
    // collapsing a nested budget failure. The SAME sticky allowance restores
    // that failure here; this neither resets nor discounts any carrier charge.
    proof.charge(0)?;
    result
}
fn read(state: &State, run: &Run, proof: &mut Proof) -> Result<InitialConsumption, Failure> {
    if !proof.active() {
        return Err(unknown("Initial classification requires bounded Proof"));
    }
    proof.measure(run)?;
    if state.runs.get(&run.id) != Some(run) {
        return Err(unknown("Initial classification current Run"));
    }
    crate::memory::pending_initial::routes(state, &run.session_id, proof)?;
    let assignment = state
        .pending_initial
        .assignment_for_run(&run.session_id, &run.id, proof)?;
    let status =
        crate::memory::native_enrollment::original(state, &run.session_id, &run.id, proof)?;
    let current = crate::memory::native_parent::source_protocol::retained_current(
        state,
        &run.session_id,
        &run.id,
        proof,
    )?;
    if let Some(current) = current {
        current.verify(run).map_err(unknown)?;
    }
    match status {
        Status::Legacy | Status::Unclassified => {
            if assignment.is_some() {
                return Err(unknown("Pending assignment without Pending native origin"));
            }
            Ok(InitialConsumption::Unproven)
        }
        Status::Pending => {
            let assignment =
                assignment.ok_or_else(|| unknown("Pending Initial missing actual assignment"))?;
            if current.is_none() {
                return Err(unknown("Pending Initial missing current native source"));
            }
            MemoryStore::classify_initial_pending(state, run, assignment, proof)
        }
    }
}
