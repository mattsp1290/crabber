//! Original detached bridge coverage, independent of the parent Run's outcome.
use super::super::{OperationReceipt, State};
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, unknown},
};
use crate::session_owner::coverage::{CheckedOwners, detached_parent};
use crabber_core::SessionId;
#[cfg(test)]
#[path = "owners/tests.rs"]
mod tests;

pub(super) fn read(
    state: &State,
    session: &SessionId,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<CheckedOwners, Failure> {
    if !proof.active() {
        return Err(unknown("detached coverage requires bounded proof"));
    }
    state
        .session_owners
        .checked_owner_history(session, 1024, safe, proof, |record, proof| {
            if record.driver.is_some() {
                return super::super::session_owner::classify_driver_join(state, record, proof);
            }
            let Some(parent) = detached_parent(record) else {
                return Ok(false);
            };
            let witness = state
                .native_parent_witnesses
                .get(&parent.source.event_cursor)
                .ok_or_else(|| unknown("detached parent original witness"))?;
            proof.measure(witness)?;
            if witness != parent {
                return Err(unknown("detached parent witness association"));
            }
            let receipt = state
                .operation_receipts
                .get(&(
                    parent.source.session_id.clone(),
                    parent.source.operation_id.clone(),
                ))
                .ok_or_else(|| unknown("detached parent original receipt"))?;
            proof.measure(receipt)?;
            let OperationReceipt::Phase(receipt) = receipt else {
                return Err(unknown("detached parent receipt domain"));
            };
            let event = state
                .events
                .get(parent.source.event_cursor)
                .ok_or_else(|| unknown("detached parent original event"))?;
            proof.measure(event)?;
            let issuance = state
                .native_run_issuances
                .get(&(
                    session.clone(),
                    parent.source.run_id.clone(),
                    parent.run_issuance,
                ))
                .ok_or_else(|| unknown("detached parent original issuer"))?;
            proof.measure(issuance)?;
            witness
                .verify_historical_phase(receipt, event, issuance)
                .map_err(unknown)?;
            Ok(true)
        })
}
