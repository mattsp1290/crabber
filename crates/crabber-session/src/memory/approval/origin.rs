use super::{Error, State};
use crate::approval::ApprovalRecord;
use crate::public_session::v2::proof::{ApprovalReadError, Proof};
use crabber_core::Run;

pub(in super::super) fn verify(
    state: &State,
    run: &Run,
    record: &ApprovalRecord,
) -> Result<(), Error> {
    verify_with_proof(state, run, record, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) fn verify_with_proof(
    state: &State,
    run: &Run,
    record: &ApprovalRecord,
    proof: &mut Proof,
) -> Result<(), ApprovalReadError> {
    let suspension = super::receipt_with_proof(state, &record.session_id, &record.batch.id, proof)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let key = crate::approval::derived_id("suspend-attempt", &record.batch.id);
    let Some(super::OperationReceipt::Attempt(native)) = state
        .operation_receipts
        .get(&(record.session_id.clone(), key))
    else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    proof.measure(native).map_err(ApprovalReadError::Budget)?;
    let event = |cursor: crabber_core::EventCursor| {
        state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)
    };
    let audit = event(suspension.event_cursor)?;
    let native_audit = event(native.event_cursor)?;
    let source = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == record.session_id
                && event
                    .cursor
                    .is_some_and(|cursor| cursor < native.event_cursor)
        })
        .ok_or(Error::UnknownStoreOutcome)?;
    proof.measure(audit).map_err(ApprovalReadError::Budget)?;
    proof
        .measure(native_audit)
        .map_err(ApprovalReadError::Budget)?;
    proof.measure(source).map_err(ApprovalReadError::Budget)?;
    proof
        .measure(&(&run.session_id, &run.id, run.created_at))
        .map_err(ApprovalReadError::Budget)?;
    state
        .continuation_inputs
        .verify(
            crate::continuation_input::Input::ApprovalSuspension(&record.batch),
            crate::continuation_input::Receipt::Approval(&suspension),
            audit,
            proof,
        )
        .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?;
    Ok(record.verify_origin(run, &suspension, audit, native, native_audit, source)?)
}
