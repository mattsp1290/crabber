use super::{Error, State};
use crate::approval::ApprovalRecord;
use crabber_core::Run;

pub(in super::super) fn verify(
    state: &State,
    run: &Run,
    record: &ApprovalRecord,
) -> Result<(), Error> {
    let suspension = super::receipt(state, &record.session_id, &record.batch.id)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let key = crate::approval::derived_id("suspend-attempt", &record.batch.id);
    let Some(super::OperationReceipt::Attempt(native)) = state
        .operation_receipts
        .get(&(record.session_id.clone(), key))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
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
    record.verify_origin(run, &suspension, audit, native, native_audit, source)
}
