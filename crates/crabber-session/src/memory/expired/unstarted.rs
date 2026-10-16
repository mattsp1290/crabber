//! A genuine initial `ReserveBound` remains pending; no cleanup is fabricated.
use super::{Error, State, unknown};
use crate::session_owner::preparation::{ExpiredRunOrigin, expired::unstarted::Original};
use crabber_core::{InputFingerprint, Run};
#[cfg(test)]
#[path = "unstarted/tests.rs"]
mod tests;
pub(super) fn guard(
    state: &State,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
    now: time::OffsetDateTime,
    initial: (InputFingerprint, ExpiredRunOrigin),
) -> Result<(InputFingerprint, ExpiredRunOrigin), Error> {
    let mut records = state
        .attempts
        .values()
        .filter(|record| record.snapshot.run_id == run.id);
    let Some(record) = records.next() else {
        super::super::attempt::pristine_for_driver(state, run, binding)
            .map_err(|_| Error::Unsupported)?;
        return Ok(initial);
    };
    if records.next().is_some()
        || state
            .native_parent_witnesses
            .values()
            .any(|witness| witness.source.run_id == run.id)
        || state.calls.values().any(|call| call.run_id == run.id)
        || state
            .phases
            .values()
            .any(|phase| phase.snapshot.run_id == run.id)
        || state
            .epochs
            .values()
            .any(|epoch| epoch.run_id == run.id && epoch.id != run.epoch_id)
    {
        return Err(Error::Unsupported);
    }
    let count = crate::attempt::pristine::input_count(run, binding).map_err(unknown)?;
    if state
        .messages
        .iter()
        .filter(|message| message.run_id.as_ref() == Some(&run.id))
        .take(count + 1)
        .count()
        != count
    {
        return Err(Error::Unsupported);
    }
    let mut events = state.events.iter().filter(|event| event.run_id == run.id);
    let event = events.next().ok_or(Error::UnknownStoreOutcome)?;
    if events.next().is_some() || crate::snapshot::encoded_bytes(event, 65536).is_none() {
        return Err(Error::Unsupported);
    }
    let id = crabber_core::ControlId(
        event
            .correlation
            .clone()
            .ok_or(Error::UnknownStoreOutcome)?,
    );
    crate::attempt::validate_identifier(&id.0).map_err(unknown)?;
    let original = read(state, &run.session_id, &id)?;
    original.verify_record(run, record, now)?;
    Ok((original.digest(&initial.0)?, original.origin(initial.1)?))
}
pub(in crate::memory) fn read(
    state: &State,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
) -> Result<Original, Error> {
    let Some(super::super::OperationReceipt::Attempt(receipt)) =
        state.operation_receipts.get(&(session.clone(), id.clone()))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    if receipt.operation_id != *id
        || receipt.attempt.session_id != *session
        || crate::snapshot::encoded_bytes(receipt, 8192).is_none()
        || crate::snapshot::encoded_bytes(event, 65536).is_none()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let original = Original {
        receipt: receipt.clone(),
        event: event.clone(),
    };
    original.verify()?;
    Ok(original)
}
