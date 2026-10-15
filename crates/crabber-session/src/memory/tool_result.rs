//! Canonical logical result publication, independent of native phase ownership.
use super::State;
use crate::PhaseError as Error;
use crabber_core::Run;

pub(super) fn publish_result(
    state: &mut State,
    run: &Run,
    call_id: &crabber_core::ToolCallId,
    records: crate::PhaseSettlementRecords,
) -> Result<(), Error> {
    super::insert_message(state, run, records.message).map_err(|_| Error::UnknownStoreOutcome)?;
    super::insert_event(state, run, records.event).map_err(|_| Error::UnknownStoreOutcome)?;
    let call = state.calls.get_mut(call_id).ok_or(Error::NotFound)?;
    call.status = records.call_status;
    call.result = Some(records.result);
    let snapshot_revision = state
        .snapshot_revisions
        .entry(run.session_id.clone())
        .or_default();
    *snapshot_revision = snapshot_revision
        .checked_add(1)
        .ok_or(Error::RevisionExhausted)?;
    Ok(())
}
