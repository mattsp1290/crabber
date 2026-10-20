//! Bounded private record and receipt decoding shared by SQL adapters.
use super::{ApprovalError as Error, ApprovalReceipt, ApprovalRecord, ContinuationTarget};
use crabber_core::{ControlId, EventCursor, EventRecord, RunId, SessionId};

pub(crate) const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;

pub(crate) fn decode_record(
    data: Option<String>,
    session: &SessionId,
    run: &RunId,
    target: &ContinuationTarget,
    state: &str,
    version: i64,
) -> Result<ApprovalRecord, Error> {
    let data = data.ok_or(Error::UnknownStoreOutcome)?;
    if data.len() > MAX_RECORD_BYTES || version != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    let record: ApprovalRecord =
        serde_json::from_str(&data).map_err(|_| Error::UnknownStoreOutcome)?;
    record.validate(session, run)?;
    if record.batch.continuation != *target || enum_text(record.state)? != state {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(record)
}
pub(crate) fn decode_receipt(
    data: Option<String>,
    session: &SessionId,
    id: &ControlId,
    digest: Option<&str>,
    seq: i64,
    event: &mut EventRecord,
) -> Result<ApprovalReceipt, Error> {
    let data = data.ok_or(Error::UnknownStoreOutcome)?;
    if data.len() > 32768 || seq <= 0 {
        return Err(Error::UnknownStoreOutcome);
    }
    let receipt: ApprovalReceipt =
        serde_json::from_str(&data).map_err(|_| Error::UnknownStoreOutcome)?;
    let cursor = EventCursor(u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?);
    if receipt.session_id != *session
        || receipt.operation_id != *id
        || digest != Some(receipt.semantic_digest.as_str())
        || receipt.event_cursor != cursor
        || event.cursor.is_some_and(|saved| saved != cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(cursor);
    super::verify_receipt(&receipt, event)?;
    Ok(receipt)
}
pub(crate) fn unknown(_: crate::StoreError) -> Error {
    Error::UnknownStoreOutcome
}
pub(crate) fn enum_text(value: impl serde::Serialize) -> Result<String, Error> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(Error::UnknownStoreOutcome)
}

pub(crate) fn before_commit(fault: u8) -> Result<(), Error> {
    if fault == 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn after_commit<T>(result: Result<(T, bool), Error>, fault: u8) -> Result<T, Error> {
    let (value, changed) = result?;
    if changed && fault == 2 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(value)
}
