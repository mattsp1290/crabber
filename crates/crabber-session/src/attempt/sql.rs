//! Bounded SQL row codecs shared by the bundled adapters.
use super::{
    AttemptError as Error, AttemptReceipt, AttemptRecord, AttemptSnapshot, verify_receipt,
};
use crabber_core::{ControlId, EventRecord, SessionId, SessionRevision};
use serde::Deserialize;
use time::OffsetDateTime;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    snapshot: AttemptSnapshot,
    owner: String,
    token: String,
    run_token: String,
    lease_until: i64,
}
pub(crate) fn decode_record(data: Option<String>) -> Result<AttemptRecord, Error> {
    let wire: Record = serde_json::from_str(&data.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if wire.snapshot.session_id.0.is_empty()
        || wire.snapshot.session_id.0.len() > 512
        || wire.snapshot.session_id.0.chars().any(char::is_control)
        || wire.snapshot.run_id.0.is_empty()
        || wire.snapshot.run_id.0.len() > 128
        || wire.snapshot.run_id.0.chars().any(char::is_control)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    super::validate_identifier(&wire.snapshot.target.id.0)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let record = AttemptRecord {
        snapshot: wire.snapshot,
        owner: wire.owner,
        token: wire.token,
        run_token: wire.run_token,
        lease_until: OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(wire.lease_until) * 1_000,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?,
    };
    record.validate()?;
    Ok(record)
}
pub(crate) fn revision(value: i64) -> Result<SessionRevision, Error> {
    SessionRevision::new(u64::try_from(value).map_err(|_| Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)
}
pub(crate) fn receipt(
    session: &SessionId,
    id: &ControlId,
    digest: &str,
    seq: i64,
    data: Option<String>,
    event: &EventRecord,
) -> Result<AttemptReceipt, Error> {
    let receipt: AttemptReceipt = serde_json::from_str(&data.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if seq <= 0
        || receipt.attempt.session_id != *session
        || receipt.operation_id != *id
        || receipt.semantic_digest.as_str() != digest
        || u64::try_from(seq).ok() != Some(receipt.event_cursor.0)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    verify_receipt(&receipt, event)?;
    Ok(receipt)
}
pub(crate) fn unknown(_: crate::StoreError) -> Error {
    Error::UnknownStoreOutcome
}
pub(crate) fn enum_text(value: impl serde::Serialize) -> Result<String, Error> {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or(Error::UnknownStoreOutcome)
}
