//! Bounded native phase row codecs shared by the SQL adapters.
use super::{PhaseError as Error, PhaseReceipt, PhaseRecord, PhaseSnapshot, verify_receipt};
use crabber_core::{
    ControlId, EventRecord, InputFingerprint, SessionId, SessionRevision, ToolOutput,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    version: u32,
    snapshot: PhaseSnapshot,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    snapshot: PhaseSnapshot,
    token: String,
    output: Option<ToolOutput>,
}
#[derive(Serialize)]
struct Envelope<'a> {
    version: u32,
    call_binding: &'a InputFingerprint,
    output: &'a Option<ToolOutput>,
}

pub(crate) fn decode_metadata(data: Option<String>) -> Result<PhaseSnapshot, Error> {
    let wire: Metadata = serde_json::from_str(&data.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if wire.version != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    super::validate_snapshot(&wire.snapshot)?;
    Ok(wire.snapshot)
}
pub(crate) fn decode_record(data: Option<String>) -> Result<PhaseRecord, Error> {
    let wire: Record = serde_json::from_str(&data.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if wire.version != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    let record = PhaseRecord {
        snapshot: wire.snapshot,
        token: wire.token,
        output: wire.output,
    };
    record.validate()?;
    Ok(record)
}
pub(crate) fn encode_outcome(record: &PhaseRecord) -> Result<String, Error> {
    record.validate()?;
    serde_json::to_string(&Envelope {
        version: 1,
        call_binding: &record.snapshot.call_binding,
        output: &record.output,
    })
    .map_err(|_| Error::UnknownStoreOutcome)
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
) -> Result<PhaseReceipt, Error> {
    let receipt: PhaseReceipt = serde_json::from_str(&data.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if seq <= 0
        || receipt.phase.session_id != *session
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
pub(crate) fn enum_text(value: impl Serialize) -> Result<String, Error> {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or(Error::UnknownStoreOutcome)
}
