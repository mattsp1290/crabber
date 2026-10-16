//! Private frontend evidence remains anchored to the immutable staging source.
use super::{Error, FrontendCallStaging, FrontendStagingReceipt};
use crabber_core::{EventCursor, EventKind, EventRecord, SessionRevision};
use serde::{Deserialize, Serialize};
pub(crate) fn verify_declaration(
    request: &FrontendCallStaging,
    input: &crabber_core::InputEnvelope,
) -> Result<(), Error> {
    if input
        .frontend_tools()
        .iter()
        .filter(|tool| tool.name == request.descriptor.name)
        .collect::<Vec<_>>()
        != [&request.descriptor]
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
pub(crate) fn verify_assistant_source(
    request: &FrontendCallStaging,
    message: &crabber_core::Message,
) -> Result<(), Error> {
    if message.id != request.message_id || message.session_id != request.session_id
        || message.run_id.as_ref() != Some(&request.run_id) || message.role != crabber_core::Role::Assistant
        || message.parts.iter().filter(|part|matches!(&part.content,crabber_core::ContentBlock::ToolCall {call_id,name,arguments} if call_id==&request.call.id && name==&request.call.name && arguments==&request.call.arguments)).count()!=1
    { return Err(Error::SemanticConflict); }
    Ok(())
}
pub(crate) fn staging_receipt(
    request: &FrontendCallStaging,
    event_cursor: EventCursor,
) -> Result<FrontendStagingReceipt, Error> {
    Ok(FrontendStagingReceipt {
        operation_id: request.id.clone(),
        semantic_digest: request.fingerprint()?,
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        call_id: request.call.id.clone(),
        revision: request
            .expected_revision
            .next()
            .map_err(|_| Error::UnknownStoreOutcome)?,
        event_cursor,
    })
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FrontendOriginRecord {
    pub request: FrontendCallStaging,
    pub receipt: FrontendStagingReceipt,
}
impl FrontendOriginRecord {
    pub(crate) fn verify(&self, event: &EventRecord) -> Result<(), Error> {
        let digest = self
            .request
            .fingerprint()
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let mut expected = self.request.materialize()?;
        expected.cursor = Some(self.receipt.event_cursor);
        if self.receipt.operation_id != self.request.id
            || self.receipt.semantic_digest != digest
            || self.receipt.session_id != self.request.session_id
            || self.receipt.run_id != self.request.run_id
            || self.receipt.call_id != self.request.call.id
            || self.receipt.revision
                != self
                    .request
                    .expected_revision
                    .next()
                    .map_err(|_| Error::UnknownStoreOutcome)?
            || *event != expected
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
pub(crate) fn verify_receipt(
    receipt: &FrontendStagingReceipt,
    event: &EventRecord,
) -> Result<(), Error> {
    let source = &event.payload["frontend_origin"];
    let captured: SessionRevision = serde_json::from_value(source["captured_revision"].clone())
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if event.kind != EventKind::ToolCallPending
        || event.live_only
        || event.session_id != receipt.session_id
        || event.run_id != receipt.run_id
        || event.cursor != Some(receipt.event_cursor)
        || event.correlation.as_deref() != Some(receipt.call_id.0.as_str())
        || event.payload["call_id"] != serde_json::json!(receipt.call_id)
        || event.payload["status"] != "pending"
        || source["version"] != 1
        || source["operation_id"] != serde_json::json!(receipt.operation_id)
        || source["semantic_digest"] != serde_json::json!(receipt.semantic_digest)
        || captured.next().map_err(|_| Error::UnknownStoreOutcome)? != receipt.revision
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
