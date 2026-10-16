//! Immutable frontend origin is established while the owned model attempt lives.
use super::{Error, frontend_descriptor_fingerprint};
use crate::{
    AttemptTarget,
    tool_continuation::canonical::{digest, identity},
};
use crabber_core::{
    ControlId, EventCursor, EventKind, EventRecord, FrontendTool, InputFingerprint, MessageId,
    RunId, SessionId, SessionRevision, ToolCallId, ToolCallRecord, ToolCallStatus, TurnId,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendCallStaging {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub parent: AttemptTarget,
    pub call: ToolCallRecord,
    pub descriptor: FrontendTool,
    pub descriptor_fingerprint: InputFingerprint,
    pub message_id: MessageId,
    pub source_turn_id: Option<TurnId>,
    pub created_at: OffsetDateTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendStagingReceipt {
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
}
impl FrontendCallStaging {
    /// # Errors
    /// Rejects malformed origin, invalid input schema/arguments and oversized data.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        for (text, bound) in [
            (&self.id.0, 128),
            (&self.session_id.0, 512),
            (&self.run_id.0, 128),
            (&self.parent.id.0, 128),
            (&self.call.id.0, 512),
            (&self.message_id.0, 512),
        ] {
            identity(text, bound)?;
        }
        if let Some(turn) = &self.source_turn_id {
            identity(&turn.0, 512)?;
        }
        if self.call.run_id != self.run_id
            || self.call.name != self.descriptor.name
            || self.call.status != ToolCallStatus::Pending
            || self.call.result.is_some()
            || self.call.retry_safe
            || self.descriptor_fingerprint != frontend_descriptor_fingerprint(&self.descriptor)?
        {
            return Err(Error::InvalidRequest);
        }
        crabber_core::AcceptedToolResponse::validate_value(&self.call.arguments)
            .map_err(|_| Error::InvalidRequest)?;
        crate::ensure_storable(&self.call).map_err(|_| Error::InvalidRequest)?;
        jsonschema::validator_for(&self.descriptor.input_parameters)
            .map_err(|_| Error::InvalidRequest)?
            .validate(&self.call.arguments)
            .map_err(|_| Error::InvalidRequest)?;
        digest(&("crabber.frontend-tool.stage.v1", self))
    }
    /// Build the original protected staging event for post-commit notification.
    /// # Errors
    /// Rejects an invalid frozen descriptor, original call or scope.
    pub fn materialize(&self) -> Result<EventRecord, Error> {
        self.event(&self.fingerprint()?, None)
    }
    pub(crate) fn event(
        &self,
        digest: &InputFingerprint,
        cursor: Option<EventCursor>,
    ) -> Result<EventRecord, Error> {
        let mut payload = serde_json::json!({"call_id":self.call.id,"name":self.call.name,"status":"pending","message_id":self.message_id,"frontend_origin":{"version":1,"operation_id":self.id,"semantic_digest":digest,"captured_revision":self.expected_revision,"parent":self.parent,"descriptor_fingerprint":self.descriptor_fingerprint,"call_binding":crate::phase::call_binding(&self.call).map_err(|_|Error::InvalidRequest)?}});
        payload.sort_all_objects();
        Ok(EventRecord {
            cursor,
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            turn_id: self.source_turn_id.clone(),
            kind: EventKind::ToolCallPending,
            payload,
            correlation: Some(self.call.id.0.clone()),
            live_only: false,
            created_at: self.created_at,
        })
    }
}
impl std::fmt::Debug for FrontendCallStaging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrontendCallStaging([redacted])")
    }
}
