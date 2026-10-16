//! Presentation records copied from closed history. These are never native calls.
use crate::{MessageId, PartId, RunId, SessionId, SessionRevision, ToolCallId, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable correspondence for a copied message and its surviving public parts.
/// The source Run identifies provenance only; the target message has no Run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportedMessageProvenance {
    pub session_id: SessionId,
    pub message_id: MessageId,
    pub source_session_id: SessionId,
    pub source_revision: SessionRevision,
    pub source_message_id: MessageId,
    pub source_run_id: Option<RunId>,
    /// Ordered (target, source) identities; private parts are not imported.
    pub parts: Vec<(PartId, PartId)>,
}

/// A closed call/result relationship for history display and provider context.
/// It has no target Run, execution status, retry flag, attempt or phase. Native
/// unfinished-call queries must never enumerate these records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportedToolCall {
    pub session_id: SessionId,
    pub id: ToolCallId,
    pub assistant_message_id: MessageId,
    pub result_message_id: MessageId,
    pub name: String,
    pub arguments: Value,
    pub result: ToolResult,
    pub source_session_id: SessionId,
    pub source_revision: SessionRevision,
    pub source_call_id: ToolCallId,
    pub source_run_id: Option<RunId>,
}
