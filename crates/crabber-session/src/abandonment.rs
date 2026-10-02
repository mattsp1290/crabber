//! Shared durable evidence and settlement artifacts for store implementations.
use crate::StoreError;
use crabber_core::{
    AbandonError, AbandonOutcome, AbandonRequest, ContentBlock, EventKind, EventRecord, Message,
    MessageId, Part, PartId, PartKind, Role, Run, ToolCallId, ToolCallRecord, ToolResult,
    ToolResultStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;

#[derive(Serialize, Deserialize)]
pub(crate) struct AbandonEvidence {
    pub request: AbandonRequest,
    pub run: Run,
    pub interrupted_tools: Vec<ToolCallId>,
}

fn event(run: &Run, kind: EventKind, now: OffsetDateTime) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: now,
    }
}

pub(crate) fn interrupted_tool(
    run: &Run,
    call: &ToolCallRecord,
    now: OffsetDateTime,
) -> (ToolResult, Message, EventRecord) {
    let content = vec![ContentBlock::Text {
        text: "interrupted".into(),
    }];
    let result = ToolResult {
        status: ToolResultStatus::Interrupted,
        content: content.clone(),
    };
    let message_id = MessageId::new();
    let message = Message {
        id: message_id.clone(),
        session_id: run.session_id.clone(),
        run_id: Some(run.id.clone()),
        role: Role::Tool,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id,
            ordinal: 0,
            kind: PartKind::FunctionToolResult,
            content: ContentBlock::ToolResult {
                call_id: call.id.clone(),
                content,
                is_error: true,
            },
        }],
        created_at: now,
    };
    let mut settled = event(run, EventKind::ToolCallSettled, now);
    settled.payload =
        json!({"call_id": call.id, "name": call.name, "status": "interrupted", "is_error": true});
    settled.correlation = Some(call.id.to_string());
    (result, message, settled)
}

pub(crate) fn terminal_event(
    evidence: &AbandonEvidence,
    now: OffsetDateTime,
) -> Result<EventRecord, StoreError> {
    let mut settled = event(&evidence.run, EventKind::RunSettled, now);
    let value = serde_json::to_value(evidence)
        .map_err(|_| StoreError::Validation("invalid abandonment evidence".into()))?;
    settled.payload =
        json!({"status": "interrupted", "usage": evidence.run.usage, "abandonment_v1": value});
    Ok(settled)
}

/// Replay authority lives in private store state, never in caller-writable events.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct AbandonCommit {
    pub request: AbandonRequest,
    pub outcome: AbandonOutcome,
}

impl AbandonCommit {
    pub fn replay(
        &self,
        run: &Run,
        request: &AbandonRequest,
    ) -> Result<AbandonOutcome, AbandonError> {
        if self.outcome.run != *run
            || !matches!(run.status, crabber_core::RunStatus::Interrupted)
            || self.outcome.terminal_event.kind != EventKind::RunSettled
            || self.outcome.terminal_event.run_id != run.id
            || self.outcome.terminal_event.cursor.is_none()
            || self.request.expected.claim_token == run.claim_token
        {
            return Err(StoreError::Validation("invalid abandonment commit".into()).into());
        }
        if &self.request != request {
            return Err(AbandonError::StaleOwner);
        }
        Ok(self.outcome.clone())
    }
}
