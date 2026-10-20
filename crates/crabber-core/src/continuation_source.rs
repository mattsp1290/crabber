//! Safe committed continuation presentation. These values never grant execution,
//! answer authority, or permission to reconstruct a native owner.
#[cfg(test)]
mod tests;
use crate::{
    AttemptGeneration, AttemptId, ContinuationGeneration, ContinuationId, ControlId, CoreError,
    EventCursor, InputFingerprint, InteractionId, InteractionKind, MessageId, PublicStateContents,
    RunId, SessionId, SessionRevision, ToolCallId, TurnId, input,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptIdentity {
    pub id: AttemptId,
    pub generation: AttemptGeneration,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationIdentity {
    pub id: ContinuationId,
    pub generation: ContinuationGeneration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeMode {
    /// Acquired positive Unstarted work; a subsequent real start is required.
    NativeUnstarted,
    /// A fresh acknowledged attempt continues the selected persisted work.
    Continued,
}
/// Selected persisted call presentation, without its result or phase authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCall {
    pub call_id: ToolCallId,
    pub message_id: MessageId,
    pub name: String,
    pub arguments: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeSource {
    pub version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub attempt: AttemptIdentity,
    pub current_turn: Option<TurnId>,
    pub continuation: Option<ContinuationIdentity>,
    pub mode: ResumeMode,
    pub revision: SessionRevision,
    pub contents: PublicStateContents,
    pub calls: Vec<FrozenCall>,
}
/// A public prompt/schema copied only from an explicitly declared interaction.
/// Resolver credentials and nonsecret authority-policy assertions are excluded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanInterrupt {
    pub interaction_id: InteractionId,
    pub call_id: ToolCallId,
    pub kind: InteractionKind,
    pub name: String,
    pub prompt: String,
    pub response_schema: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitingSource {
    pub version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub attempt: AttemptIdentity,
    pub current_turn: Option<TurnId>,
    pub continuation: ContinuationIdentity,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub source_event_cursor: EventCursor,
    pub interrupts: Vec<HumanInterrupt>,
    pub frontend_pending: Vec<ToolCallId>,
    pub backend_pending: Vec<ToolCallId>,
}
/// One native read snapshot after an acknowledged attempt Begin. No checkpoint,
/// resolver assertion, token, private result, or message body is returned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeSnapshot {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub attempt: AttemptIdentity,
    pub revision: SessionRevision,
    pub contents: PublicStateContents,
    pub calls: Vec<FrozenCall>,
}

impl ResumeSource {
    /// Validate safe presentation shape and bounds, never native provenance.
    /// # Errors
    /// Rejects malformed identities, duplicate calls, excessive JSON or size.
    pub fn validate(&self) -> Result<(), CoreError> {
        scope(
            self.version,
            &self.session_id,
            &self.run_id,
            &self.attempt,
            self.current_turn.as_ref(),
        )?;
        if let Some(continuation) = &self.continuation {
            identity(&continuation.id.0)?;
        }
        if self.mode == ResumeMode::NativeUnstarted
            && (self.current_turn.is_some()
                || self.continuation.is_some()
                || !self.calls.is_empty())
        {
            return Err(invalid());
        }
        if self.calls.len() > 64 {
            return Err(invalid());
        }
        let mut calls = BTreeSet::new();
        for call in &self.calls {
            identity(&call.call_id.0)?;
            identity(&call.message_id.0)?;
            identity(&call.name)?;
            if !calls.insert(&call.call_id) {
                return Err(invalid());
            }
            input::validate_json(&call.arguments, 0).map_err(|_| invalid())?;
        }
        input::validate_serialized_bound(self).map_err(|_| invalid())
    }
}
impl WaitingSource {
    /// Validate exact disjoint pending identities and declared safe prompts.
    /// # Errors
    /// Rejects duplicate/foreign call references or unbounded public content.
    pub fn validate(&self) -> Result<(), CoreError> {
        scope(
            self.version,
            &self.session_id,
            &self.run_id,
            &self.attempt,
            self.current_turn.as_ref(),
        )?;
        identity(&self.continuation.id.0)?;
        identity(&self.operation_id.0)?;
        if self.source_event_cursor.0 == 0 || self.revision == SessionRevision::INITIAL {
            return Err(invalid());
        }
        if self.interrupts.len() > 64
            || self.frontend_pending.len() + self.backend_pending.len() > 64
        {
            return Err(invalid());
        }
        let mut calls = BTreeSet::new();
        for call in self.frontend_pending.iter().chain(&self.backend_pending) {
            identity(&call.0)?;
            if !calls.insert(call) {
                return Err(invalid());
            }
        }
        if calls.is_empty() {
            return Err(invalid());
        }
        let mut interactions = BTreeSet::new();
        for interrupt in &self.interrupts {
            identity(&interrupt.interaction_id.0)?;
            identity(&interrupt.name)?;
            if !interactions.insert(&interrupt.interaction_id)
                || !self.backend_pending.contains(&interrupt.call_id)
                || interrupt.prompt.len() > 8192
                || interrupt.prompt.contains('\0')
                || !matches!(interrupt.response_schema, Value::Object(_) | Value::Bool(_))
            {
                return Err(invalid());
            }
            input::validate_json(&interrupt.response_schema, 0).map_err(|_| invalid())?;
        }
        input::validate_serialized_bound(self).map_err(|_| invalid())
    }
}
fn scope(
    version: u32,
    session: &SessionId,
    run: &RunId,
    attempt: &AttemptIdentity,
    turn: Option<&TurnId>,
) -> Result<(), CoreError> {
    if version != 1 {
        return Err(invalid());
    }
    for value in [&session.0, &run.0, &attempt.id.0] {
        identity(value)?;
    }
    if let Some(turn) = turn {
        identity(&turn.0)?;
    }
    Ok(())
}
fn identity(value: &str) -> Result<(), CoreError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid continuation source".into())
}
