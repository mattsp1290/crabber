//! Typed public data and native-selected attribution. Values are not authority.
mod patch;
use crate::{
    AttemptGeneration, AttemptId, CoreError, InvocationId, MAX_PUBLIC_STATE_BYTES, PublicActivity,
    PublicStateContents, RunId, SessionId, SessionOwnerGeneration, SessionRevision, ToolCallId,
    input,
};
pub use patch::{PublicPatchOperation, apply_public_patch};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicAttemptIdentity {
    pub id: AttemptId,
    pub generation: AttemptGeneration,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicInvocationIdentity {
    pub id: InvocationId,
    pub generation: SessionOwnerGeneration,
    pub parent_invocation: Option<InvocationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_generation: Option<SessionOwnerGeneration>,
    pub parent_call_id: Option<ToolCallId>,
}
/// Selected by the trusted native writer, never accepted from a tool payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicSourceIdentity {
    pub workspace_id: String,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub attempt: PublicAttemptIdentity,
    pub call_id: Option<ToolCallId>,
    pub invocation: Option<PublicInvocationIdentity>,
}
/// Explicit public presentation. Provider signatures, reasoning and private state
/// cannot enter through this text-only snapshot carrier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicSourceMessage {
    pub message_id: crate::MessageId,
    pub role: crate::Role,
    pub text: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicPresentationChange {
    MessageStarted {
        message_id: crate::MessageId,
        role: crate::Role,
    },
    TextDelta {
        message_id: crate::MessageId,
        text: String,
    },
    MessageEnded {
        message_id: crate::MessageId,
    },
    MessagesSnapshot {
        messages: Vec<PublicSourceMessage>,
    },
    ToolProgress {
        content: crate::ContentBlock,
    },
    Raw {
        source: String,
        event: Value,
    },
    Custom {
        name: String,
        value: Value,
    },
}
impl PublicPresentationChange {
    fn validate(&self) -> Result<(), CoreError> {
        match self {
            Self::MessageStarted { message_id, .. }
            | Self::TextDelta { message_id, .. }
            | Self::MessageEnded { message_id } => identifier(&message_id.0),
            Self::MessagesSnapshot { messages } => {
                if messages.len() > 256 {
                    return Err(invalid());
                }
                let mut ids = std::collections::BTreeSet::new();
                for message in messages {
                    identifier(&message.message_id.0)?;
                    if !ids.insert(&message.message_id) {
                        return Err(invalid());
                    }
                }
                Ok(())
            }
            Self::Raw { source, event } => {
                identifier(source)?;
                input::validate_json(event, 0).map_err(|_| invalid())
            }
            Self::Custom { name, value } => {
                identifier(name)?;
                input::validate_json(value, 0).map_err(|_| invalid())
            }
            Self::ToolProgress { content } => match content {
                crate::ContentBlock::Text { .. } => Ok(()),
                _ => Err(invalid()),
            },
        }
    }
}
fn identifier(value: &str) -> Result<(), CoreError> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        Err(invalid())
    } else {
        Ok(())
    }
}
/// Payload-only operations. A new activity snapshot with the same ID explicitly
/// replaces its type and content; a delta cannot silently change that type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicSourceChange {
    Presentation {
        presentation: PublicPresentationChange,
    },
    StateSnapshot {
        state: Value,
    },
    StateDelta {
        delta: Vec<PublicPatchOperation>,
    },
    ActivitySnapshot {
        activity: PublicActivity,
    },
    ActivityDelta {
        activity_id: String,
        activity_type: String,
        delta: Vec<PublicPatchOperation>,
    },
}
impl PublicSourceChange {
    /// Bound the complete borrowed request before copying its payload or state.
    /// # Errors
    /// Rejects excessive patches, invalid activity identities and nonobject activity.
    pub fn validate(&self) -> Result<(), CoreError> {
        input::validate_serialized_limit(self, MAX_PUBLIC_STATE_BYTES).map_err(|_| invalid())?;
        match self {
            Self::Presentation { presentation } => presentation.validate(),
            Self::StateSnapshot { state } => input::validate_json(state, 0).map_err(|_| invalid()),
            Self::StateDelta { delta } => validate_delta(delta),
            Self::ActivitySnapshot { activity } => {
                if activity.content().is_object() {
                    Ok(())
                } else {
                    Err(invalid())
                }
            }
            Self::ActivityDelta {
                activity_id,
                activity_type,
                delta,
            } => {
                if [activity_id, activity_type]
                    .iter()
                    .any(|id| id.is_empty() || id.len() > 128 || id.chars().any(char::is_control))
                {
                    return Err(invalid());
                }
                validate_delta(delta)
            }
        }
    }
    /// Apply to a private working value before publishing state, event and revision.
    /// # Errors
    /// Rejects invalid patches, activity type drift and bounded-value violations.
    pub fn apply(&self, current: &PublicStateContents) -> Result<PublicStateContents, CoreError> {
        self.validate()?;
        let mut state = current.state().clone();
        let mut activities = current.activities().to_vec();
        match self {
            Self::Presentation { .. } => {}
            Self::StateSnapshot { state: replacement } => state.clone_from(replacement),
            Self::StateDelta { delta } => state = apply_public_patch(&state, delta)?,
            Self::ActivitySnapshot { activity } => {
                if let Some(old) = activities.iter_mut().find(|old| old.id() == activity.id()) {
                    *old = activity.clone();
                } else {
                    activities.push(activity.clone());
                }
            }
            Self::ActivityDelta {
                activity_id,
                activity_type,
                delta,
            } => {
                let old = activities
                    .iter_mut()
                    .find(|old| old.id() == activity_id)
                    .ok_or_else(invalid)?;
                if old.activity_type() != activity_type {
                    return Err(invalid());
                }
                let content = apply_public_patch(old.content(), delta)?;
                if !content.is_object() {
                    return Err(invalid());
                }
                *old = PublicActivity::new(activity_id, activity_type, content)?;
            }
        }
        PublicStateContents::new(state, activities)
    }
}
fn validate_delta(delta: &[PublicPatchOperation]) -> Result<(), CoreError> {
    if delta.len() > 256 {
        Err(invalid())
    } else {
        Ok(())
    }
}
/// Version 2 distinguishes trusted source mutations from legacy state snapshots.
/// `contents` is the actual committed result, including required whole JSON null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicSourceEvent {
    pub version: u32,
    pub source: PublicSourceIdentity,
    pub revision: SessionRevision,
    pub change: PublicSourceChange,
    pub contents: PublicStateContents,
    pub before: Option<PublicStateContents>,
    pub before_event_cursor: Option<crate::EventCursor>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicInvocationStatus {
    Started,
    Finished,
    Error,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicInvocationEvent {
    pub version: u32,
    pub source: PublicSourceIdentity,
    pub revision: SessionRevision,
    pub status: PublicInvocationStatus,
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid public source mutation".into())
}

#[cfg(test)]
mod tests;
