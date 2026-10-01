//! Durable domain vocabulary shared by the Crabber workspace.

/// Name of the core crate, retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-core";

mod trace_context;
pub use trace_context::{TraceContext, TraceContextError, TraceLink};
mod abandonment;
pub use abandonment::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod admission;
pub use admission::{AdmissionKey, AdmissionOptions, AdmissionReceipt, InputFingerprint};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use time::OffsetDateTime;

macro_rules! id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);
        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(uuid::Uuid::new_v4().to_string())
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
id!(SessionId);
id!(RunId);
id!(MessageId);
id!(PartId);
id!(ToolCallId);
id!(EpochId);
id!(TurnId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub session_id: SessionId,
    pub run_id: Option<RunId>,
    pub role: Role,
    pub parent_id: Option<MessageId>,
    pub parts: Vec<Part>,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub id: PartId,
    pub message_id: MessageId,
    pub ordinal: u32,
    pub kind: PartKind,
    pub content: ContentBlock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartKind {
    UserInputText,
    AssistantText,
    Reasoning,
    FunctionToolCall,
    FunctionToolResult,
    ProviderState,
    Custom { custom_type: String },
    CustomMessage { custom_type: String },
    CompactionSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
        provider_state: Option<Value>,
    },
    Media {
        uri: String,
        mime_type: String,
    },
    ToolCall {
        call_id: ToolCallId,
        name: String,
        arguments: Value,
    },
    ToolResult {
        call_id: ToolCallId,
        content: Vec<Self>,
        is_error: bool,
    },
    ProviderState {
        codec_id: String,
        payload: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolInfo {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    pub retry_safe: bool,
    pub required_permissions: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub id: ToolCallId,
    pub run_id: RunId,
    pub name: String,
    pub arguments: Value,
    pub status: ToolCallStatus,
    pub retry_safe: bool,
    pub result: Option<ToolResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub status: ToolResultStatus,
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Paused,
    Interrupted,
    Failed,
    Completed,
}
impl RunStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Interrupted | Self::Failed | Self::Completed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub session_id: SessionId,
    pub status: RunStatus,
    pub owner: String,
    pub claim_token: String,
    pub lease_until: OffsetDateTime,
    pub epoch_id: EpochId,
    pub config_hash: String,
    pub plan_fingerprint: String,
    pub checkpoint: Option<Value>,
    pub error: Option<String>,
    pub usage: Usage,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFence {
    pub run_id: RunId,
    pub claim_token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEpoch {
    pub id: EpochId,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub parent: Option<EpochId>,
    pub summarized_range: Option<(MessageId, MessageId)>,
    pub summary_message_id: Option<MessageId>,
    pub tail_start_message_id: Option<MessageId>,
    pub provider_id: String,
    pub model_id: String,
    pub reason: String,
    pub next_policy: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventCursor(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    RunAdmitted,
    RunStarted,
    TurnStarted,
    TurnCompleted,
    MessageCommitted,
    TextDelta,
    ReasoningDelta,
    ToolCallPending,
    ToolCallRunning,
    ToolCallSettled,
    PermissionRequested,
    PermissionDecided,
    RunPaused,
    RunResumed,
    RunSettled,
    ContextEpochStarted,
    ContextEpochFinished,
    ExtensionNotice,
    Custom { name: String },
}
impl EventKind {
    #[must_use]
    pub const fn is_live_only(&self) -> bool {
        matches!(self, Self::TextDelta | Self::ReasoningDelta)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub cursor: Option<EventCursor>,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub turn_id: Option<TurnId>,
    pub kind: EventKind,
    pub payload: Value,
    pub correlation: Option<String>,
    pub live_only: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteLimits {
    pub max_input: usize,
    pub max_output: usize,
    pub max_message: usize,
    pub max_state_entries: usize,
    pub max_state_bytes: usize,
}
impl Default for ByteLimits {
    fn default() -> Self {
        Self {
            max_input: 1_048_576,
            max_output: 1_048_576,
            max_message: 1_048_576,
            max_state_entries: 1_000,
            max_state_bytes: 1_048_576,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoreError {
    #[error("invalid input: {0}")]
    Validation(String),
    #[error("limit exceeded: {0}")]
    Limit(String),
    #[error("write conflict")]
    Conflict,
    #[error("admission key conflicts with retained input")]
    AdmissionConflict,
    #[error("keyed admission is unsupported by this store")]
    AdmissionUnsupported,
    #[error("bounded snapshots are unsupported by this store")]
    SnapshotUnsupported,
    #[error("session identity does not match")]
    SessionIdentityMismatch,
    #[error("session is busy")]
    Busy,
    #[error("record not found")]
    NotFound,
    #[error("unconsumed input remains")]
    PendingInput,
}

pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug, Clone)]
pub struct ManualClock(Arc<Mutex<OffsetDateTime>>);
impl ManualClock {
    #[must_use]
    pub fn new(now: OffsetDateTime) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }
    pub fn set(&self, now: OffsetDateTime) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
    }
}
impl Clock for ManualClock {
    fn now(&self) -> OffsetDateTime {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(env!("CARGO_PKG_NAME"), "crabber-core");
    }
}
