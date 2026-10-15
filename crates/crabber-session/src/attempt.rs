//! Experimental attempt ownership; public identities never grant execution authority.
mod records;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) mod sql;
use crabber_core::{
    AttemptGeneration, AttemptId, ControlId, EventCursor, InputFingerprint, RunId, SessionId,
    SessionRevision,
};
pub(crate) use records::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AttemptError {
    #[error("execution attempts are unsupported")]
    Unsupported,
    #[error("execution attempt target not found")]
    NotFound,
    #[error("invalid execution attempt operation")]
    InvalidRequest,
    #[error("execution attempt operation semantics conflict")]
    SemanticConflict,
    #[error("public session revision changed")]
    RevisionConflict,
    #[error("execution attempt generation changed")]
    GenerationConflict,
    #[error("execution attempt generation exhausted")]
    GenerationExhausted,
    #[error("public session revision exhausted")]
    RevisionExhausted,
    #[error("execution attempt still has an owner")]
    Busy,
    #[error("execution attempt owner changed")]
    StaleOwner,
    #[error("execution attempt owner has a live lease")]
    LiveOwner,
    #[error("invalid execution attempt transition")]
    InvalidTransition,
    #[error("execution attempt has unquiesced phases")]
    CleanupBlocked,
    #[error("pending input requires another active turn")]
    PendingInput,
    #[error("execution attempt storage outcome is unknown")]
    UnknownStoreOutcome,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptState {
    Unstarted,
    Started,
    Suspended,
    Completed,
    Failed,
    Interrupted,
    OutcomeUnknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptCleanup {
    Pending,
    Complete,
    Incomplete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptFinish {
    Suspended,
    Completed,
    Failed,
    Interrupted,
    OutcomeUnknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptCleanupResult {
    Complete,
    Incomplete,
}
impl From<AttemptFinish> for AttemptState {
    fn from(value: AttemptFinish) -> Self {
        match value {
            AttemptFinish::Suspended => Self::Suspended,
            AttemptFinish::Completed => Self::Completed,
            AttemptFinish::Failed => Self::Failed,
            AttemptFinish::Interrupted => Self::Interrupted,
            AttemptFinish::OutcomeUnknown => Self::OutcomeUnknown,
        }
    }
}
impl From<AttemptCleanupResult> for AttemptCleanup {
    fn from(value: AttemptCleanupResult) -> Self {
        match value {
            AttemptCleanupResult::Complete => Self::Complete,
            AttemptCleanupResult::Incomplete => Self::Incomplete,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptTarget {
    pub id: AttemptId,
    pub generation: AttemptGeneration,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttemptCommand {
    /// Initial reservation guarded atomically by retained typed admission input
    /// and absence of execution history. Safe after admission acknowledgment loss.
    ReserveBound {
        target: AttemptTarget,
    },
    Reserve {
        target: AttemptTarget,
    },
    ReclaimUnstarted {
        target: AttemptTarget,
    },
    Begin {
        target: AttemptTarget,
    },
    Finish {
        target: AttemptTarget,
        outcome: AttemptFinish,
    },
    RecordCleanup {
        target: AttemptTarget,
        result: AttemptCleanupResult,
    },
}
impl AttemptCommand {
    #[must_use]
    pub fn target(&self) -> &AttemptTarget {
        match self {
            Self::ReserveBound { target }
            | Self::Reserve { target }
            | Self::ReclaimUnstarted { target }
            | Self::Begin { target }
            | Self::Finish { target, .. }
            | Self::RecordCleanup { target, .. } => target,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptMutation {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub command: AttemptCommand,
}
impl AttemptMutation {
    pub(crate) fn validate(&self) -> Result<(), AttemptError> {
        records::validate_request(self)
    }

    /// Canonical outbox semantics. A matching hash grants no execution authority.
    /// # Errors
    /// Rejects invalid or unbounded identity.
    pub fn fingerprint(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<InputFingerprint, AttemptError> {
        self.validate()?;
        if session.0.is_empty()
            || session.0.len() > 512
            || session.0.chars().any(char::is_control)
            || run.0.is_empty()
            || run.0.len() > 128
            || run.0.chars().any(char::is_control)
        {
            return Err(AttemptError::InvalidRequest);
        }
        let mut value = serde_json::to_value((
            "crabber.attempt.v1",
            session,
            run,
            self.expected_revision,
            &self.command,
        ))
        .map_err(|_| AttemptError::InvalidRequest)?;
        value.sort_all_objects();
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(value.to_string().as_bytes())
        ))
        .map_err(|_| AttemptError::InvalidRequest)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptSnapshot {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub target: AttemptTarget,
    pub state: AttemptState,
    pub cleanup: AttemptCleanup,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedAttempt {
    pub revision: SessionRevision,
    pub attempt: AttemptSnapshot,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptReceipt {
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
    pub attempt: AttemptSnapshot,
}
/// Ephemeral native-worker authority. Never serialize it into an outbox or receipt.
/// Attempt work starts only after a fresh Applied Begin. Individual tool phases
/// additionally require their own phase authority; replay never grants dispatch.
#[derive(Clone)]
pub struct AttemptAuthority {
    pub(crate) snapshot: AttemptSnapshot,
    pub(crate) token: String,
    pub(crate) run_token: String,
}
impl std::fmt::Debug for AttemptAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AttemptAuthority([redacted])")
    }
}
impl AttemptAuthority {
    /// Construct worker authority inside a trusted custom Store adapter after
    /// its atomic ownership decision. This constructor does not verify storage.
    /// # Errors
    /// Rejects empty, oversized or nonprintable opaque tokens.
    pub fn from_store(
        snapshot: AttemptSnapshot,
        token: String,
        run_token: String,
    ) -> Result<Self, AttemptError> {
        for value in [&token, &run_token] {
            if value.is_empty() || value.len() > 512 || !value.bytes().all(|b| b.is_ascii_graphic())
            {
                return Err(AttemptError::InvalidRequest);
            }
        }
        Ok(Self {
            snapshot,
            token,
            run_token,
        })
    }
    /// Verify against the adapter's current private record without exposing tokens.
    #[must_use]
    pub fn matches_store(&self, snapshot: &AttemptSnapshot, token: &str, run_token: &str) -> bool {
        self.snapshot.session_id == snapshot.session_id
            && self.snapshot.run_id == snapshot.run_id
            && self.snapshot.target == snapshot.target
            && self.token == token
            && self.run_token == run_token
    }
}
#[derive(Debug)]
pub enum AttemptMutationOutcome {
    Applied {
        receipt: AttemptReceipt,
        authority: Option<AttemptAuthority>,
    },
    Replayed(AttemptReceipt),
}
