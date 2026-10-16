//! Copying closed conversation history does not create execution records.
use crate::{SessionDefaults, SessionSummary, SnapshotLimit, SnapshotLimits};
use crabber_core::{ControlId, InputFingerprint, MessageId, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) mod history;
mod records;
pub(crate) use records::Audit;

/// An explicitly selected inclusive message boundary, independent of snapshot
/// pagination. The native store must authenticate its committed source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ForkBoundary {
    Empty,
    AfterMessage { message_id: MessageId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkSession {
    pub id: ControlId,
    pub source_session_id: SessionId,
    /// Exact admitted source context; the target inherits both values. Including
    /// them in original semantics also authenticates replay after source erasure.
    pub workspace_id: String,
    pub directory: String,
    pub expected_source_revision: SessionRevision,
    pub boundary: ForkBoundary,
    pub target_session_id: SessionId,
    pub title: String,
    pub defaults: SessionDefaults,
    /// Total raw prefix read limits, before private reasoning/state is omitted.
    /// A larger allowance on retry does not change the selected copy semantics.
    pub limits: SnapshotLimits,
    /// Actual immutable source carriers, charged separately from history.
    pub max_proof_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkReceipt {
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub source_session_id: SessionId,
    pub source_revision: SessionRevision,
    pub boundary: ForkBoundary,
    pub target: SessionSummary,
    pub messages: usize,
    pub imported_calls: usize,
    /// Immutable copied message/call/provenance identity, excluding later target work.
    pub history_digest: InputFingerprint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForkOutcome {
    Applied(ForkReceipt),
    Replayed(ForkReceipt),
    /// No copy was committed. Choosing the returned boundary requires another
    /// explicit request; the store never silently shortens the copied prefix.
    UnsafeBoundary {
        nearest_earlier: ForkBoundary,
    },
    Limited(SnapshotLimit),
    ProofLimited,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ForkError {
    #[error("safe session fork unsupported")]
    Unsupported,
    #[error("source session or boundary not found")]
    NotFound,
    #[error("invalid session fork request")]
    InvalidRequest,
    #[error("source session revision changed")]
    RevisionConflict,
    #[error("target session or reserved identity already exists")]
    TargetExists,
    #[error("target session was permanently deleted")]
    Deleted,
    #[error("session fork semantics conflict")]
    SemanticConflict,
    #[error("session fork source integrity failure or unknown outcome")]
    UnknownStoreOutcome,
}

impl ForkSession {
    /// Original copy identity; read allowances are deliberately not semantic.
    /// # Errors
    /// Rejects malformed scope, self-forks and unbounded display metadata/limits.
    pub fn fingerprint(&self) -> Result<InputFingerprint, ForkError> {
        crate::management::validate_scope(&self.source_session_id, &self.id)
            .map_err(|_| ForkError::InvalidRequest)?;
        crate::management::validate_scope(&self.target_session_id, &self.id)
            .map_err(|_| ForkError::InvalidRequest)?;
        if self.source_session_id == self.target_session_id
            || self.limits.messages > 1024
            || self.limits.tool_calls > 4096
            || self.limits.parts > 8192
            || self.limits.encoded_bytes > 32 * 1024 * 1024
            || self.limits.text_bytes > 32 * 1024 * 1024
            || self.max_proof_bytes > 32 * 1024 * 1024
        {
            return Err(ForkError::InvalidRequest);
        }
        if let ForkBoundary::AfterMessage { message_id } = &self.boundary {
            history::validate_id(&message_id.0).map_err(|_| ForkError::InvalidRequest)?;
        }
        crate::management::bounded(&self.title, 1024, true)
            .map_err(|_| ForkError::InvalidRequest)?;
        crate::management::bounded(&self.workspace_id, 512, false)
            .map_err(|_| ForkError::InvalidRequest)?;
        crate::management::bounded(&self.directory, 4096, false)
            .map_err(|_| ForkError::InvalidRequest)?;
        crate::management::validate_defaults(&self.defaults)
            .map_err(|_| ForkError::InvalidRequest)?;
        let mut value = serde_json::json!([
            "crabber.session-fork.v1",
            self.source_session_id,
            self.workspace_id,
            self.directory,
            self.expected_source_revision,
            self.boundary,
            self.target_session_id,
            self.title,
            self.defaults
        ]);
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value).map_err(|_| ForkError::InvalidRequest)?;
        InputFingerprint::new(format!("{:x}", Sha256::digest(bytes)))
            .map_err(|_| ForkError::InvalidRequest)
    }
}
