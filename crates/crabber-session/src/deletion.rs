//! Guarded erasure controls. Hosts authorize erasure before calling the Store.
//!
//! A deletion receipt retains only the permanent identity needed for exact
//! retries. Session IDs cannot be reused. Erasure also removes historical
//! payload-bearing receipts; their absence never authorizes redispatch.
use crabber_core::{ControlId, InputFingerprint, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSession {
    pub id: ControlId,
    pub session_id: SessionId,
    /// Exact revision of an existing session. Missing sessions are not erased
    /// by assertion: pre-session ownership must still be considered separately.
    pub expected_revision: SessionRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionReceipt {
    pub session_id: SessionId,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub final_revision: SessionRevision,
    pub deleted_at: OffsetDateTime,
}

/// Safe, bounded reasons. No run checkpoints, owner claims or input payloads
/// are returned to diagnose why a session cannot yet be erased.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DeletionBlocker {
    Runtime,
    Attempt,
    Phase,
    Continuation,
    Queue,
    PendingInput,
    PendingControl,
    SessionOwner,
    OwnershipUnknown,
    ProofLimit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DeletionOutcome {
    Applied(DeletionReceipt),
    Replayed(DeletionReceipt),
    /// Refusal leaves the session, receipts and ownership unchanged. The caller
    /// performs explicit cleanup before submitting another guarded request.
    Blocked(DeletionBlocker),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SessionDisposition {
    Missing,
    Present {
        revision: SessionRevision,
    },
    Deleted {
        final_revision: SessionRevision,
        deleted_at: OffsetDateTime,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeletionError {
    #[error("session deletion unsupported")]
    Unsupported,
    #[error("invalid session deletion request")]
    InvalidRequest,
    #[error("session not found")]
    NotFound,
    #[error("session was permanently deleted")]
    Deleted,
    #[error("session deletion semantics conflict")]
    SemanticConflict,
    #[error("session revision changed")]
    RevisionConflict,
    #[error("session revision exhausted")]
    RevisionExhausted,
    #[error("session deletion outcome unknown")]
    UnknownStoreOutcome,
}

impl DeleteSession {
    /// Stable correlation for a host outbox, without dispatch or authority.
    /// The operation key selects a receipt and is outside semantic identity.
    /// # Errors
    /// Rejects empty, oversized or control-bearing identifiers before encoding.
    pub fn fingerprint(&self) -> Result<InputFingerprint, DeletionError> {
        use sha2::{Digest, Sha256};
        for (value, maximum) in [(&self.session_id.0, 512), (&self.id.0, 128)] {
            if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
                return Err(DeletionError::InvalidRequest);
            }
        }
        let encoded = serde_json::to_vec(&(
            "crabber.session-deletion.v1",
            &self.session_id,
            self.expected_revision,
        ))
        .map_err(|_| DeletionError::InvalidRequest)?;
        InputFingerprint::new(format!("{:x}", Sha256::digest(encoded)))
            .map_err(|_| DeletionError::InvalidRequest)
    }
}

pub(crate) mod coordinator;
pub(crate) mod coverage;
pub(crate) mod origin;
mod records;
pub(crate) use records::Tombstone;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) use records::timestamp;
#[cfg(test)]
pub(crate) mod contract;

mod validation;
pub(crate) use validation::{fork_error, management_error, owner_error, validate_session};

#[cfg(test)]
pub(crate) mod coordinator_contract;

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
pub(crate) mod process;
