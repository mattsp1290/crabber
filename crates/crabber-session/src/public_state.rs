//! Public-state transactions. Unstable until durable-host qualification.
use crabber_core::{
    ControlId, EventCursor, InputFingerprint, PublicStateContents, RunId, SessionId,
    SessionRevision,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicStateError {
    #[error("public state operations are unsupported")]
    Unsupported,
    #[error("public state target not found")]
    NotFound,
    #[error("invalid public state operation")]
    InvalidRequest,
    #[error("public state operation semantics conflict")]
    SemanticConflict,
    #[error("public session revision changed")]
    RevisionConflict,
    #[error("public session revision exhausted")]
    RevisionExhausted,
    #[error("public state owner is stale or unstarted")]
    StaleOwner,
    #[error("public state storage outcome is unknown")]
    UnknownStoreOutcome,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicStateSnapshot {
    pub session_id: SessionId,
    pub revision: SessionRevision,
    pub contents: PublicStateContents,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicStateMutation {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub contents: PublicStateContents,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicStateReceipt {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
}
impl PublicStateMutation {
    pub(crate) fn validate(&self) -> Result<(), PublicStateError> {
        records::validate_id(&self.id)
    }
    /// Compute the canonical semantics to retain with an outbox request.
    /// Unstable until durable-host qualification. It grants no authority.
    /// # Errors
    /// Rejects unbounded identity or invalid operation metadata.
    pub fn fingerprint(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<InputFingerprint, PublicStateError> {
        self.validate()?;
        if session.0.is_empty()
            || session.0.len() > 512
            || session.0.chars().any(char::is_control)
            || run.0.is_empty()
            || run.0.len() > 128
            || run.0.chars().any(char::is_control)
        {
            return Err(PublicStateError::InvalidRequest);
        }
        records::semantic_digest(session, run, self.expected_revision, &self.contents)
    }
}
mod records;
pub(crate) use records::*;
