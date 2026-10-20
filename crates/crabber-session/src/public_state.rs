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
mod source;
pub use source::{PublicSourceAuthority, PublicSourceMutation};
pub(crate) use source::{attach_before, before_event, verify_before_event};
pub(crate) use source::{source_event, source_matches, verify_source_event};

pub(crate) use source::{verify_before_event_with_proof, verify_source_event_with_proof};

/// Canonical immutable projection records for one committed source mutation.
/// This is metadata for delivery/reconciliation and grants no writer authority.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicSourcePublication {
    pub receipt: PublicStateReceipt,
    /// Actual pre-state snapshot (for deltas), followed by the actual final fact.
    pub events: Vec<crabber_core::EventRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_core::PublicActivity;
    use serde_json::json;

    #[test]
    fn legacy_activity_decode_and_fingerprint_do_not_apply_fresh_write_rules() {
        let session = SessionId::new();
        let run = RunId::new();
        for content in [
            serde_json::Value::Null,
            json!("legacy scalar"),
            json!([null, 1]),
        ] {
            let mutation = PublicStateMutation {
                id: ControlId::new(),
                expected_revision: SessionRevision::INITIAL,
                contents: PublicStateContents::new(
                    serde_json::Value::Null,
                    vec![PublicActivity::new("activity", "legacy", content).unwrap()],
                )
                .unwrap(),
            };
            let fingerprint = mutation.fingerprint(&session, &run).unwrap();
            let restored: PublicStateMutation =
                serde_json::from_value(serde_json::to_value(&mutation).unwrap()).unwrap();
            assert_eq!(restored.contents, mutation.contents);
            assert_eq!(restored.fingerprint(&session, &run).unwrap(), fingerprint);
            assert_eq!(
                validate_fresh_activities(&restored.contents),
                Err(PublicStateError::InvalidRequest)
            );
        }
    }
}
