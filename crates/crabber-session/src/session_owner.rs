//! Independently fenced session resource identities. These are lifecycle
//! contracts, not a deletion-readiness assertion or execution authority on read.
use crabber_core::{
    InputFingerprint, RunId, SessionId, SessionOwnerGeneration, SessionOwnerId,
    SessionOwnerOperationId, ToolCallId,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionOwnerError {
    #[error("durable session ownership unsupported")]
    Unsupported,
    #[error("approval preparation source: {0}")]
    ApprovalSource(crate::ApprovalError),
    #[error("tool continuation preparation source: {0}")]
    ToolSource(crate::ToolContinuationError),
    #[error("session owner not found")]
    NotFound,
    #[error("session was permanently deleted")]
    Deleted,
    #[error("invalid session ownership request")]
    InvalidRequest,
    #[error("session ownership semantics conflict")]
    SemanticConflict,
    #[error("session owner context changed")]
    IdentityMismatch,
    #[error("another run or preparation owns the session")]
    Busy,
    #[error("session revision changed before preparation")]
    RevisionConflict,
    #[error("session ownership generation changed")]
    GenerationConflict,
    #[error("session ownership authority is stale")]
    StaleOwner,
    #[error("session owner lease is live")]
    LiveOwner,
    #[error("session owner transition is invalid")]
    InvalidTransition,
    #[error("session owner cancellation was already requested")]
    CancellationPending,
    #[error("session ownership capacity exceeded")]
    Capacity,
    #[error("session ownership storage outcome unknown")]
    UnknownStoreOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOwnerKind {
    Preparation,
    DetachedInvocation,
    SessionService,
    RetainedCapacity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerTarget {
    pub id: SessionOwnerId,
    pub generation: SessionOwnerGeneration,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerParent {
    pub run_id: RunId,
    pub call_id: Option<ToolCallId>,
    pub phase: Option<crate::PhaseTarget>,
}
/// Exact original resource identity, including preparations before a Run exists.
/// Empty workspace/directory values stay unavailable; they never become defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerBinding {
    pub session_id: SessionId,
    pub workspace_id: String,
    pub directory: String,
    pub id: SessionOwnerId,
    pub kind: SessionOwnerKind,
    pub component_id: String,
    pub behavior_fingerprint: InputFingerprint,
    pub parent: Option<SessionOwnerParent>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOwnerState {
    ReservedUnstarted,
    Active,
    Uncertain,
    Joined,
}
/// Safe public metadata. Private lease renewal cannot change this view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerSnapshot {
    pub binding: SessionOwnerBinding,
    pub target: SessionOwnerTarget,
    pub state: SessionOwnerState,
    pub termination: Option<SessionOwnerTermination>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<SessionOwnerCancellation>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerResolver {
    pub resolver_id: String,
    pub authority_claim: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOwnerTermination {
    Returned,
    Panicked,
    Cancelled,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveSessionOwner {
    pub id: SessionOwnerOperationId,
    pub binding: SessionOwnerBinding,
    /// Private worker bookkeeping, excluded from original retry identity.
    pub owner: String,
    pub lease_ms: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub enum SessionOwnerCommand {
    Begin,
    Renew {
        lease_ms: u64,
    },
    /// Caller has observed the actual task/resource join and complete cleanup.
    /// A returned result, cancellation or expired lease is insufficient.
    Join {
        termination: SessionOwnerTermination,
    },
    MarkUncertain {
        claim: SessionOwnerResolver,
    },
    ReclaimUnstarted {
        owner: String,
        lease_ms: u64,
    },
    /// Exact expired owner reconciliation, never a replacement execution grant.
    ResolveJoined {
        termination: SessionOwnerTermination,
        claim: SessionOwnerResolver,
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutateSessionOwner {
    pub id: SessionOwnerOperationId,
    pub session_id: SessionId,
    pub target: SessionOwnerTarget,
    pub command: SessionOwnerCommand,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOwnerOperation {
    Reserve,
    Begin,
    Renew,
    Join,
    MarkUncertain,
    ReclaimUnstarted,
    ResolveJoined,
    RequestCancel,
    ObserveCancel,
    ActivateScope,
}
/// Original operation metadata. It is never a replacement private grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerReceipt {
    pub session_id: SessionId,
    pub operation_id: SessionOwnerOperationId,
    pub semantic_digest: InputFingerprint,
    pub operation: SessionOwnerOperation,
    pub owner: SessionOwnerSnapshot,
}
#[derive(Clone)]
pub struct SessionOwnerAuthority {
    pub(crate) session_id: SessionId,
    pub(crate) target: SessionOwnerTarget,
    pub(crate) token: String,
}
impl SessionOwnerAuthority {
    /// Construct only inside a trusted Store adapter after its fresh commit.
    /// This does not verify storage and cannot turn receipt replay into a grant.
    /// # Errors
    /// Rejects invalid routing identity or opaque token representation.
    pub fn from_store(
        session_id: SessionId,
        target: SessionOwnerTarget,
        token: String,
    ) -> Result<Self, SessionOwnerError> {
        identifier(&session_id.0, 512, false)?;
        identifier(&target.id.0, 128, false)?;
        if token.is_empty() || token.len() > 512 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(SessionOwnerError::InvalidRequest);
        }
        Ok(Self {
            session_id,
            target,
            token,
        })
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn target(&self) -> &SessionOwnerTarget {
        &self.target
    }
    /// Compare an opaque grant with a trusted adapter's stored ownership token.
    #[must_use]
    pub fn authenticates(
        &self,
        session: &SessionId,
        target: &SessionOwnerTarget,
        token: &str,
    ) -> bool {
        self.session_id == *session
            && self.target == *target
            && !token.is_empty()
            && self.token == token
    }
}
pub enum SessionOwnerOutcome {
    Applied {
        receipt: SessionOwnerReceipt,
        /// Fresh Reserve/Reclaim only. Begin requires an acknowledged Applied
        /// receipt before dispatch using the already retained owner authority.
        authority: Option<SessionOwnerAuthority>,
    },
    Replayed(SessionOwnerReceipt),
}

impl SessionOwnerBinding {
    /// # Errors
    /// Rejects unbounded identity or internally inconsistent parent provenance.
    pub fn validate(&self) -> Result<(), SessionOwnerError> {
        identifier(&self.session_id.0, 512, false)?;
        validation::context_text(&self.workspace_id, 512)?;
        validation::context_text(&self.directory, 4096)?;
        identifier(&self.id.0, 128, false)?;
        identifier(&self.component_id, 512, false)?;
        if (self.kind == SessionOwnerKind::Preparation && self.parent.is_some())
            || (self.kind == SessionOwnerKind::DetachedInvocation && self.parent.is_none())
        {
            return Err(SessionOwnerError::InvalidRequest);
        }
        if let Some(parent) = &self.parent {
            identifier(&parent.run_id.0, 128, false)?;
            if let Some(call) = &parent.call_id {
                identifier(&call.0, 512, false)?;
            }
            if let Some(phase) = &parent.phase {
                identifier(&phase.id.0, 128, false)?;
                identifier(&phase.attempt.id.0, 128, false)?;
                identifier(&phase.call_id.0, 512, false)?;
                if parent.call_id.as_ref() != Some(&phase.call_id) {
                    return Err(SessionOwnerError::InvalidRequest);
                }
            }
        }
        Ok(())
    }
}
impl ReserveSessionOwner {
    /// Owner/lease bookkeeping may change on retry; original metadata grants
    /// no authority. The immutable resource binding cannot change.
    /// # Errors
    /// Rejects invalid binding, identifiers or lease declarations.
    pub fn fingerprint(&self) -> Result<InputFingerprint, SessionOwnerError> {
        identifier(&self.id.0, 128, false)?;
        self.binding.validate()?;
        lease(&self.owner, self.lease_ms)?;
        digest(&("crabber.session-owner.reserve.v1", &self.id, &self.binding))
    }
}
impl MutateSessionOwner {
    /// # Errors
    /// Rejects invalid targets, bounded resolver claims or lease declarations.
    pub fn fingerprint(&self) -> Result<InputFingerprint, SessionOwnerError> {
        identifier(&self.id.0, 128, false)?;
        identifier(&self.session_id.0, 512, false)?;
        identifier(&self.target.id.0, 128, false)?;
        let command = match &self.command {
            SessionOwnerCommand::ReclaimUnstarted { owner, lease_ms } => {
                lease(owner, *lease_ms)?;
                // Generated worker bookkeeping does not change reconciliation identity.
                serde_json::json!({"operation":"ReclaimUnstarted"})
            }
            command => {
                match command {
                    SessionOwnerCommand::Renew { lease_ms } => lease("retained-owner", *lease_ms)?,
                    SessionOwnerCommand::MarkUncertain { claim }
                    | SessionOwnerCommand::ResolveJoined { claim, .. } => resolver(claim)?,
                    _ => {}
                }
                serde_json::to_value(command).map_err(|_| SessionOwnerError::InvalidRequest)?
            }
        };
        digest(&(
            "crabber.session-owner.mutate.v1",
            &self.id,
            &self.session_id,
            &self.target,
            command,
        ))
    }
}
macro_rules! redacted_debug {
    ($($ty:ty),* $(,)?) => { $(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($ty), "([redacted])"))
        }
    })* };
}
redacted_debug!(
    SessionOwnerAuthority,
    ReserveSessionOwner,
    MutateSessionOwner,
    SessionOwnerCommand,
    SessionOwnerOutcome
);

pub mod owned_scope;
pub(crate) mod validation;
use validation::{digest, identifier, lease, resolver};
pub(crate) mod audit;
mod ledger;
pub(crate) mod record;
pub(crate) use ledger::Ledger;
/// Authority for the actual native parent at a detached ownership handoff.
/// Session-only preparation/service reservations have no execution parent.
#[derive(Clone, Debug)]
pub enum SessionOwnerParentAuthority {
    Attempt(crate::AttemptAuthority),
    Phase(crate::PhaseAuthority),
}

pub mod preparation;

pub mod cancellation;
pub use cancellation::{
    ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerCancelObservation,
    SessionOwnerCancellation,
};

#[cfg(test)]
pub(crate) mod preparation_contract;

#[cfg(test)]
pub(crate) mod cancellation_contract;
