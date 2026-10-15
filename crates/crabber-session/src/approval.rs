//! Durable approval of a complete prepared batch, before any call starts.
mod audit;
mod boundary;
mod records;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) mod sql;
mod validation;
use crate::{AttemptAuthority, AttemptSnapshot, AttemptTarget, PhaseTarget};
pub(crate) use audit::{event, verify_receipt};
use crabber_core::{
    ContinuationGeneration, ContinuationId, ControlId, EventCursor, InputFingerprint,
    InteractionDescription, InteractionId, InteractionState, RunFence, RunId, SessionId,
    SessionRevision, Usage,
};
pub(crate) use records::*;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const MAX_APPROVAL_CALLS: usize = 64;
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ApprovalError {
    #[error("durable approvals are unsupported")]
    Unsupported,
    #[error("approval target not found")]
    NotFound,
    #[error("invalid approval operation")]
    InvalidRequest,
    #[error("approval operation semantics conflict")]
    SemanticConflict,
    #[error("public session revision changed")]
    RevisionConflict,
    #[error("approval generation changed")]
    GenerationConflict,
    #[error("approval owner is stale")]
    StaleOwner,
    #[error("approval response set is incomplete")]
    Pending,
    #[error("approval expired")]
    Expired,
    #[error("approval authority claim does not match")]
    AuthorityMismatch,
    #[error("continuation cleanup is incomplete")]
    CleanupBlocked,
    #[error("approval transition is invalid")]
    InvalidTransition,
    #[error("approval storage outcome is unknown")]
    UnknownStoreOutcome,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationTarget {
    pub id: ContinuationId,
    pub generation: ContinuationGeneration,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallApproval {
    Allow,
    Deny,
    NotRequired,
    Ask {
        id: InteractionId,
        description: InteractionDescription,
    },
}
/// A phase planned for the next attempt; possession never permits dispatch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedApproval {
    pub target: PhaseTarget,
    pub call_binding: InputFingerprint,
    pub decision: CallApproval,
}
impl PreparedApproval {
    /// Freeze exact stored call semantics. This descriptor grants no dispatch
    /// authority; the Store verifies the complete batch under its transaction.
    /// # Errors
    /// Rejects a mismatched target or an unbounded stored call.
    pub fn new(
        target: PhaseTarget,
        call: &crabber_core::ToolCallRecord,
        decision: CallApproval,
    ) -> Result<Self, ApprovalError> {
        if target.call_id != call.id {
            return Err(ApprovalError::InvalidRequest);
        }
        let call_binding =
            crate::phase::call_binding(call).map_err(|_| ApprovalError::InvalidRequest)?;
        Ok(Self {
            target,
            call_binding,
            decision,
        })
    }
    /// # Errors
    /// Rejects a changed call identity, arguments, name, scope or retry declaration.
    pub fn verify_call(&self, call: &crabber_core::ToolCallRecord) -> Result<(), ApprovalError> {
        if self.target.call_id != call.id
            || crate::phase::call_binding(call).map_err(|_| ApprovalError::InvalidRequest)?
                != self.call_binding
        {
            return Err(ApprovalError::SemanticConflict);
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSuspension {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub attempt: AttemptTarget,
    pub expected_plan: InputFingerprint,
    pub continuation: ContinuationTarget,
    pub next_attempt: AttemptTarget,
    pub calls: Vec<PreparedApproval>,
    pub usage: Usage,
    /// Total model turns consumed by this run. Legacy missing counts are ambiguous.
    #[serde(default)]
    pub turns_used: Option<u64>,
    /// Generated source presentation, excluded from semantic retry identity.
    #[serde(default)]
    pub source_turn_id: Option<crabber_core::TurnId>,
    #[serde(default)]
    pub source_created_at: Option<OffsetDateTime>,
}
impl std::fmt::Debug for ApprovalSuspension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApprovalSuspension([redacted])")
    }
}
/// Host-asserted verified identity and nonsecret policy authority. Store records
/// this assertion; it cannot authenticate a human or validate a credential.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalResolverClaim {
    pub resolver_id: String,
    pub authority: crabber_core::AuthorityRequirement,
    pub authority_claim: String,
}
impl std::fmt::Debug for ApprovalResolverClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApprovalResolverClaim([redacted])")
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReply {
    Grant,
    Deny,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalAnswer {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub interaction_id: InteractionId,
    pub resolver: ApprovalResolverClaim,
    pub reply: ApprovalReply,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalContinuationClaim {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub expected_plan: InputFingerprint,
    /// Dispatch bookkeeping, excluded from semantic retry identity.
    pub owner: String,
    pub lease_ms: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalContinuationState {
    Waiting,
    Claimed,
    Released,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalInteraction {
    pub id: InteractionId,
    pub state: InteractionState,
    pub expires_at: Option<OffsetDateTime>,
    pub consumed: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSnapshot {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub workspace_id: String,
    pub revision: SessionRevision,
    pub continuation: ContinuationTarget,
    pub attempt: AttemptTarget,
    pub next_attempt: AttemptTarget,
    pub state: ApprovalContinuationState,
    pub calls: Vec<PreparedApproval>,
    pub interactions: Vec<ApprovalInteraction>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalOutcome {
    Suspended {
        continuation: ContinuationTarget,
    },
    Answered {
        continuation: ContinuationTarget,
        interaction_id: InteractionId,
        state: InteractionState,
    },
    Claimed {
        continuation: ContinuationTarget,
        attempt: AttemptSnapshot,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalReceipt {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
    pub outcome: ApprovalOutcome,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantedDecision {
    Allow,
    Deny,
    NotRequired,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedApproval {
    pub target: PhaseTarget,
    pub call_binding: InputFingerprint,
    pub decision: GrantedDecision,
}
/// Ephemeral grant returned only by a fresh atomic continuation claim. Receipt
/// replay and snapshot reads never contain these native authority capabilities.
pub struct ApprovalGrant {
    pub fence: RunFence,
    pub authority: AttemptAuthority,
    pub calls: Vec<GrantedApproval>,
    pub source_turn_id: Option<crabber_core::TurnId>,
    pub usage: Usage,
    pub turns_used: u64,
}
impl std::fmt::Debug for ApprovalGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApprovalGrant([redacted])")
    }
}
#[derive(Debug)]
pub enum ApprovalClaimOutcome {
    Applied {
        receipt: ApprovalReceipt,
        grant: Box<ApprovalGrant>,
    },
    Replayed(ApprovalReceipt),
}
