//! Versioned explicit tool continuations; private state never enters results.
use crate::{
    AttemptAuthority, AttemptSnapshot, AttemptTarget, ContinuationTarget, PhaseAuthority,
    PhaseTarget, PreparedApproval,
};
use crabber_core::{
    ControlId, EventCursor, InputFingerprint, InteractionDescription, InteractionId,
    InteractionState, ResumableToolIdentity, RunFence, RunId, SessionId, SessionRevision,
    ToolCheckpoint, TurnId, Usage,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ToolContinuationError {
    #[error("durable tool continuations are unsupported")]
    Unsupported,
    #[error("tool continuation schema changed")]
    SchemaMismatch,
    #[error("tool continuation target not found")]
    NotFound,
    #[error("invalid tool continuation operation")]
    InvalidRequest,
    #[error("tool continuation operation semantics conflict")]
    SemanticConflict,
    #[error("public session revision changed")]
    RevisionConflict,
    #[error("tool continuation generation changed")]
    GenerationConflict,
    #[error("tool continuation owner is stale")]
    StaleOwner,
    #[error("tool continuation continuation owner lease is live")]
    LiveOwner,
    #[error("tool continuation response set is incomplete")]
    Pending,
    #[error("tool continuation expired")]
    Expired,
    #[error("tool continuation authority claim does not match")]
    AuthorityMismatch,
    #[error("continuation cleanup is incomplete")]
    CleanupBlocked,
    #[error("tool continuation transition is invalid")]
    InvalidTransition,
    #[error("tool continuation storage outcome is unknown")]
    UnknownStoreOutcome,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredToolInteraction {
    pub id: InteractionId,
    pub description: InteractionDescription,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenToolPhase {
    pub previous: PhaseTarget,
    pub next: PhaseTarget,
    pub call_binding: InputFingerprint,
    pub identity: ResumableToolIdentity,
    pub checkpoint: ToolCheckpoint,
    pub interactions: Vec<RequiredToolInteraction>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolContinuationSuspension {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub attempt: AttemptTarget,
    pub next_attempt: AttemptTarget,
    pub continuation: ContinuationTarget,
    pub expected_plan: InputFingerprint,
    pub phases: Vec<FrozenToolPhase>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontend: Vec<crate::FrozenFrontendCall>,
    /// All unfinished calls, in their original frozen execution order.
    pub calls: Vec<PreparedApproval>,
    pub usage: Usage,
    pub turns_used: u64,
    pub source_turn_id: Option<TurnId>,
    pub source_created_at: OffsetDateTime,
}
#[derive(Clone)]
pub struct ToolSuspensionAuthority {
    pub attempt: AttemptAuthority,
    pub phases: Vec<PhaseAuthority>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reply", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolInteractionReply {
    Answer { value: serde_json::Value },
    Deny,
    Cancel,
    Expire,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInteractionAnswer {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub interaction_id: InteractionId,
    pub resolver: crate::ApprovalResolverClaim,
    pub reply: ToolInteractionReply,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolContinuationClaim {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub expected_plan: InputFingerprint,
    pub owner: String,
    pub lease_ms: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolContinuationState {
    Waiting,
    Claimed,
    Released,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInteractionView {
    pub id: InteractionId,
    pub call_id: crabber_core::ToolCallId,
    pub phase: PhaseTarget,
    pub description: InteractionDescription,
    pub state: InteractionState,
    pub expires_at: Option<OffsetDateTime>,
    pub consumed: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolContinuationSnapshot {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub workspace_id: String,
    pub continuation: ContinuationTarget,
    pub attempt: AttemptTarget,
    pub next_attempt: AttemptTarget,
    pub revision: SessionRevision,
    pub state: ToolContinuationState,
    pub interactions: Vec<ToolInteractionView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontend_calls: Vec<crate::FrontendCallView>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolContinuationOutcome {
    Terminated {
        continuation: ContinuationTarget,
        original_cancel_id: ControlId,
    },
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
    FrontendClaimed {
        continuation: ContinuationTarget,
        attempt: AttemptSnapshot,
    },
    ReclaimedUnstarted {
        continuation: ContinuationTarget,
        original_claim_id: ControlId,
        attempt: AttemptSnapshot,
    },
    FrontendReclaimedUnstarted {
        continuation: ContinuationTarget,
        original_claim_id: ControlId,
        attempt: AttemptSnapshot,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolContinuationReceipt {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
    pub outcome: ToolContinuationOutcome,
}
#[derive(Clone)]
pub struct ToolPhaseResume {
    pub target: PhaseTarget,
    pub authority: PhaseAuthority,
    pub identity: ResumableToolIdentity,
    pub checkpoint: ToolCheckpoint,
    pub responses: Vec<crabber_core::AcceptedToolResponse>,
}
pub struct ToolContinuationGrant {
    pub fence: RunFence,
    pub authority: AttemptAuthority,
    pub phases: Vec<ToolPhaseResume>,
    pub frontend: Vec<crate::FrontendPhaseResume>,
    pub calls: Vec<crate::GrantedApproval>,
    pub source_turn_id: Option<TurnId>,
    pub usage: Usage,
    pub turns_used: u64,
}
pub enum ToolContinuationClaimOutcome {
    Applied {
        receipt: ToolContinuationReceipt,
        grant: Box<ToolContinuationGrant>,
    },
    Replayed(ToolContinuationReceipt),
}
macro_rules! redacted_debug { ($($t:ty),* $(,)?) => { $(impl std::fmt::Debug for $t { fn fmt(&self, f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { f.write_str(concat!(stringify!($t), "([redacted])")) } })* }; }
redacted_debug!(
    FrozenToolPhase,
    RequiredToolInteraction,
    ToolContinuationSuspension,
    ToolSuspensionAuthority,
    ToolInteractionAnswer,
    ToolInteractionView,
    ToolContinuationSnapshot,
    ToolPhaseResume,
    ToolContinuationGrant,
    ToolContinuationClaimOutcome
);
mod recovery;
pub use recovery::ToolContinuationRecovery;
mod audit;
mod current;
pub(crate) use current::{CurrentAudits, audit_key, terminal_candidate};
mod boundary;
pub(crate) mod canonical;
pub(crate) use audit::{audit, checkpoint, derived_id, verify_receipt};
mod records;
mod termination;
pub(crate) use termination::ToolContinuationTermination;
mod validation;
pub(crate) use records::*;
