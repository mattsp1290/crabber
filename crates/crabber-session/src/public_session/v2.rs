//! Fixed public aggregate profile. The metadata sections are complete or Limited.
pub(crate) mod compose;
mod poll;
pub(crate) mod projection;
pub(crate) mod proof;
use crate::session_owner::{
    SessionOwnerKind, SessionOwnerParent, SessionOwnerSnapshot, SessionOwnerState,
    SessionOwnerTarget, SessionOwnerTermination,
};
use crate::{
    AttemptSnapshot, AttemptTarget, ContinuationTarget, PhaseTarget, PublicApprovalBarrier,
    PublicApprovalSummary, PublicHeaderUsage, PublicHistoryPage, PublicRunSummary,
    PublicSessionError, QueueSummary, RunControlSummary, SessionSummary, SnapshotContinuation,
    SnapshotLimit, SnapshotLimits, ToolContinuationState,
};
use crabber_core::{
    ControlId, InteractionId, InteractionState, RunId, SessionId, SessionRevision, ToolCallId,
};
pub use poll::{PublicSessionPollV2, PublicSessionSubscriptionV2};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicSessionLimitV2 {
    HeaderBytes,
    ProofBytes,
    ApprovalInteractions,
    ToolRequirements,
    QueueEntries,
    RegisteredOwners,
    RunControls,
    History(SnapshotLimit),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicSessionLimitsV2 {
    pub header_bytes: usize,
    pub proof_bytes: usize,
    pub approval_interactions: u16,
    pub tool_requirements: u16,
    pub queue_entries: u16,
    pub registered_owners: u16,
    pub run_controls: u16,
    pub history: SnapshotLimits,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicSessionRequestV2 {
    pub session_id: SessionId,
    pub limits: PublicSessionLimitsV2,
    pub continuation: Option<SnapshotContinuation>,
}
#[derive(Debug, Clone)]
pub struct PublicHeaderRequestV2 {
    pub session_id: SessionId,
    pub run_id: Option<RunId>,
    pub expected_revision: SessionRevision,
    pub limits: PublicSessionLimitsV2,
}
impl PublicHeaderRequestV2 {
    pub(crate) fn validate(&self) -> Result<(), PublicSessionError> {
        proof::validate_request(self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicRegisteredOwner {
    pub target: SessionOwnerTarget,
    pub kind: SessionOwnerKind,
    pub component_id: String,
    pub parent: Option<SessionOwnerParent>,
    pub state: SessionOwnerState,
    pub termination: Option<SessionOwnerTermination>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}
impl From<&SessionOwnerSnapshot> for PublicRegisteredOwner {
    fn from(owner: &SessionOwnerSnapshot) -> Self {
        Self {
            target: owner.target.clone(),
            kind: owner.binding.kind,
            component_id: owner.binding.component_id.clone(),
            parent: owner.binding.parent.clone(),
            state: owner.state,
            termination: owner.termination,
            created_at: owner.created_at,
            updated_at: owner.updated_at,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicToolRequirement {
    pub id: InteractionId,
    pub call_id: ToolCallId,
    pub phase: PhaseTarget,
    pub state: InteractionState,
    pub expires_at: Option<OffsetDateTime>,
    pub consumed: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicFrontendRequirement {
    pub call_id: ToolCallId,
    /// Frozen resume identity; this does not assert the phase has started.
    pub phase: PhaseTarget,
    pub name: String,
    pub descriptor_fingerprint: crabber_core::InputFingerprint,
    pub consumed: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicToolSummary {
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub attempt: AttemptTarget,
    pub next_attempt: AttemptTarget,
    pub state: ToolContinuationState,
    /// Authenticated explicit cancellation source; no resolver claim.
    pub original_cancel_id: Option<ControlId>,
    pub requirements: Vec<PublicToolRequirement>,
    pub frontend: Vec<PublicFrontendRequirement>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicContinuationBarrierV2 {
    Approval(PublicApprovalBarrier),
    Tool {
        run_id: RunId,
        continuation: ContinuationTarget,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicControlHeaderV2 {
    pub version: u32,
    pub revision: SessionRevision,
    pub session: SessionSummary,
    pub attempt: Option<AttemptSnapshot>,
    pub approval: Option<PublicApprovalSummary>,
    pub tool: Option<PublicToolSummary>,
    pub barrier: Option<PublicContinuationBarrierV2>,
    pub queue: Vec<QueueSummary>,
    /// Registered unjoined resources only. Absence does not prove global quiescence.
    pub owners: Vec<PublicRegisteredOwner>,
    /// Controls for the latest Run only.
    pub controls: Vec<RunControlSummary>,
    pub contents: crabber_core::PublicStateContents,
    pub read_usage: PublicHeaderUsage,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicHeaderOutcomeV2 {
    Header(Box<PublicControlHeaderV2>),
    Limited(PublicSessionLimitV2),
    Invalidated,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicSessionPageV2 {
    pub version: u32,
    pub captured_revision: SessionRevision,
    pub run: Option<PublicRunSummary>,
    pub header: PublicControlHeaderV2,
    pub history: PublicHistoryPage,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicSessionOutcomeV2 {
    Page(Box<PublicSessionPageV2>),
    Limited {
        limit: PublicSessionLimitV2,
        continuation: Option<SnapshotContinuation>,
    },
    Invalidated,
}

impl PublicSessionPageV2 {
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.run
            .as_ref()
            .is_some_and(|run| run.status.is_terminal())
    }
    #[must_use]
    pub fn high_water(&self) -> crabber_core::EventCursor {
        self.history.high_water
    }
}
impl PublicSessionRequestV2 {
    pub(crate) fn validate(&self) -> Result<(), PublicSessionError> {
        if self.session_id.0.is_empty()
            || self.session_id.0.len() > 512
            || self.session_id.0.chars().any(char::is_control)
            || self.continuation.as_ref().is_some_and(|c| c.0.len() > 4096)
        {
            return Err(PublicSessionError::InvalidRequest);
        }
        PublicHeaderRequestV2 {
            session_id: self.session_id.clone(),
            run_id: None,
            expected_revision: SessionRevision::INITIAL,
            limits: self.limits,
        }
        .validate()
    }
}

#[cfg(test)]
pub(crate) mod tests;
