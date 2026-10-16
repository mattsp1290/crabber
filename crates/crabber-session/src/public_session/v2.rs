//! Fixed public aggregate profile. The metadata sections are complete or Limited.
pub(crate) mod proof;
use crate::session_owner::*;
use crate::*;
use crabber_core::{
    ControlId, InteractionId, InteractionState, RunId, SessionId, SessionRevision, ToolCallId,
};
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
        // Validate untrusted routing text before constructing the legacy view.
        if self.session_id.0.is_empty()
            || self.session_id.0.len() > 512
            || self.session_id.0.chars().any(char::is_control)
            || self.run_id.as_ref().is_some_and(|id| {
                id.0.is_empty() || id.0.len() > 128 || id.0.chars().any(char::is_control)
            })
        {
            return Err(PublicSessionError::InvalidRequest);
        }
        PublicHeaderRequest {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            expected_revision: self.expected_revision,
            max_bytes: self.limits.header_bytes,
            max_proof_bytes: self.limits.proof_bytes,
            max_interactions: self.limits.approval_interactions,
        }
        .validate()?;
        if self.limits.tool_requirements > 64
            || self.limits.queue_entries > 64
            || self.limits.registered_owners > 1024
            || self.limits.run_controls > 64
        {
            return Err(PublicSessionError::InvalidRequest);
        }
        Ok(())
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
