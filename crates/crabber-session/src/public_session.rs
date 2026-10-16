//! Bounded public metadata and history at one authenticated session revision.
pub(crate) mod compose;
mod poll;
use crate::{
    ApprovalContinuationState, ApprovalInteraction, ApprovalTerminationKind, AttemptSnapshot,
    ContinuationTarget, PublicHistoryPage, PublicRunSummary, SessionSummary, SnapshotContinuation,
    SnapshotLimits,
};
use crabber_core::{EventCursor, PublicStateContents, RunId, SessionId, SessionRevision};
pub use poll::{PublicResyncReason, PublicSessionPoll, PublicSessionSubscription};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicSessionLimits {
    /// Total safe header read/materialization bytes; envelope/token bytes are excluded.
    pub header_bytes: usize,
    /// Private bytes fetched solely for immutable approval proof, charged before decoding.
    pub proof_bytes: usize,
    pub interactions: u16,
    pub history: SnapshotLimits,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicSessionRequest {
    pub session_id: SessionId,
    pub limits: PublicSessionLimits,
    pub continuation: Option<SnapshotContinuation>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicSessionLimit {
    HeaderBytes,
    ProofBytes,
    Interactions,
    History(crate::SnapshotLimit),
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicSessionError {
    #[error("public session snapshots unsupported")]
    Unsupported,
    #[error("session not found")]
    NotFound,
    #[error("invalid public session snapshot request")]
    InvalidRequest,
    #[error("public session metadata integrity failure")]
    UnknownStoreOutcome,
}
impl From<crate::StoreError> for PublicSessionError {
    fn from(error: crate::StoreError) -> Self {
        match error {
            crate::StoreError::NotFound => Self::NotFound,
            crate::StoreError::HostUnsupported => Self::Unsupported,
            _ => Self::UnknownStoreOutcome,
        }
    }
}
/// Approval identity and response state only; private prompts and resolver claims are absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicApprovalSummary {
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub state: ApprovalContinuationState,
    pub termination: Option<ApprovalTerminationKind>,
    pub interactions: Vec<ApprovalInteraction>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicApprovalBarrier {
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
}
/// Version 1 covers native attempts, pre-effect approvals and explicit public UI state.
/// Queue and frontend/checkpoint continuation summaries require their later integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicControlHeader {
    pub version: u32,
    pub revision: SessionRevision,
    pub session: SessionSummary,
    pub attempt: Option<AttemptSnapshot>,
    pub approval: Option<PublicApprovalSummary>,
    pub barrier: Option<PublicApprovalBarrier>,
    pub contents: PublicStateContents,
    pub read_usage: PublicHeaderUsage,
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PublicHeaderUsage {
    pub encoded_bytes: usize,
    pub proof_bytes: usize,
}
#[derive(Debug, Clone)]
pub struct PublicHeaderRequest {
    pub session_id: SessionId,
    pub run_id: Option<RunId>,
    pub expected_revision: SessionRevision,
    pub max_bytes: usize,
    pub max_proof_bytes: usize,
    pub max_interactions: u16,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicHeaderOutcome {
    Header(Box<PublicControlHeader>),
    Limited(PublicSessionLimit),
    Invalidated,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicSessionPage {
    pub version: u32,
    pub captured_revision: SessionRevision,
    pub run: Option<PublicRunSummary>,
    pub header: PublicControlHeader,
    pub history: PublicHistoryPage,
}
impl PublicSessionPage {
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.run
            .as_ref()
            .is_some_and(|run| run.status.is_terminal())
    }
    #[must_use]
    pub fn high_water(&self) -> EventCursor {
        self.history.high_water
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicSessionOutcome {
    Page(Box<PublicSessionPage>),
    Limited {
        limit: PublicSessionLimit,
        continuation: Option<SnapshotContinuation>,
    },
    Invalidated,
}
impl PublicHeaderRequest {
    pub(crate) fn validate(&self) -> Result<(), PublicSessionError> {
        budget::validate_request(self)
    }
}
pub(crate) mod budget;
#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod races;

#[cfg(test)]
pub(crate) mod allocation;

#[cfg(test)]
pub(crate) mod rounds;
