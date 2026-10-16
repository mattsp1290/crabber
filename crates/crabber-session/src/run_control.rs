//! Durable, exactly targeted host cancellation and next-turn input.
mod fingerprint;
mod suspension;
pub(crate) mod terminal;
pub(crate) use suspension::*;
mod records;
use crabber_core::{
    ControlId, EventCursor, InputEnvelope, InputFingerprint, RunId, RunStatus, SessionId,
    SessionRevision, TurnId,
};
pub(crate) use records::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RunControlError {
    #[error("durable run controls are unsupported")]
    Unsupported,
    #[error("run control target not found")]
    NotFound,
    #[error("invalid run control request")]
    InvalidRequest,
    #[error("run control semantics conflict")]
    SemanticConflict,
    #[error("session revision changed")]
    RevisionConflict,
    #[error("run control target is terminal")]
    Terminal,
    #[error("run control capacity exhausted")]
    Capacity,
    #[error("run control owner is stale")]
    StaleOwner,
    #[error("run control requires waiting-state reconciliation")]
    Busy,
    #[error("run control storage outcome is unknown")]
    UnknownStoreOutcome,
}

/// Native fence rejection is distinct from a failed read of its evidence.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn fenced_error(error: &crate::StoreError) -> RunControlError {
    match error {
        crate::StoreError::Conflict
        | crate::StoreError::AdmissionRecoveryRequired
        | crate::StoreError::NotFound => RunControlError::StaleOwner,
        _ => RunControlError::UnknownStoreOutcome,
    }
}

/// Immutable run binding, excluding renewable lease and worker credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlTarget {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub config_hash: String,
    pub plan_fingerprint: String,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RunControlAction {
    Cancel {
        resolver: crate::ApprovalResolverClaim,
    },
    /// User messages only. Supplied message, parent and part IDs are retained.
    /// State/context/frontend declarations require their dedicated APIs.
    Steer { input: InputEnvelope },
}
impl std::fmt::Debug for RunControlAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Cancel { .. } => "Cancel",
            Self::Steer { .. } => "Steer([redacted])",
        })
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlRequest {
    pub id: ControlId,
    pub target: RunControlTarget,
    pub expected_revision: SessionRevision,
    pub action: RunControlAction,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunControlKind {
    Cancel,
    Steer,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunControlStatus {
    Accepted,
    /// The current fenced worker durably acknowledged cancellation.
    Observed,
    Incorporated {
        turn_id: TurnId,
    },
    UnappliedTerminal {
        status: RunStatus,
    },
    /// A terminal runtime transaction committed. Does not assert external death.
    TerminalCommitted {
        status: RunStatus,
    },
}
/// Immutable acceptance evidence. Replaying this grants no worker authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlReceipt {
    pub operation_id: ControlId,
    pub target: RunControlTarget,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
    pub sequence: u64,
    pub kind: RunControlKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlSummary {
    pub receipt: RunControlReceipt,
    pub status: RunControlStatus,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunControlOutcome {
    Applied(RunControlReceipt),
    Replayed(RunControlReceipt),
}
/// Coherent bounded metadata: at most 64 entries and 256 KiB. Input is private.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunControlPage {
    pub revision: SessionRevision,
    pub controls: Vec<RunControlSummary>,
}
/// Result of an atomic worker boundary. No receipt or public poll grants a fence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunControlBoundary {
    pub cancel_observed: bool,
    pub incorporated: usize,
}

/// Coherent host target metadata; no lease, worker token, checkpoint or capsule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlBinding {
    pub revision: SessionRevision,
    pub target: RunControlTarget,
}
/// Progress the original accepted Cancel after rediscovering a quiescent pause.
/// No new intent, resolver claim, run selection or execution authority is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitingCancellation {
    pub target: RunControlTarget,
    pub original_cancel_id: ControlId,
    pub expected_revision: SessionRevision,
}
