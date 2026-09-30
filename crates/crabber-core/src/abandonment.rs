//! Fenced, non-executing settlement of work whose owner has stopped.
use crate::{CoreError, EventRecord, Run, RunFence, ToolCallId};
use serde::{Deserialize, Serialize};

/// Authority to revoke the exact ownership asserted in an abandonment request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbandonAuthority {
    /// The store must check lease expiry using its clock inside the transaction.
    ExpiredLease,
    /// The host has verified that the worker has stopped using authoritative
    /// process or coordinator evidence. The expected fence alone is NOT evidence
    /// of process death. This explicit assertion permits revoking a live lease.
    HostStoppedOwner,
}

/// Always bound to an observed owner and fence, including in expired mode.
/// Reuse the same request after an unknown response; a replacement owner fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbandonRequest {
    pub expected: RunFence,
    pub expected_owner: String,
    pub authority: AbandonAuthority,
}

/// Complete durable settlement. Replay returns the same run snapshot and event.
/// No execution authority is granted, and unrelated inbox input is retained.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AbandonOutcome {
    pub run: Run,
    pub terminal_event: EventRecord,
    pub interrupted_tools: Vec<ToolCallId>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AbandonError {
    #[error("abandonment is unsupported by this store")]
    Unsupported,
    #[error("run not found")]
    NotFound,
    #[error("run has a live lease")]
    LiveLease,
    #[error("expected run ownership is stale")]
    StaleOwner,
    #[error("run was settled by another operation")]
    AlreadyTerminal,
    /// No success may be inferred from an error. Retry the identical request to
    /// reconcile a potentially committed response loss; never resume to retry.
    #[error("abandonment store failure: {0}")]
    Store(#[from] CoreError),
}
