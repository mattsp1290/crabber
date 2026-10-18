//! Durable future admissions. Public metadata never carries a dispatch capability.
mod fingerprint;
mod projection;
mod records;
#[cfg(any(feature = "postgres", feature = "sqlite", test))]
pub(crate) use projection::former_encoded_len;
pub(crate) mod source;
mod transition;
use crate::{AdmissionRequestData, BoundKeyedAdmitRequest, StoreError};
use crabber_core::{
    AdmissionOptions, AdmissionReceipt, ControlId, InputEnvelope, InputFingerprint, QueueId,
    SessionId, SessionRevision,
};
pub(crate) use records::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
pub(crate) use transition::*;

/// Private host intent. Defaults resolve at preparation, never enqueue or reclaim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueInput {
    pub request: AdmissionRequestData,
    pub input: InputEnvelope,
    pub admission: AdmissionOptions,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueState {
    Queued,
    Claimed,
    Admitted,
    Cancelled,
}
/// Safe metadata; input, credentials, owner, token and binding are excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSummary {
    pub session_id: SessionId,
    pub queue_id: QueueId,
    pub sequence: u64,
    pub state: QueueState,
    pub prepared: bool,
    pub claim_generation: u64,
    pub admission: Option<AdmissionReceipt>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReceipt {
    pub session_id: SessionId,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub summary: QueueSummary,
}
/// Live authority is returned only by a freshly committed claim, never its receipt.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueAuthority {
    pub session_id: SessionId,
    pub queue_id: QueueId,
    pub generation: u64,
    pub token: String,
}
/// Private bounded retained evidence. Reading it grants no dispatch authority.
#[derive(Clone)]
pub struct RetainedQueue {
    pub summary: QueueSummary,
    pub input: QueueInput,
    pub prepared: Option<BoundKeyedAdmitRequest>,
}
#[derive(Clone, Serialize, Deserialize)]
pub enum QueueAction {
    Enqueue {
        queue_id: QueueId,
        input: Box<QueueInput>,
    },
    Claim {
        owner: String,
        lease: Duration,
    },
    Prepare {
        authority: QueueAuthority,
        prepared: Box<BoundKeyedAdmitRequest>,
    },
    Admit {
        authority: QueueAuthority,
        owner: String,
        lease: Duration,
    },
    Cancel {
        queue_id: QueueId,
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMutation {
    pub session_id: SessionId,
    pub id: ControlId,
    /// Exact latest revision, including for admission of an already prepared item.
    pub expected_revision: SessionRevision,
    pub action: QueueAction,
}
pub enum QueueOutcome {
    Applied {
        receipt: QueueReceipt,
        authority: Option<QueueAuthority>,
        admitted: Option<Box<crate::AdmitOutcome>>,
    },
    Replayed(QueueReceipt),
}
impl QueueOutcome {
    #[must_use]
    pub fn receipt(&self) -> &QueueReceipt {
        match self {
            Self::Applied { receipt, .. } | Self::Replayed(receipt) => receipt,
        }
    }
}
#[derive(Debug, Clone)]
pub struct QueuePageRequest {
    pub session_id: SessionId,
    pub limit: u16,
    pub after_sequence: Option<u64>,
    pub expected_revision: Option<SessionRevision>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuePage {
    pub revision: SessionRevision,
    pub entries: Vec<QueueSummary>,
    pub next_after: Option<u64>,
}
macro_rules! redacted { ($($ty:ty),*) => {$(impl std::fmt::Debug for $ty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(concat!(stringify!($ty), "([redacted])")) }
})*}; }
redacted!(
    QueueInput,
    QueueAuthority,
    RetainedQueue,
    QueueAction,
    QueueMutation,
    QueueOutcome
);
/// Queue controls use the same sanitized native storage errors as admission.
pub(crate) fn invalid() -> StoreError {
    StoreError::Validation("invalid future admission queue request".into())
}
pub(crate) fn corrupt() -> StoreError {
    StoreError::Validation("invalid retained future admission queue evidence".into())
}
