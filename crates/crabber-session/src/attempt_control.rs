//! Host reconciliation of a stopped/lost parent; no operation grants dispatch.
use crate::{AttemptError, AttemptReceipt, AttemptTarget};
use crabber_core::{ControlId, InputFingerprint, RunId, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptResolverClaim {
    pub resolver_id: String,
    pub authority_claim: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AttemptUncertaintyCause {
    ExpiredOrRevokedOwner,
    HostStopped { claim: AttemptResolverClaim },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptUncertainty {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub target: AttemptTarget,
    pub cause: AttemptUncertaintyCause,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptRetirementOutcome {
    Failed,
    Interrupted,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingCallDisposition {
    RequireNone,
    CancelNeverStarted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptRetirement {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub target: AttemptTarget,
    pub outcome: AttemptRetirementOutcome,
    /// Host assertion that this exact worker and every attempt-owned resource
    /// actually joined. Expiry or an absent local task is not sufficient evidence.
    pub joined: AttemptResolverClaim,
    pub pending: PendingCallDisposition,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptControlOutcome {
    Applied(AttemptReceipt),
    Replayed(AttemptReceipt),
}
impl AttemptUncertainty {
    /// Canonical host outbox semantics. No execution authority is granted.
    /// # Errors
    /// Rejects invalid or unbounded identities/claims.
    pub fn fingerprint(&self) -> Result<InputFingerprint, AttemptError> {
        Control::Uncertain(self.clone()).fingerprint()
    }
}
impl AttemptRetirement {
    /// Canonical host outbox semantics. Generated records are excluded.
    /// # Errors
    /// Rejects invalid or unbounded identities/claims.
    pub fn fingerprint(&self) -> Result<InputFingerprint, AttemptError> {
        Control::Retire(self.clone()).fingerprint()
    }
}
mod records;
pub(crate) use records::*;
mod turn;
pub(crate) use turn::*;
mod readiness;
pub(crate) use readiness::*;

impl std::fmt::Debug for AttemptResolverClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AttemptResolverClaim([redacted])")
    }
}
