//! Explicit host lifecycle assertions, independent of worker leases.
use crate::{
    PhaseCommand, PhaseError, PhaseMutation, PhaseQuiescenceResult, PhaseReceipt,
    PhaseResolverClaim, PhaseSnapshot, PhaseTarget,
};
use crabber_core::{ControlId, InputFingerprint, PhaseId, RunId, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Expiry revokes authority; it does not prove that external effects stopped.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PhaseUncertaintyCause {
    /// Checked using the Store clock or revocation of the original parent token.
    ExpiredOrRevokedOwner,
    /// An authenticated host asserts that this exact owner stopped.
    HostStopped { claim: PhaseResolverClaim },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseUncertainty {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub target: PhaseTarget,
    pub cause: PhaseUncertaintyCause,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseCleanup {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub target: PhaseTarget,
    pub result: PhaseQuiescenceResult,
    /// Host evidence about the exact resources; metadata is not authentication.
    pub claim: PhaseResolverClaim,
}
/// Receipts carry no worker fence, native token, or dispatch capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhaseControlOutcome {
    Applied(PhaseReceipt),
    Replayed(PhaseReceipt),
}
impl PhaseUncertainty {
    /// Return the normalized transition descriptor for a trusted Store adapter.
    /// This metadata grants no worker authority.
    #[must_use]
    pub fn mutation(&self) -> PhaseMutation {
        PhaseMutation {
            id: self.id.clone(),
            expected_revision: self.expected_revision,
            command: match &self.cause {
                PhaseUncertaintyCause::ExpiredOrRevokedOwner => {
                    PhaseCommand::MarkExpiredUncertain {
                        target: self.target.clone(),
                    }
                }
                PhaseUncertaintyCause::HostStopped { claim } => {
                    PhaseCommand::MarkStoppedUncertain {
                        target: self.target.clone(),
                        claim: claim.clone(),
                    }
                }
            },
        }
    }
    /// Stable semantics for receipt reconciliation.
    /// # Errors
    /// Rejects unbounded or invalid identifiers and claims.
    pub fn fingerprint(&self) -> Result<InputFingerprint, PhaseError> {
        digest(
            "crabber.phase-uncertainty.v1",
            &self.session_id,
            &self.run_id,
            &self.mutation(),
        )
    }
}
impl PhaseCleanup {
    /// Return the normalized transition descriptor for a trusted Store adapter.
    /// This metadata grants no worker authority.
    #[must_use]
    pub fn mutation(&self) -> PhaseMutation {
        PhaseMutation {
            id: self.id.clone(),
            expected_revision: self.expected_revision,
            command: PhaseCommand::RecordQuiescence {
                target: self.target.clone(),
                result: self.result,
                host_claim: Some(self.claim.clone()),
            },
        }
    }
    /// Stable semantics for receipt reconciliation.
    /// # Errors
    /// Rejects unbounded or invalid identifiers and claims.
    pub fn fingerprint(&self) -> Result<InputFingerprint, PhaseError> {
        digest(
            "crabber.phase-cleanup.v1",
            &self.session_id,
            &self.run_id,
            &self.mutation(),
        )
    }
}
fn digest(
    domain: &str,
    session: &SessionId,
    run: &RunId,
    request: &PhaseMutation,
) -> Result<InputFingerprint, PhaseError> {
    let inner = request.fingerprint(session, run)?;
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(format!("{domain}:{}", inner.as_str()).as_bytes())
    ))
    .map_err(|_| PhaseError::InvalidRequest)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhasePageRequest {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub limit: u8,
    pub after: Option<PhaseId>,
    /// Required for continuation; a stale snapshot is rejected.
    pub expected_revision: Option<SessionRevision>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhasePage {
    pub revision: SessionRevision,
    pub phases: Vec<PhaseSnapshot>,
    pub next_after: Option<PhaseId>,
}
impl PhasePageRequest {
    /// Validate bounds before a trusted adapter performs its coherent read.
    /// # Errors
    /// Rejects invalid bounds, identifiers, or a continuation without revision.
    pub fn validate(&self) -> Result<(), PhaseError> {
        if !(1..=64).contains(&self.limit)
            || self.after.is_some() && self.expected_revision.is_none()
        {
            return Err(PhaseError::InvalidRequest);
        }
        for (id, limit) in [(&self.session_id.0, 512), (&self.run_id.0, 128)] {
            if id.is_empty() || id.len() > limit || id.chars().any(char::is_control) {
                return Err(PhaseError::InvalidRequest);
            }
        }
        if let Some(after) = &self.after {
            crate::attempt::validate_identifier(&after.0)
                .map_err(|_| PhaseError::InvalidRequest)?;
        }
        Ok(())
    }
    /// Validate bounded metadata and form the page under the adapter snapshot.
    /// # Errors
    /// Rejects stale revision, scope, ordering, corrupt metadata or excess bytes.
    pub fn finish(
        &self,
        revision: SessionRevision,
        mut phases: Vec<PhaseSnapshot>,
    ) -> Result<PhasePage, PhaseError> {
        if self
            .expected_revision
            .is_some_and(|expected| expected != revision)
        {
            return Err(PhaseError::RevisionConflict);
        }
        let mut bytes = 0usize;
        let mut previous = self.after.as_ref();
        for phase in &phases {
            crate::phase::validate_snapshot(phase)?;
            if phase.session_id != self.session_id
                || phase.run_id != self.run_id
                || previous.is_some_and(|id| id.0.as_bytes() >= phase.target.id.0.as_bytes())
            {
                return Err(PhaseError::UnknownStoreOutcome);
            }
            previous = Some(&phase.target.id);
            bytes = bytes
                .checked_add(
                    serde_json::to_vec(phase)
                        .map_err(|_| PhaseError::UnknownStoreOutcome)?
                        .len(),
                )
                .ok_or(PhaseError::UnknownStoreOutcome)?;
        }
        if phases.len() > usize::from(self.limit) + 1 || bytes > 256 * 1024 {
            return Err(PhaseError::UnknownStoreOutcome);
        }
        let next_after = if phases.len() > usize::from(self.limit) {
            phases.pop();
            phases.last().map(|phase| phase.target.id.clone())
        } else {
            None
        };
        Ok(PhasePage {
            revision,
            phases,
            next_after,
        })
    }
}
