//! Explicit recovery is limited to a positively Unstarted reserved continuation.
use super::{
    ToolContinuationClaim, ToolContinuationError as Error, ToolContinuationOutcome,
    ToolContinuationReceipt,
};
use crate::AttemptTarget;
use crabber_core::{ControlId, InputFingerprint, SessionRevision};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolContinuationRecovery {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub original: ToolContinuationClaim,
    pub attempt: AttemptTarget,
    pub owner: String,
    pub lease_ms: u64,
}
impl ToolContinuationRecovery {
    /// # Errors
    /// Rejects invalid recovery identity, original semantics or lease bounds.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        super::canonical::identity(&self.id.0, 128)?;
        super::canonical::identity(&self.owner, 512)?;
        super::canonical::identity(&self.attempt.id.0, 128)?;
        self.original.fingerprint()?;
        if self.lease_ms == 0 || self.lease_ms > 900_000 {
            return Err(Error::InvalidRequest);
        }
        super::canonical::digest(&(
            "crabber.tool-continuation.recover-unstarted.v1",
            &self.id,
            self.expected_revision,
            &self.original.id,
            self.original.fingerprint()?,
            &self.attempt,
        ))
    }
    /// Check the retained original recovery request against metadata only.
    /// # Errors
    /// Rejects mismatched scope, original claim, target or changed retry choices.
    pub fn verify_receipt(&self, receipt: &ToolContinuationReceipt) -> Result<(), Error> {
        if receipt.session_id != self.original.session_id
            || receipt.run_id != self.original.run_id
            || receipt.operation_id != self.id
            || receipt.semantic_digest != self.fingerprint()?
            || !matches!(&receipt.outcome,ToolContinuationOutcome::ReclaimedUnstarted{continuation,original_claim_id,attempt} if continuation==&self.original.continuation && original_claim_id==&self.original.id && attempt.target==self.attempt)
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}
