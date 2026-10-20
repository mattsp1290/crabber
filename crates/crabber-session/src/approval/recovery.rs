//! A new expired-Unstarted ownership operation; receipts never reissue authority.
use super::{ApprovalContinuationClaim, ApprovalError as Error, ApprovalOutcome, ApprovalReceipt};
use crate::{AttemptCleanup, AttemptSnapshot, AttemptState, AttemptTarget};
use crabber_core::{ControlId, InputFingerprint, SessionRevision};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalContinuationRecovery {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub original: ApprovalContinuationClaim,
    pub attempt: AttemptTarget,
    /// Generated fresh owner bookkeeping, excluded from semantic retry identity.
    pub owner: String,
    pub lease_ms: u64,
}
impl ApprovalContinuationRecovery {
    /// # Errors
    /// Rejects malformed identities, invalid leases or unbounded control semantics.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        super::validation::id(&self.id.0)?;
        super::validation::id(&self.attempt.id.0)?;
        let mut bookkeeping = self.original.clone();
        bookkeeping.owner.clone_from(&self.owner);
        bookkeeping.lease_ms = self.lease_ms;
        bookkeeping.fingerprint()?;
        super::validation::fingerprint(&(
            "crabber.approval.reclaim-unstarted.v1",
            &self.original.id,
            self.original.fingerprint()?,
            &self.attempt,
            self.expected_revision,
        ))
    }
    fn verify_target(
        &self,
        continuation: &super::ContinuationTarget,
        attempt: &AttemptSnapshot,
    ) -> Result<(), Error> {
        if *continuation != self.original.continuation
            || attempt.target != self.attempt
            || attempt.session_id != self.original.session_id
            || attempt.run_id != self.original.run_id
            || attempt.state != AttemptState::Unstarted
            || attempt.cleanup != AttemptCleanup::Pending
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
    /// Validate metadata acknowledgments without acquiring execution authority.
    /// # Errors
    /// Rejects a foreign, changed or non-recovery receipt.
    pub fn verify_receipt(&self, receipt: &ApprovalReceipt) -> Result<(), Error> {
        if receipt.operation_id != self.id
            || receipt.session_id != self.original.session_id
            || receipt.run_id != self.original.run_id
            || receipt.semantic_digest != self.fingerprint()?
            || receipt.revision <= self.expected_revision
            || receipt.event_cursor.0 == 0
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let ApprovalOutcome::ReclaimedUnstarted {
            continuation,
            original_claim_id,
            attempt,
        } = &receipt.outcome
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        if *original_claim_id != self.original.id {
            return Err(Error::UnknownStoreOutcome);
        }
        self.verify_target(continuation, attempt)
            .map_err(|_| Error::UnknownStoreOutcome)
    }
}
