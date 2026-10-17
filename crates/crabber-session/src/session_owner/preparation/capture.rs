//! One completion call's actual publisher output, never persisted authority.
use crate::session_owner::SessionOwnerError as Error;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Clones share only this transaction's slot. Every proof construction starts
/// empty; replay/rollback/unknown ACK never exports its selected locator.
#[derive(Clone, Default)]
pub(super) struct ClaimCapture(Arc<AtomicU64>);
impl ClaimCapture {
    pub(super) fn record(&self, sequence: Option<u64>) -> Result<(), Error> {
        let sequence = sequence.ok_or(Error::UnknownStoreOutcome)?;
        if sequence <= 1 || sequence > i64::MAX as u64 {
            return Err(Error::UnknownStoreOutcome);
        }
        self.0
            .compare_exchange(0, sequence, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::SemanticConflict)?;
        Ok(())
    }
    pub(super) fn sequence(&self) -> Option<u64> {
        match self.0.load(Ordering::SeqCst) {
            0 => None,
            value => Some(value),
        }
    }
}

impl super::proof::VerifiedPreparation<'_> {
    pub(crate) fn record_continuation_claim(&self, sequence: Option<u64>) -> Result<(), Error> {
        self.claim_capture.record(sequence)
    }
    pub(crate) fn continuation_claim_locator(
        &self,
    ) -> Result<Option<crate::session_owner::driver::NativeClaimLocator>, Error> {
        self.claim_capture
            .sequence()
            .map(|issuance| {
                let claim = crate::session_owner::driver::NativeClaimLocator {
                    issuance,
                    root_join_id: self.grant.original_reservation().reconciliation()?.join_id,
                };
                claim.validate()?;
                Ok(claim)
            })
            .transpose()
    }
}
