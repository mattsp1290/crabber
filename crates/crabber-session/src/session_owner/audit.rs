//! Shared immutable owner receipts and current-proof verification.
use crate::session_owner::{
    ReserveSessionOwner, SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerReceipt,
    SessionOwnerState, record::Record,
};
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Audit {
    pub version: u32,
    pub receipt: SessionOwnerReceipt,
    pub record: Record,
}
impl Audit {
    pub fn validate(&self) -> Result<(), Error> {
        self.record.validate()?;
        crate::session_owner::identifier(&self.receipt.operation_id.0, 128, false)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if self.version != 1
            || self.receipt.owner != self.record.snapshot
            || self.receipt.session_id != self.record.snapshot.binding.session_id
            || (self.receipt.operation != SessionOwnerOperation::Reserve
                && self.record.sequence < 2)
            || crate::snapshot::encoded_bytes(self, 32768).is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let valid = match self.receipt.operation {
            SessionOwnerOperation::Reserve => {
                self.record.sequence == 1
                    && self.record.snapshot.state == SessionOwnerState::ReservedUnstarted
                    && self.record.snapshot.target.generation
                        == crabber_core::SessionOwnerGeneration::INITIAL
                    && self.record.origin == self.receipt.operation_id
            }
            SessionOwnerOperation::Begin => self.record.snapshot.state == SessionOwnerState::Active,
            SessionOwnerOperation::Renew => matches!(
                self.record.snapshot.state,
                SessionOwnerState::Active | SessionOwnerState::ReservedUnstarted
            ),
            SessionOwnerOperation::Join | SessionOwnerOperation::ResolveJoined => {
                self.record.snapshot.state == SessionOwnerState::Joined
            }
            SessionOwnerOperation::MarkUncertain => {
                self.record.snapshot.state == SessionOwnerState::Uncertain
            }
            SessionOwnerOperation::ReclaimUnstarted => {
                self.record.snapshot.state == SessionOwnerState::ReservedUnstarted
                    && self.record.snapshot.target.generation
                        > crabber_core::SessionOwnerGeneration::INITIAL
            }
        };
        if !valid {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
/// Verify only the requested current owner against independently selected sources.
pub(crate) fn verify_current(record: &Record, origin: &Audit, latest: &Audit) -> Result<(), Error> {
    record.validate()?;
    origin.validate()?;
    latest.validate()?;
    if latest.record != *record
        || origin.receipt.operation != SessionOwnerOperation::Reserve
        || origin.receipt.operation_id != record.origin
        || origin.record.parent != record.parent
        || origin.record.snapshot.binding != record.snapshot.binding
        || origin.record.snapshot.created_at != record.snapshot.created_at
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let request = ReserveSessionOwner {
        id: record.origin.clone(),
        binding: record.snapshot.binding.clone(),
        owner: origin.record.owner.clone(),
        lease_ms: 1,
    };
    if request.fingerprint()? != origin.receipt.semantic_digest {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
