//! Shared immutable owner receipts and current-proof verification.
use crate::session_owner::{
    ReserveSessionOwner, SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerReceipt,
    SessionOwnerState, record::Record,
};
use crabber_core::SessionRevision;
use serde::{Deserialize, Serialize};

/// Session presence is part of revision provenance: an absent session must not
/// silently become an existing session at the same numeric revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RevisionState {
    Absent(u64),
    Present(SessionRevision),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publication {
    pub before: RevisionState,
    pub after: RevisionState,
    pub public_change: bool,
}
impl Publication {
    pub fn new(before: RevisionState, public_change: bool) -> Result<Self, Error> {
        let after = match (before, public_change) {
            (RevisionState::Absent(clock), true) => {
                RevisionState::Absent(clock.checked_add(1).ok_or(Error::UnknownStoreOutcome)?)
            }
            (RevisionState::Present(revision), true) => {
                RevisionState::Present(revision.next().map_err(|_| Error::UnknownStoreOutcome)?)
            }
            _ => before,
        };
        Ok(Self {
            before,
            after,
            public_change,
        })
    }
    fn validate(self) -> Result<(), Error> {
        if Self::new(self.before, self.public_change)? != self {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Audit {
    pub version: u32,
    pub receipt: SessionOwnerReceipt,
    pub record: Record,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<Publication>,
}
impl Audit {
    pub fn validate(&self) -> Result<(), Error> {
        self.record.validate()?;
        crate::session_owner::identifier(&self.receipt.operation_id.0, 128, false)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if !matches!(
            (self.version, self.record.version, self.publication),
            (1, 1, None) | (2 | 3, 2, Some(_))
        ) || self.receipt.owner != self.record.snapshot
            || self.receipt.session_id != self.record.snapshot.binding.session_id
            || (self.receipt.operation != SessionOwnerOperation::Reserve
                && self.record.sequence < 2)
            || crate::snapshot::encoded_bytes(self, 32768).is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(publication) = self.publication {
            publication.validate()?;
            if publication.public_change != (self.receipt.operation != SessionOwnerOperation::Renew)
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        let valid = match self.receipt.operation {
            SessionOwnerOperation::ActivateScope => {
                self.record.owned_scope.as_ref().is_some_and(|scope| {
                    scope.id == self.receipt.operation_id
                        && scope.fingerprint().ok().as_ref() == Some(&self.receipt.semantic_digest)
                        && self.record.snapshot.state == SessionOwnerState::Active
                })
            }
            SessionOwnerOperation::RequestCancel | SessionOwnerOperation::ObserveCancel => {
                crate::session_owner::cancellation::proof::operation(self)?;
                true
            }
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
            SessionOwnerOperation::Join => {
                self.record.snapshot.state == SessionOwnerState::Joined
                    && super::preparation::handoff::record_fingerprint(
                        &self.record,
                        &self.receipt.operation_id,
                    )? == self.receipt.semantic_digest
            }
            SessionOwnerOperation::ResolveJoined => {
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
        if self.record.owned_join.is_some() && self.receipt.operation != SessionOwnerOperation::Join
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if self.record.retained_resources.is_some()
            && self.receipt.operation != SessionOwnerOperation::Join
        {
            return Err(Error::UnknownStoreOutcome);
        }
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
        || origin.record.preparation != record.preparation
        || origin.record.resource != record.resource
        || origin.record.service != record.service
        || origin.record.driver != record.driver
        || origin.record.successor_driver != record.successor_driver
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
    let digest = if let Some(preparation) = &record.preparation {
        preparation.fingerprint(&request)?
    } else if let Some(resource) = &record.resource {
        resource.fingerprint(&request)?
    } else if let Some(driver) = &record.successor_driver {
        driver.fingerprint(&request)?
    } else if let Some(driver) = &record.driver {
        driver.fingerprint(&request)?
    } else if let Some(service) = &record.service {
        service.fingerprint(&request)?
    } else {
        request.fingerprint()?
    };
    if digest != origin.receipt.semantic_digest {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
