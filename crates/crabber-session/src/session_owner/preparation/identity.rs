//! Stable identities are derived before defaults, factories or current CAS reads.
use super::{PreparationKey, PreparationReconciliation};
use crate::session_owner::{SessionOwnerError as Error, digest};
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};

pub(crate) fn ids(key: &PreparationKey, session: &SessionId) -> Result<Ids, Error> {
    match key {
        PreparationKey::Admission(key) => {
            crate::session_owner::identifier(key.as_str(), 512, false)?;
        }
        PreparationKey::Unkeyed(id)
        | PreparationKey::Queue(id)
        | PreparationKey::Approval(id)
        | PreparationKey::ToolContinuation(id) => {
            crate::session_owner::identifier(&id.0, 128, false)?;
        }
    }
    let digest = digest(&("crabber.preparation.identity.v1", session, key))?;
    let identity = digest.as_str();
    Ok(Ids {
        owner: SessionOwnerId(format!("prep-owner-{identity}")),
        reserve: SessionOwnerOperationId(format!("prep-reserve-{identity}")),
        begin: SessionOwnerOperationId(format!("prep-begin-{identity}")),
        join: SessionOwnerOperationId(format!("prep-join-{identity}")),
    })
}

pub(crate) struct Ids {
    pub owner: SessionOwnerId,
    pub reserve: SessionOwnerOperationId,
    pub begin: SessionOwnerOperationId,
    pub join: SessionOwnerOperationId,
}

pub(crate) fn metadata(
    audit: &crate::session_owner::audit::Audit,
    session: &SessionId,
    key: &PreparationKey,
) -> Result<PreparationReconciliation, Error> {
    crate::session_owner::audit::verify_current(&audit.record, audit, audit)?;
    let origin = audit
        .record
        .preparation
        .as_ref()
        .ok_or(Error::SemanticConflict)?;
    if audit.receipt.session_id != *session || origin.key != *key {
        return Err(Error::SemanticConflict);
    }
    let reservation = super::ReservePreparation {
        owner: crate::session_owner::ReserveSessionOwner {
            id: origin.operation.clone(),
            binding: audit.record.snapshot.binding.clone(),
            owner: audit.record.owner.clone(),
            lease_ms: 1,
        },
        key: origin.key.clone(),
        source: None,
        expected_revision: origin.expected_revision,
        purpose: origin.purpose.clone(),
        intent: origin.intent.clone(),
    };
    reservation.reconciliation()
}
