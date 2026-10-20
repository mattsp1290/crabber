//! Independent capacity linked to an authenticated preparation generation.
use super::PreparationGrant;
use crate::session_owner::{
    ReserveSessionOwner, SessionOwnerError as Error, SessionOwnerKind, SessionOwnerReceipt,
    SessionOwnerTarget, digest,
};
use crabber_core::{InputFingerprint, SessionId, SessionOwnerId, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};

/// Positive declaration carried by the immutable original root binding.
pub const OWNED_PREPARATION_COMPONENT: &str = "crabber.runtime.preparation-owned-resources.v1";

/// Immutable source identity, never authority. Only the typed Store operation
/// may attach this link after authenticating the original preparation grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationResourceLink {
    pub session_id: SessionId,
    pub root: SessionOwnerTarget,
    pub root_reserve: SessionOwnerOperationId,
    pub root_begin: SessionOwnerOperationId,
    pub root_digest: InputFingerprint,
}
impl PreparationResourceLink {
    /// Derive the exact immutable link from the retained original grant.
    /// # Errors
    /// Refuses malformed original reservation identity.
    pub fn for_grant(grant: &PreparationGrant) -> Result<Self, Error> {
        let original = grant.original_reservation();
        Ok(Self {
            session_id: original.owner.binding.session_id.clone(),
            root: grant.owner_authority().target().clone(),
            root_reserve: original.owner.id.clone(),
            root_begin: original.reconciliation()?.begin_id,
            root_digest: original.fingerprint()?,
        })
    }
    /// Stable child identities exclude changing intent so changed retries hit
    /// the original key and conflict. These identities grant no authority.
    /// # Errors
    /// Refuses malformed source scope.
    pub fn identities(&self) -> Result<PreparationResourceIds, Error> {
        crate::session_owner::identifier(&self.session_id.0, 512, false)?;
        crate::session_owner::identifier(&self.root.id.0, 128, false)?;
        crate::session_owner::identifier(&self.root_reserve.0, 128, false)?;
        crate::session_owner::identifier(&self.root_begin.0, 128, false)?;
        let hash = digest(&(
            "crabber.preparation.resources.identity.v1",
            &self.session_id,
            &self.root,
        ))?;
        Ok(PreparationResourceIds {
            owner: SessionOwnerId(format!("prep-resource-owner-{}", hash.as_str())),
            reserve: SessionOwnerOperationId(format!("prep-resource-reserve-{}", hash.as_str())),
            begin: SessionOwnerOperationId(format!("prep-resource-begin-{}", hash.as_str())),
            join: SessionOwnerOperationId(format!("prep-resource-join-{}", hash.as_str())),
        })
    }
    /// Canonical immutable reservation domain. A plain owner Reserve cannot
    /// masquerade as a linked resource reservation.
    /// # Errors
    /// Refuses mismatched child routing, kind or original source identity.
    pub fn fingerprint(&self, request: &ReserveSessionOwner) -> Result<InputFingerprint, Error> {
        request.fingerprint()?;
        let ids = self.identities()?;
        if request.id != ids.reserve
            || request.binding.id != ids.owner
            || request.binding.session_id != self.session_id
            || request.binding.kind != SessionOwnerKind::RetainedCapacity
            || request.binding.parent.is_some()
            || request.binding.component_id != "crabber.runtime.preparation-resources.v1"
        {
            return Err(Error::InvalidRequest);
        }
        digest(&(
            "crabber.preparation.resources.reserve.v1",
            &request.id,
            &request.binding,
            self,
        ))
    }
    /// Derive the child reservation without accepting caller-provided labels.
    /// # Errors
    /// Refuses an invalid original preparation grant.
    pub fn reservation(grant: &PreparationGrant) -> Result<ReserveSessionOwner, Error> {
        let link = Self::for_grant(grant)?;
        let ids = link.identities()?;
        let mut request = grant.original_reservation().owner.clone();
        request.id = ids.reserve;
        request.binding.id = ids.owner;
        request.binding.kind = SessionOwnerKind::RetainedCapacity;
        request.binding.component_id = "crabber.runtime.preparation-resources.v1".into();
        request.lease_ms = 30_000;
        link.fingerprint(&request)?;
        Ok(request)
    }
}

/// Safe stable correlation; no child token or dispatch authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparationResourceIds {
    pub owner: SessionOwnerId,
    pub reserve: SessionOwnerOperationId,
    pub begin: SessionOwnerOperationId,
    pub join: SessionOwnerOperationId,
}
/// Original resource reservation and Begin metadata. Replay grants nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparationResourceReceipt {
    pub link: PreparationResourceLink,
    pub reserved: SessionOwnerReceipt,
    pub began: SessionOwnerReceipt,
}
/// Only a fresh typed Store commit can return child authority in a grant.
#[derive(Debug)]
pub enum PreparationResourceOutcome {
    Applied {
        receipt: Box<PreparationResourceReceipt>,
        grant: Box<PreparationGrant>,
    },
    Replayed(Box<PreparationResourceReceipt>),
}
