//! Exact session dependencies on independently owned physical service generations.
#[cfg(test)]
#[path = "service/tests.rs"]
mod tests;
use super::{
    ReserveSessionOwner, SessionOwnerError as Error, SessionOwnerKind, digest, identifier,
    preparation::{PreparationGrant, PreparationResourceLink},
};
use crabber_core::{InputFingerprint, SessionId, SessionOwnerId, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};

/// This bridge owns only the session dependency and its actual joins. The
/// physical generation's cleanup policy does not confer whole-service coverage.
pub const RUNTIME_SESSION_SERVICE_COMPONENT: &str = "crabber.runtime.session-service-dependency.v1";

fn resource_contract() -> Result<InputFingerprint, Error> {
    digest(&(
        RUNTIME_SESSION_SERVICE_COMPONENT,
        "dependency-only-owned-joins",
        1u32,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", deny_unknown_fields)]
pub enum SessionServiceGenerationScope {
    Shared,
    Workspace {
        workspace_id: String,
        directory: String,
    },
    Session {
        session_id: SessionId,
        workspace_id: String,
        directory: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionServiceCleanupPolicy {
    LegacySharedMount,
    SealedFactoryLegacyMount,
}

/// Bounded captured generation metadata; correlation never grants dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionServiceGeneration {
    pub id: String,
    pub scope: SessionServiceGenerationScope,
    pub registration_fingerprint: InputFingerprint,
    pub cleanup_policy: SessionServiceCleanupPolicy,
}
impl SessionServiceGeneration {
    pub(crate) fn validate_scope(
        &self,
        session: &SessionId,
        workspace: &str,
        directory: &str,
    ) -> Result<(), Error> {
        identifier(&self.id, 128, false)?;
        let matches = match &self.scope {
            SessionServiceGenerationScope::Shared => {
                self.cleanup_policy == SessionServiceCleanupPolicy::LegacySharedMount
            }
            SessionServiceGenerationScope::Workspace {
                workspace_id,
                directory: root,
            } => {
                !workspace_id.is_empty()
                    && workspace_id == workspace
                    && root == directory
                    && self.cleanup_policy == SessionServiceCleanupPolicy::SealedFactoryLegacyMount
            }
            SessionServiceGenerationScope::Session {
                session_id,
                workspace_id,
                directory: root,
            } => {
                session_id == session
                    && workspace_id == workspace
                    && root == directory
                    && self.cleanup_policy == SessionServiceCleanupPolicy::SealedFactoryLegacyMount
            }
        };
        if !matches || crate::snapshot::encoded_bytes(self, 8192).is_none() {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
}

/// Attached only by the typed transaction after authenticating the original
/// preparation grant. A plain owner Reserve cannot invent this association.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionServiceLink {
    pub root: PreparationResourceLink,
    pub generation: SessionServiceGeneration,
}
impl SessionServiceLink {
    /// Capture the immutable original root and generation before any dispatch.
    /// # Errors
    /// Refuses generation scope drift or malformed original source identities.
    pub fn for_grant(
        grant: &PreparationGrant,
        generation: SessionServiceGeneration,
    ) -> Result<Self, Error> {
        let binding = &grant.original_reservation().owner.binding;
        if grant.original_reservation().resource_contract.is_none()
            || grant.retained_resources_authority().is_none()
        {
            return Err(Error::Unsupported);
        }
        generation.validate_scope(
            &binding.session_id,
            &binding.workspace_id,
            &binding.directory,
        )?;
        Ok(Self {
            root: PreparationResourceLink::for_grant(grant)?,
            generation,
        })
    }

    /// Stable retry identities omit changing intent and the preparation root.
    /// An uncertain original registration cannot be rebound to a new root.
    /// # Errors
    /// Refuses malformed routing identities.
    pub fn identities(&self) -> Result<SessionServiceIds, Error> {
        identifier(&self.root.session_id.0, 512, false)?;
        identifier(&self.generation.id, 128, false)?;
        let hash = digest(&(
            "crabber.session-service.identity.v1",
            &self.root.session_id,
            &self.generation.id,
        ))?;
        Ok(SessionServiceIds {
            owner: SessionOwnerId(format!("service-owner-{}", hash.as_str())),
            reserve: SessionOwnerOperationId(format!("service-reserve-{}", hash.as_str())),
            begin: SessionOwnerOperationId(format!("service-begin-{}", hash.as_str())),
            join: SessionOwnerOperationId(format!("service-join-{}", hash.as_str())),
        })
    }

    /// Domain-separated original reservation, including the exact root source.
    /// # Errors
    /// Refuses changed routing, kind, scope or source identities.
    pub fn fingerprint(&self, request: &ReserveSessionOwner) -> Result<InputFingerprint, Error> {
        request.fingerprint()?;
        self.root.identities()?;
        self.generation.validate_scope(
            &request.binding.session_id,
            &request.binding.workspace_id,
            &request.binding.directory,
        )?;
        let ids = self.identities()?;
        if request.id != ids.reserve
            || request.binding.id != ids.owner
            || request.binding.session_id != self.root.session_id
            || request.binding.kind != SessionOwnerKind::SessionService
            || request.binding.component_id != RUNTIME_SESSION_SERVICE_COMPONENT
            || request.binding.parent.is_some()
            || request.binding.resource_contract.as_ref() != Some(&resource_contract()?)
            || request.binding.behavior_fingerprint
                != digest(&(
                    "crabber.runtime.session-service-dependency.v1",
                    &self.generation,
                ))?
        {
            return Err(Error::InvalidRequest);
        }
        digest(&(
            "crabber.session-service.reserve.v1",
            &request.id,
            &request.binding,
            self,
        ))
    }

    /// Derive metadata for the exact original reservation. This grants no
    /// authority and cannot attach the link through the generic Reserve API.
    /// # Errors
    /// Refuses another preparation source or invalid captured generation.
    pub fn reservation(&self, grant: &PreparationGrant) -> Result<ReserveSessionOwner, Error> {
        if self.root != PreparationResourceLink::for_grant(grant)? {
            return Err(Error::InvalidRequest);
        }
        let ids = self.identities()?;
        let mut request = grant.original_reservation().owner.clone();
        request.id = ids.reserve;
        request.binding.id = ids.owner;
        request.binding.kind = SessionOwnerKind::SessionService;
        request.binding.component_id = RUNTIME_SESSION_SERVICE_COMPONENT.into();
        request.binding.resource_contract = Some(resource_contract()?);
        request.binding.behavior_fingerprint = digest(&(
            "crabber.runtime.session-service-dependency.v1",
            &self.generation,
        ))?;
        request.lease_ms = 30_000;
        self.fingerprint(&request)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionServiceIds {
    pub owner: SessionOwnerId,
    pub reserve: SessionOwnerOperationId,
    pub begin: SessionOwnerOperationId,
    pub join: SessionOwnerOperationId,
}
