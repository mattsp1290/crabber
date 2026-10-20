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
/// One captured generation for each Shared, Workspace and Session scope.
pub const MAX_SESSION_SERVICES_PER_PREPARATION: usize = 3;

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
    FullySealedFactory,
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
    /// Validate captured identity against an exact immutable session context.
    /// This is a metadata check and never grants service access or dispatch.
    /// # Errors
    /// Rejects mismatched scope, unsupported cleanup policy or oversized metadata.
    pub fn validate_scope(
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
                    && matches!(
                        self.cleanup_policy,
                        SessionServiceCleanupPolicy::SealedFactoryLegacyMount
                            | SessionServiceCleanupPolicy::FullySealedFactory
                    )
            }
            SessionServiceGenerationScope::Session {
                session_id,
                workspace_id,
                directory: root,
            } => {
                session_id == session
                    && workspace_id == workspace
                    && root == directory
                    && matches!(
                        self.cleanup_policy,
                        SessionServiceCleanupPolicy::SealedFactoryLegacyMount
                            | SessionServiceCleanupPolicy::FullySealedFactory
                    )
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

/// The private original preparation capability is revalidated in the same
/// transaction as Reserve/Begin. It is never serialized into a receipt.
#[derive(Clone)]
pub struct RegisterSessionServiceDependency {
    pub grant: PreparationGrant,
    pub generation: SessionServiceGeneration,
}

impl RegisterSessionServiceDependency {
    /// Validate borrowed generation bounds before copying captured metadata.
    /// This derives an original source only and grants no service authority.
    /// # Errors
    /// Rejects unbounded metadata or another immutable session context.
    pub fn source(&self) -> Result<SessionServiceLink, Error> {
        let binding = &self.grant.original_reservation().owner.binding;
        self.generation.validate_scope(
            &binding.session_id,
            &binding.workspace_id,
            &binding.directory,
        )?;
        SessionServiceLink::for_grant(&self.grant, self.generation.clone())
    }
}

/// Current access checks retain the first registration's source and authority.
/// A later preparation may validate access without registering or dispatching a
/// second task, rotating authority, or rewriting the original source.
#[derive(Clone)]
pub struct SessionServiceDependencyAccess {
    pub grant: PreparationGrant,
    pub source: SessionServiceLink,
    pub authority: super::SessionOwnerAuthority,
}

/// Original operation metadata; replay contains no dispatch or owner authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionServiceDependencyReceipt {
    pub source: SessionServiceLink,
    pub reserved: super::SessionOwnerReceipt,
    pub began: super::SessionOwnerReceipt,
}
impl SessionServiceDependencyReceipt {
    /// Verify original metadata only. Adapters additionally authenticate both
    /// immutable audits and the original root before returning this receipt.
    /// # Errors
    /// Rejects changed scope, binding, operation identity or semantic digest.
    pub fn verify(&self) -> Result<(), Error> {
        use super::{
            MutateSessionOwner, SessionOwnerCommand, SessionOwnerOperation, SessionOwnerState,
        };
        let ids = self.source.identities()?;
        let reserved = &self.reserved;
        let began = &self.began;
        let request = ReserveSessionOwner {
            id: ids.reserve.clone(),
            binding: reserved.owner.binding.clone(),
            owner: "metadata".into(),
            lease_ms: 1,
        };
        let command = MutateSessionOwner {
            id: ids.begin.clone(),
            session_id: self.source.root.session_id.clone(),
            target: began.owner.target.clone(),
            command: SessionOwnerCommand::Begin,
        };
        if reserved.session_id != self.source.root.session_id
            || began.session_id != reserved.session_id
            || reserved.operation != SessionOwnerOperation::Reserve
            || began.operation != SessionOwnerOperation::Begin
            || reserved.operation_id != ids.reserve
            || began.operation_id != ids.begin
            || reserved.owner.target != began.owner.target
            || began.owner.target.id != ids.owner
            || began.owner.target.generation != crabber_core::SessionOwnerGeneration::INITIAL
            || reserved.owner.binding != began.owner.binding
            || reserved.owner.created_at != began.owner.created_at
            || reserved.owner.state != SessionOwnerState::ReservedUnstarted
            || began.owner.state != SessionOwnerState::Active
            || reserved.owner.cancellation.is_some()
            || began.owner.cancellation.is_some()
            || reserved.owner.termination.is_some()
            || began.owner.termination.is_some()
            || reserved.semantic_digest != self.source.fingerprint(&request)?
            || began.semantic_digest != command.fingerprint()?
            || crate::snapshot::encoded_bytes(self, 32768).is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum SessionServiceDependencyOutcome {
    Applied {
        receipt: Box<SessionServiceDependencyReceipt>,
        authority: super::SessionOwnerAuthority,
    },
    Replayed(Box<SessionServiceDependencyReceipt>),
}
