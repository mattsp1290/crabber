//! Protected local dispatch and physical join observations. A metadata receipt
//! cannot open a registered gate; storage must freshly acknowledge activation.
pub(crate) mod join;
mod task;
pub use join::{JoinSource, JoinedOwnedScope, OwnedCleanupFailure, OwnedCleanupTask};
pub(crate) mod join_verify;
pub use task::{JoinedOwnedTask, OwnedTaskRegistration};
pub(crate) mod source;
use crate::session_owner::{SessionOwnerError as Error, SessionOwnerTarget};
use crabber_core::{InputFingerprint, SessionId, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};
mod origin;
pub use origin::OwnedScopeSource;

/// Frozen Runtime detached-invocation resource bridge, version 1. Its factory
/// runs behind the registered gate and all owned children join through the
/// exact sealed invocation cleanup scope. This identifier alone proves no join.
pub const RUNTIME_DETACHED_RESOURCE_CONTRACT_V1: &str =
    "f45197b20788989cc382159ed8213ec99262cd540a0503f327938343c545d22e";

/// Exact private parent capability needed for fresh dispatch, never metadata.
/// Linked preparation children must still belong to the live eligible root;
/// detached scopes revalidate their actual native parent immediately at activation.
#[derive(Clone)]
pub enum OwnedScopeParentAuthority {
    ExecutionDriver(Box<super::driver::ExecutionDriverGrant>),
    SuccessorExecutionDriver(Box<super::driver::SuccessorExecutionDriverGrant>),
    Preparation(Box<super::preparation::PreparationGrant>),
    ServicePreparation(Box<super::preparation::PreparationGrant>),
    Native(super::SessionOwnerParentAuthority),
}

impl OwnedScopeParentAuthority {
    #[must_use]
    pub fn preparation(grant: super::preparation::PreparationGrant) -> Self {
        Self::Preparation(Box::new(grant))
    }
}

/// Original private registration identity. This is never dispatch authority.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedScopeActivation {
    pub id: SessionOwnerOperationId,
    pub source: OwnedScopeSource,
    pub nonce: String,
}
impl OwnedScopeActivation {
    pub(crate) fn new(source: OwnedScopeSource) -> Result<Self, Error> {
        super::identifier(&source.session.0, 512, false)?;
        super::identifier(&source.target.id.0, 128, false)?;
        Ok(Self {
            id: key(&source)?,
            source,
            nonce: uuid::Uuid::new_v4().to_string(),
        })
    }
    /// Original stable operation key. Retrying a new registration with the same
    /// key conflicts rather than obtaining another dispatch grant.
    #[must_use]
    pub fn operation_id(&self) -> &SessionOwnerOperationId {
        &self.id
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.source.session
    }
    #[must_use]
    pub fn target(&self) -> &SessionOwnerTarget {
        &self.source.target
    }
    /// # Errors
    /// Refuses malformed or oversized original registration metadata.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        super::identifier(&self.id.0, 128, false)?;
        super::identifier(&self.source.session.0, 512, false)?;
        super::identifier(&self.source.target.id.0, 128, false)?;
        super::identifier(&self.source.reserve.0, 128, false)?;
        super::identifier(&self.source.begin.0, 128, false)?;
        if self.id != key(&self.source)? || uuid::Uuid::parse_str(&self.nonce).is_err() {
            return Err(Error::InvalidRequest);
        }
        super::validation::digest(&("crabber.owned-scope.activation.v1", self))
    }
}

fn key(source: &OwnedScopeSource) -> Result<SessionOwnerOperationId, Error> {
    source::activation_key(&source.session, &source.target)
}

/// Fresh acknowledgement for exactly one pre-registered local dispatch. This
/// cannot be cloned, serialized or constructed from a metadata receipt.
pub struct OwnedDispatchPermit {
    activation: OwnedScopeActivation,
}
impl OwnedDispatchPermit {
    pub(crate) fn applied(activation: OwnedScopeActivation) -> Self {
        Self { activation }
    }
}

/// Main task disposition observed through the actual retained `JoinHandle`.
/// Cancellation is a task result only after its future has actually returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnedMainDisposition {
    NoDispatch,
    Returned,
    Panicked,
    Aborted,
}

#[cfg(test)]
mod tests;

/// Activation acknowledgement. Replay metadata grants no execution authority.
pub enum OwnedScopeActivationOutcome {
    Applied {
        receipt: super::SessionOwnerReceipt,
        permit: Box<OwnedDispatchPermit>,
    },
    Replayed(super::SessionOwnerReceipt),
}
