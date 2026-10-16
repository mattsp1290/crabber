//! Per-preparation resources, distinct from shared service lifetimes and cleanup proof.
use crate::{
    ComponentIdentity, ExtensionError, ResourceFailure, ResourceRegistrationError, ToolResources,
};
use crabber_core::{InputFingerprint, SessionContext};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{future::Future, sync::Arc};
use tokio_util::sync::CancellationToken;

pub const PREPARATION_RESOURCE_CONTRACT_VERSION: u32 = 1;

/// A pure declared bridge identity. This is configuration, never cleanup evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreparationResourceContract {
    protocol: u32,
    component: ComponentIdentity,
    config: InputFingerprint,
}
impl PreparationResourceContract {
    /// Declare every input that changes the resource ownership/cleanup policy.
    /// # Errors
    /// Refuses empty, oversized or control-bearing component identities.
    pub fn new(
        component: ComponentIdentity,
        config: InputFingerprint,
    ) -> Result<Self, ExtensionError> {
        if component.id.is_empty()
            || component.id.len() > 512
            || component.version.is_empty()
            || component.version.len() > 128
            || component
                .id
                .chars()
                .chain(component.version.chars())
                .any(char::is_control)
        {
            return Err(ExtensionError::PreparationResourceContractInvalid);
        }
        Ok(Self {
            protocol: PREPARATION_RESOURCE_CONTRACT_VERSION,
            component,
            config,
        })
    }
    /// Stable declaration hash included in both the original reservation and
    /// actual plan/runtime semantics. Credentials never belong in this value.
    /// # Panics
    /// Only if serialization of the validated declaration or SHA256 fails.
    #[must_use]
    pub fn fingerprint(&self) -> InputFingerprint {
        let mut value =
            serde_json::to_value(self).expect("bounded resource declaration serializes");
        value.sort_all_objects();
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(value.to_string().as_bytes())
        ))
        .expect("SHA256 is a fingerprint")
    }
}

/// Registration scope supplied only after durable child capacity is acquired.
/// Providers declaring a bridge must register cleanup before creating resources.
/// Shared services need independent per-session dependencies; closing this scope
/// must never shut down services belonging to another session.
#[derive(Clone)]
pub struct PreparationResourceContext {
    session: SessionContext,
    resources: ToolResources,
}
impl PreparationResourceContext {
    /// Trusted host bridge. This confers registration, not storage authority.
    /// # Errors
    /// Refuses a detached resource handle without an actual join owner.
    pub fn from_host(
        session: SessionContext,
        resources: ToolResources,
    ) -> Result<Self, ResourceRegistrationError> {
        if resources.closing().is_none() {
            return Err(ResourceRegistrationError::Unsupported);
        }
        Ok(Self { session, resources })
    }
    #[must_use]
    pub fn session(&self) -> &SessionContext {
        &self.session
    }
    #[must_use]
    pub fn cleanup(&self) -> PreparationCleanup {
        PreparationCleanup(self.resources.clone())
    }
    /// Actual cooperative scope-close signal; observation never proves cleanup.
    pub async fn closing(&self) {
        if let Some(closing) = self.resources.closing() {
            closing.await;
        }
    }
}

/// Bounded registration whose owner retains actual joins, including on panic.
#[derive(Clone)]
pub struct PreparationCleanup(ToolResources);
impl PreparationCleanup {
    /// Register and start owned work. Future registration is sealed at cleanup.
    /// # Errors
    /// Refuses registration after closing or at the resource-task capacity.
    pub fn spawn(
        &self,
        work: impl Future<Output = Result<(), ResourceFailure>> + Send + 'static,
    ) -> Result<(), ResourceRegistrationError> {
        self.0.spawn(work)
    }
    /// Register cleanup before its associated effect. The action is invoked
    /// once, inside a retained task, when the scope closes. Include all nested
    /// joins in the returned future rather than registering after scope closure.
    /// # Errors
    /// Refuses registration before invoking the action when sealed or full.
    pub fn defer<F, W>(&self, cleanup: F) -> Result<(), ResourceRegistrationError>
    where
        F: FnOnce() -> W + Send + 'static,
        W: Future<Output = Result<(), ResourceFailure>> + Send + 'static,
    {
        let closed = self
            .0
            .closing()
            .ok_or(ResourceRegistrationError::Unsupported)?;
        self.0.spawn(async move {
            closed.await;
            cleanup().await
        })
    }
}

/// A plan lifetime signal. Its last disposal only notifies the supervisor; it
/// neither closes resources nor records a successful join in storage.
#[derive(Clone)]
pub struct PreparationResourceLease(Arc<LeaseSignal>);
struct LeaseSignal(CancellationToken);
impl PreparationResourceLease {
    #[must_use]
    pub fn from_host(disposed: CancellationToken) -> Self {
        Self(Arc::new(LeaseSignal(disposed)))
    }
    /// Whether two retained handles refer to the same disposal signal. This
    /// compares local ownership only and proves no physical resource join.
    #[must_use]
    pub fn same_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Drop for LeaseSignal {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(test)]
#[path = "preparation_resources/tests.rs"]
mod tests;
