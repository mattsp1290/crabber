//! Per-session dependencies on independently owned physical service generations.
use crate::{CleanupTracker, ResourceDrain, ResourceFailure, ServiceGenerationDescriptor};
use async_trait::async_trait;
use futures::future::BoxFuture;
use std::{future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// Actual task scope retained independently of the acquisition waiter. Returning
/// must include the dependency's sealed acquisition-reference drain. Shared and
/// workspace physical service cleanup belongs to a separate generation owner.
#[derive(Clone)]
pub struct SessionServiceDependencyContext {
    pub cancel: CancellationToken,
    pub cleanup: CleanupTracker,
}

pub type SessionServiceDependencyTask = Box<
    dyn FnOnce(SessionServiceDependencyContext) -> BoxFuture<'static, Result<(), ResourceFailure>>
        + Send,
>;

/// Finite ownership outcomes; none carries private dispatch or storage authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionServiceDependencyError {
    #[error("session service dependency ownership unavailable")]
    Unsupported,
    #[error("session service dependency request rejected")]
    Rejected,
    #[error("session service dependency local capacity reached")]
    Capacity,
    #[error("session service dependency cleanup remains incomplete")]
    Incomplete,
    #[error("session service dependency acknowledgement unknown; retain original identity")]
    UnknownOutcome,
    #[error("session service dependency session was deleted")]
    Deleted,
}

/// Trusted host control of the exact retained dependency. A successful return
/// requires actual main/cleanup joins and acknowledged durable publication.
/// Timeout, panic or uncertain publication must retain the original owner.
#[async_trait]
pub trait SessionServiceDependencyControl: Send + Sync {
    async fn close_and_join(
        &self,
        bound: Duration,
    ) -> Result<ResourceDrain, SessionServiceDependencyError>;
}

/// Cloneable close capability, separate from every plan acquisition lease. This
/// handle must never increment or retain the generation's acquisition counter.
#[derive(Clone)]
pub struct SessionServiceDependencyHandle(Arc<dyn SessionServiceDependencyControl>);
impl SessionServiceDependencyHandle {
    #[must_use]
    pub fn from_host(host: Arc<dyn SessionServiceDependencyControl>) -> Self {
        Self(host)
    }

    /// Seal this session's dependency and join its actual retained tasks.
    /// Closing it never closes a shared or workspace physical generation.
    /// # Errors
    /// Incomplete/unknown outcomes retain the owner and its original identities.
    /// Deleted is finite after the host completes its remaining local joins.
    pub async fn close_and_join(
        &self,
        bound: Duration,
    ) -> Result<ResourceDrain, SessionServiceDependencyError> {
        self.0.close_and_join(bound).await
    }
}
impl std::fmt::Debug for SessionServiceDependencyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionServiceDependencyHandle([redacted])")
    }
}

#[derive(Debug, Clone)]
pub enum SessionServiceDependencyAdmission {
    /// Freshly acknowledged ownership, or an exact already-live local owner.
    /// Only this outcome permits initialization or exposing the generation.
    Acquired(SessionServiceDependencyHandle),
    /// Original metadata only; the supplied task/factory was not dispatched.
    Replayed,
    /// Known refusal before dispatch; the supplied task/factory did not run.
    Cancelled,
    /// Original registration remains uncertain. Retain this control handle and
    /// the same generation; neither initialization nor exposure is permitted.
    /// Retrying must keep the original task/source/identities and cannot turn
    /// a metadata replay into a fresh acquisition.
    Uncertain(SessionServiceDependencyHandle),
}

/// Runtime capability already bound to the exact session and protected original
/// preparation source. The generation descriptor is correlation, never authority.
#[async_trait]
pub trait SessionServiceRegistrar: Send + Sync {
    async fn register(
        &self,
        generation: ServiceGenerationDescriptor,
        task: SessionServiceDependencyTask,
    ) -> Result<SessionServiceDependencyAdmission, SessionServiceDependencyError>;
}

/// Standalone contexts reject registration before invoking the task factory.
/// Clones retain the original host binding; metadata replay cannot recreate it.
#[derive(Clone, Default)]
pub struct SessionServiceDependencies(Option<Arc<dyn SessionServiceRegistrar>>);
impl SessionServiceDependencies {
    #[must_use]
    pub fn from_host(host: Arc<dyn SessionServiceRegistrar>) -> Self {
        Self(Some(host))
    }

    /// Register ownership before service factory invocation or generation exposure.
    /// The host retains the waiting task and cleanup joins on caller drop.
    /// # Errors
    /// Unknown outcomes must be reconciled using the original generation identity.
    pub async fn register<F, Fut>(
        &self,
        generation: ServiceGenerationDescriptor,
        task: F,
    ) -> Result<SessionServiceDependencyAdmission, SessionServiceDependencyError>
    where
        F: FnOnce(SessionServiceDependencyContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ResourceFailure>> + Send + 'static,
    {
        let host = self
            .0
            .as_ref()
            .ok_or(SessionServiceDependencyError::Unsupported)?;
        host.register(generation, Box::new(|context| Box::pin(task(context))))
            .await
    }
}
impl std::fmt::Debug for SessionServiceDependencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionServiceDependencies([redacted])")
    }
}
