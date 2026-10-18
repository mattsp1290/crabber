//! Explicit independent session work, registered before dispatch.
use async_trait::async_trait;
use crabber_core::{InputFingerprint, SessionOwnerId, SessionOwnerOperationId};
use futures::future::BoxFuture;
use std::{future::Future, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Stable retry identity and declared behavior of one detached job.
/// Keep this value to reconcile an unknown acknowledgement. It contains no
/// execution authority, task future, or credential.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInvocationRequest {
    pub id: SessionOwnerId,
    pub reserve_id: SessionOwnerOperationId,
    pub begin_id: SessionOwnerOperationId,
    pub join_id: SessionOwnerOperationId,
    pub component_id: String,
    pub behavior_fingerprint: InputFingerprint,
}
impl SessionInvocationRequest {
    #[must_use]
    pub fn new(component_id: String, behavior_fingerprint: InputFingerprint) -> Self {
        Self {
            id: SessionOwnerId::new(),
            reserve_id: SessionOwnerOperationId::new(),
            begin_id: SessionOwnerOperationId::new(),
            join_id: SessionOwnerOperationId::new(),
            component_id,
            behavior_fingerprint,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionInvocationError {
    #[error("session invocation ownership unavailable")]
    Unsupported,
    #[error("session invocation request rejected")]
    Rejected,
    #[error("session invocation local capacity reached")]
    Capacity,
    #[error("session invocation acknowledgement unknown; reconcile original identity")]
    UnknownOutcome,
}
/// Cancellation is independent of the parent call/run. Include every retained
/// resource in this cleanup scope before the main task returns.
#[derive(Clone)]
pub struct SessionInvocationContext {
    pub cancel: CancellationToken,
    pub cleanup: crate::CleanupTracker,
    pub public_source: crate::PublicSourceWriter,
    /// Bound nested creation under this actual independently active invocation.
    pub invocations: SessionInvocations,
}
pub type SessionInvocationTask = Box<
    dyn FnOnce(SessionInvocationContext) -> BoxFuture<'static, Result<(), crate::ResourceFailure>>
        + Send,
>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionInvocationAdmission {
    /// Acknowledged Reserve and Begin; the host now owns this job independently.
    Started,
    /// Original metadata only. The supplied task was not dispatched.
    Replayed,
}
/// Trusted runtime capability bound to one actual native parent phase. Custom
/// hosts must retain registration, dispatch and join ownership on caller drop.
#[async_trait]
pub trait SessionInvocationSpawner: Send + Sync {
    async fn spawn(
        &self,
        request: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<SessionInvocationAdmission, SessionInvocationError>;
}
/// Standalone contexts reject registration. Clones carry the exact original
/// parent capability; they cannot borrow another run's ownership.
#[derive(Clone, Default)]
pub struct SessionInvocations(Option<Arc<dyn SessionInvocationSpawner>>);
impl SessionInvocations {
    #[must_use]
    pub fn from_host(host: Arc<dyn SessionInvocationSpawner>) -> Self {
        Self(Some(host))
    }
    /// Register and begin an independent job before invoking its task factory.
    /// Parent cancellation/completion never cancels this job. No task factory
    /// is called for replay, rejected scope or an unknown Begin acknowledgement.
    /// # Errors
    /// Retain the original request on every unknown acknowledgement.
    pub async fn spawn<F, Fut>(
        &self,
        request: SessionInvocationRequest,
        task: F,
    ) -> Result<SessionInvocationAdmission, SessionInvocationError>
    where
        F: FnOnce(SessionInvocationContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), crate::ResourceFailure>> + Send + 'static,
    {
        let host = self.0.as_ref().ok_or(SessionInvocationError::Unsupported)?;
        host.spawn(request, Box::new(|context| Box::pin(task(context))))
            .await
    }
}
impl std::fmt::Debug for SessionInvocations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionInvocations([redacted])")
    }
}
