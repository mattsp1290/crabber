//! Borrowing a concrete independently owned service is distinct from shutting it down.
mod acquisition;
use crate::{BoundProvider, ProviderBindingError};
use futures::{Stream, future::BoxFuture};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Opaque producer capability. A host must validate an actual supported producer;
/// implementing this trait or supplying metadata establishes no ownership proof.
pub trait ProviderServiceSource: Any + Send + Sync {}
impl dyn ProviderServiceSource {
    /// Pure concrete type inspection; custom sources cannot replace this with
    /// a callback or claim another producer's identity.
    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProviderServiceError {
    #[error("service borrowing unavailable")]
    Unsupported,
    #[error("service borrow rejected")]
    Rejected,
    #[error("service borrow capacity reached")]
    Capacity,
    #[error("service borrow cleanup incomplete")]
    Incomplete,
    #[error("service borrow outcome unknown")]
    UnknownOutcome,
    #[error("service session deleted")]
    Deleted,
}
pub type ProviderBindingFactory =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<BoundProvider, ProviderBindingError>> + Send>;
#[async_trait::async_trait]
pub trait ProviderServiceHost: Send + Sync {
    /// Retain actual acquisition before I/O; invoke the constructor only after
    /// the exact live producer and preparation ingress have been checked.
    /// # Errors
    /// Refuses unknown producers, closed scopes or unresolved native outcomes.
    async fn acquire_binding(
        &self,
        source: Arc<dyn ProviderServiceSource>,
        factory: ProviderBindingFactory,
    ) -> Result<BoundProvider, ProviderBindingError>;
}
#[derive(Clone, Default)]
pub struct ProviderServiceDependencies(Option<Arc<dyn ProviderServiceHost>>);
impl ProviderServiceDependencies {
    #[must_use]
    pub fn from_host(host: Arc<dyn ProviderServiceHost>) -> Self {
        Self(Some(host))
    }
    pub(super) async fn acquire(
        &self,
        source: Arc<dyn ProviderServiceSource>,
        factory: ProviderBindingFactory,
    ) -> Result<BoundProvider, ProviderBindingError> {
        self.0
            .as_ref()
            .ok_or(ProviderServiceError::Unsupported)?
            .acquire_binding(source, factory)
            .await
    }
}
impl super::ProviderResourceContext {
    #[must_use]
    pub fn service_dependencies(&self) -> &ProviderServiceDependencies {
        &self.services
    }
    #[must_use]
    pub fn with_service_dependencies(mut self, services: ProviderServiceDependencies) -> Self {
        self.services = services;
        self
    }
    /// Reserve actual disposal before borrowing or calling the binding factory.
    /// This observes references; physical generation shutdown remains separate.
    /// # Errors
    /// Refuses unsupported/sealed registration before invoking the constructor.
    pub async fn acquire_service_binding<F, W>(
        &self,
        source: Arc<dyn ProviderServiceSource>,
        factory: F,
    ) -> Result<BoundProvider, ProviderBindingError>
    where
        F: FnOnce() -> W + Send + 'static,
        W: Future<Output = Result<BoundProvider, ProviderBindingError>> + Send + 'static,
    {
        self.acquire_binding(|| {
            self.services
                .acquire(source, Box::new(|| Box::pin(factory())))
        })
        .await
    }
}
impl BoundProvider {
    /// Trusted host attachment of an actual producer lease. This method alone
    /// supplies no native grant, physical Join or resource-policy certificate.
    #[must_use]
    pub fn retain_service_lease(mut self, lease: Arc<dyn Any + Send + Sync>) -> Self {
        self.streamer = Arc::new(LeasedStreamer {
            inner: Some(self.streamer),
            lease: Some(lease),
        });
        self
    }
}
struct LeasedStreamer {
    inner: Option<Arc<dyn crate::Streamer>>,
    lease: Option<Arc<dyn Any + Send + Sync>>,
}
impl Drop for LeasedStreamer {
    fn drop(&mut self) {
        super::super::release::dispose(self.inner.take(), &mut self.lease);
    }
}
#[async_trait::async_trait]
impl crate::Streamer for LeasedStreamer {
    async fn stream(
        &self,
        request: crate::ModelRequest,
    ) -> Result<crate::DeltaStream, crate::ProviderError> {
        let (result, lease) = crate::binding::future_release::observe(
            async {
                self.inner
                    .as_ref()
                    .expect("live borrowed provider")
                    .stream(request)
                    .await
            },
            self.lease.as_ref().expect("live borrowed lease").clone(),
        )
        .await;
        let stream = result?;
        Ok(Box::pin(LeasedStream {
            inner: Some(stream),
            lease: Some(lease),
        }))
    }
}
struct LeasedStream {
    inner: Option<crate::DeltaStream>,
    lease: Option<Arc<dyn Any + Send + Sync>>,
}
impl Drop for LeasedStream {
    fn drop(&mut self) {
        super::super::release::dispose(self.inner.take(), &mut self.lease);
    }
}
impl Stream for LeasedStream {
    type Item = crate::StreamDelta;
    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .inner
            .as_mut()
            .expect("live borrowed stream")
            .as_mut()
            .poll_next(context)
    }
}
