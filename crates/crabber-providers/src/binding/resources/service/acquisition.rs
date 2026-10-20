//! Dispose the real binding factory future before releasing its service lease.
use crate::{BoundProvider, ProviderBindingError, ProviderReleaseHolds};
use std::{any::Any, future::Future, sync::Arc};
type Lease = Arc<dyn Any + Send + Sync>;
impl BoundProvider {
    /// Trusted host acquisition with a genuine independently borrowed lease.
    /// The actual constructor/future is disposed before reference release. This
    /// helper supplies no physical shutdown or native ownership certificate.
    /// # Errors
    /// Preserves the factory's binding failure after actual future disposal.
    pub async fn acquire_service_reference<F, W>(
        lease: Lease,
        factory: F,
    ) -> Result<Self, ProviderBindingError>
    where
        F: FnOnce() -> W + Send + 'static,
        W: Future<Output = Result<Self, ProviderBindingError>> + Send + 'static,
    {
        let holds = ProviderReleaseHolds::capture_with_original(lease.clone());
        let (result, lease) = holds
            .scope(|| {
                crate::binding::future_release::observe(async move { factory().await }, lease)
            })
            .await;
        result.map(|binding| binding.retain_service_lease(lease))
    }
}
