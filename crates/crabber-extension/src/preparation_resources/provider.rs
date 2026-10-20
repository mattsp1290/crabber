//! Provider registration uses the same actual sealed preparation owner.
use super::PreparationResourceContext;
use crabber_providers::{
    ProviderResourceContext, ProviderResourceFailure, ProviderResourceHost,
    ProviderResourceRegistrationError, ProviderResourceTask,
};
use futures::future::BoxFuture;
use std::sync::Arc;

struct Host(PreparationResourceContext);
impl ProviderResourceHost for Host {
    fn register(
        &self,
        task: ProviderResourceTask,
    ) -> Result<(), ProviderResourceRegistrationError> {
        self.0
            .cleanup()
            .spawn(async move {
                task.await.map_err(|failure| match failure {
                    ProviderResourceFailure::CleanupFailed => crate::ResourceFailure::CleanupFailed,
                    ProviderResourceFailure::Cancelled => crate::ResourceFailure::Cancelled,
                })
            })
            .map_err(|error| match error {
                crate::ResourceRegistrationError::Unsupported => {
                    ProviderResourceRegistrationError::Unsupported
                }
                crate::ResourceRegistrationError::Sealed => {
                    ProviderResourceRegistrationError::Sealed
                }
                crate::ResourceRegistrationError::Capacity => {
                    ProviderResourceRegistrationError::Capacity
                }
            })
    }
    fn closing(&self) -> BoxFuture<'static, ()> {
        let context = self.0.clone();
        Box::pin(async move { context.closing().await })
    }
}
impl PreparationResourceContext {
    /// Register provider acquisition and cleanup in this exact preparation's
    /// existing owner. This grants no native service or execution authority.
    #[must_use]
    pub fn provider_resources(&self) -> ProviderResourceContext {
        ProviderResourceContext::from_host(self.session().clone(), Arc::new(Host(self.clone())))
    }
}
