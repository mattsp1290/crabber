//! Dependency-only provider borrows from an actual Contextual factory member.
use crabber_core::ProviderBindingDescriptor;
use crabber_extension::{
    ContextualServiceBorrowSource, ExtensionError, PreparationResourceContext,
    ResourceRegistrationError, SessionServiceDependencyError,
};
use crabber_providers::{
    BoundProvider, ProviderBindingError, ProviderBindingFactory, ProviderServiceDependencies,
    ProviderServiceError, ProviderServiceHost, ProviderServiceSource,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct ProviderServices {
    context: PreparationResourceContext,
    parent_done: CancellationToken,
}
struct Host {
    scope: ProviderServices,
    descriptor: ProviderBindingDescriptor,
}
impl ProviderServices {
    pub(crate) fn new(context: PreparationResourceContext, parent_done: CancellationToken) -> Self {
        Self {
            context,
            parent_done,
        }
    }
    pub(crate) fn bind(
        &self,
        descriptor: ProviderBindingDescriptor,
    ) -> ProviderServiceDependencies {
        ProviderServiceDependencies::from_host(Arc::new(Host {
            scope: self.clone(),
            descriptor,
        }))
    }
}
#[async_trait::async_trait]
impl ProviderServiceHost for Host {
    async fn acquire_binding(
        &self,
        source: Arc<dyn ProviderServiceSource>,
        factory: ProviderBindingFactory,
    ) -> Result<BoundProvider, ProviderBindingError> {
        // Metadata and arbitrary trait implementations cannot mint this source.
        let source = source
            .as_any()
            .downcast_ref::<ContextualServiceBorrowSource>()
            .ok_or(ProviderServiceError::Unsupported)?
            .clone();
        if self.scope.parent_done.is_cancelled() {
            return Err(ProviderServiceError::Rejected.into());
        }
        let scope = self.scope.clone();
        let descriptor = self.descriptor.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Registration retains the actual acquisition before any native call
        // or constructor poll. Dropping the response waiter cannot cancel it.
        self.scope
            .context
            .cleanup()
            .spawn(async move {
                let result = acquire(scope, source, descriptor, factory).await;
                // A dropped waiter disposes the actual returned binding before
                // this task can acknowledge completion. Drop panic stays visible.
                drop(sender.send(result));
                Ok(())
            })
            .map_err(registration)?;
        receiver
            .await
            .map_err(|_| ProviderServiceError::UnknownOutcome)?
    }
}
async fn acquire(
    scope: ProviderServices,
    source: ContextualServiceBorrowSource,
    descriptor: ProviderBindingDescriptor,
    factory: ProviderBindingFactory,
) -> Result<BoundProvider, ProviderBindingError> {
    if scope.parent_done.is_cancelled() {
        return Err(ProviderServiceError::Rejected.into());
    }
    let lease = source
        .borrow_in(
            scope.context.session(),
            scope.context.service_dependencies(),
        )
        .await
        .map_err(|error| extension(&error))?;
    let binding = BoundProvider::acquire_service_reference(Arc::new(lease), move || async move {
        // The native access check may have waited across actual parent return.
        // Check at the FnOnce boundary too; sealing supplies no Join evidence.
        if scope.parent_done.is_cancelled() {
            return Err(ProviderServiceError::Rejected.into());
        }
        factory().await
    })
    .await?;
    if binding.descriptor() != &descriptor {
        return Err(ProviderBindingError::VersionConflict);
    }
    Ok(binding)
}
fn registration(error: ResourceRegistrationError) -> ProviderBindingError {
    match error {
        ResourceRegistrationError::Unsupported => ProviderServiceError::Unsupported,
        ResourceRegistrationError::Sealed => ProviderServiceError::Rejected,
        ResourceRegistrationError::Capacity => ProviderServiceError::Capacity,
    }
    .into()
}
fn extension(error: &ExtensionError) -> ProviderBindingError {
    match error {
        ExtensionError::SessionServiceDependency(error) => match error {
            SessionServiceDependencyError::Unsupported => ProviderServiceError::Unsupported,
            SessionServiceDependencyError::Rejected => ProviderServiceError::Rejected,
            SessionServiceDependencyError::Capacity => ProviderServiceError::Capacity,
            SessionServiceDependencyError::Incomplete => ProviderServiceError::Incomplete,
            SessionServiceDependencyError::UnknownOutcome => ProviderServiceError::UnknownOutcome,
            SessionServiceDependencyError::Deleted => ProviderServiceError::Deleted,
        },
        ExtensionError::ContextCleanupIncomplete
        | ExtensionError::ContextInitializationIncomplete => ProviderServiceError::Incomplete,
        ExtensionError::PreparationResourcesUnsupported => ProviderServiceError::Unsupported,
        _ => ProviderServiceError::Rejected,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_providers::Resolver;
    struct CustomSource;
    impl ProviderServiceSource for CustomSource {}
    #[tokio::test]
    async fn custom_source_does_not_authorize_provider_constructor() {
        let resources = crabber_extension::ToolResourceOwner::new();
        let context = PreparationResourceContext::from_host(
            crabber_core::SessionContext::new(
                crabber_core::SessionId::new(),
                "workspace",
                "/workspace",
            ),
            resources.resources(),
        )
        .unwrap();
        let fake = crabber_providers::FakeProvider::scripted(vec![]);
        let descriptor = fake
            .binding_descriptor(&crabber_providers::Selection {
                provider_id: "fake".into(),
                model_id: "model".into(),
            })
            .unwrap();
        let host =
            ProviderServices::new(context.clone(), CancellationToken::new()).bind(descriptor);
        let provider = context.provider_resources().with_service_dependencies(host);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let constructor = calls.clone();
        assert!(matches!(
            provider
                .acquire_service_binding(Arc::new(CustomSource), move || async move {
                    constructor.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(ProviderBindingError::VersionConflict)
                })
                .await,
            Err(ProviderBindingError::Service(
                ProviderServiceError::Unsupported
            ))
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let joined = resources.drain(std::time::Duration::from_secs(1)).await;
        assert_eq!(joined.pending, 0);
        assert_eq!(joined.failures, []);
    }
}
