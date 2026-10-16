use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, SessionContext,
};
use crabber_extension::{MAX_TOOL_RESOURCE_TASKS, PreparationResourceContext, ToolResourceOwner};
use crabber_providers::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Provider(Arc<AtomicUsize>);
fn selection() -> Selection {
    Selection {
        provider_id: "provider".into(),
        model_id: "model".into(),
    }
}
#[async_trait]
impl Resolver for Provider {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "test",
            "1",
            InputFingerprint::new("a".repeat(64)).unwrap(),
            ProviderBindingLimits {
                context_tokens: None,
                output_tokens: None,
            },
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Empty))
    }
}
struct Empty;
#[async_trait]
impl Streamer for Empty {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        Ok(Box::pin(futures::stream::empty()))
    }
}
fn context(owner: &ToolResourceOwner) -> ProviderResourceContext {
    PreparationResourceContext::from_host(
        SessionContext::new("session".into(), "workspace", "/workspace"),
        owner.resources(),
    )
    .unwrap()
    .provider_resources()
}
#[tokio::test]
async fn actual_owner_reserves_capacity_before_binding_factory() {
    let owner = ToolResourceOwner::new();
    for _ in 0..MAX_TOOL_RESOURCE_TASKS {
        owner.resources().spawn(async { Ok(()) }).unwrap();
    }
    let provider = Provider(Arc::new(AtomicUsize::new(0)));
    let selection = selection();
    let result = context(&owner)
        .acquire_binding(|| provider.bind(&selection))
        .await;
    assert!(matches!(
        result,
        Err(ProviderBindingError::ResourceRegistration(
            ProviderResourceRegistrationError::Capacity
        ))
    ));
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        owner.drain(Duration::from_secs(1)).await.joined,
        MAX_TOOL_RESOURCE_TASKS
    );
}
#[tokio::test]
async fn actual_owner_waits_for_acquired_provider_reference_release() {
    let owner = ToolResourceOwner::new();
    let provider = Provider(Arc::new(AtomicUsize::new(0)));
    let context = context(&owner);
    let selection = selection();
    let bound = context
        .acquire_binding(|| provider.bind(&selection))
        .await
        .unwrap();
    let extracted = bound.streamer();
    drop(bound);
    assert_eq!(owner.drain(Duration::ZERO).await.pending, 1);
    drop(extracted);
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.joined, 1);
    assert_eq!(joined.failures, []);
}
#[tokio::test]
async fn acquisition_error_does_not_join_an_independent_failed_cleanup() {
    let owner = ToolResourceOwner::new();
    let context = context(&owner);
    context
        .defer(|| async { Err(ProviderResourceFailure::CleanupFailed) })
        .unwrap();
    assert!(matches!(
        context
            .acquire_binding(|| async { Err(ProviderBindingError::Unavailable) })
            .await,
        Err(ProviderBindingError::Unavailable)
    ));
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.joined, 2);
    assert_eq!(
        joined.failures,
        [crabber_extension::ResourceFailure::CleanupFailed]
    );
}
#[tokio::test]
async fn declared_owned_binding_refuses_legacy_fallback() {
    let owner = ToolResourceOwner::new();
    let provider = Provider(Arc::new(AtomicUsize::new(0)));
    assert!(
        provider
            .binding_resource_contract(&selection())
            .unwrap()
            .is_none()
    );
    let descriptor = provider.binding_descriptor(&selection()).unwrap();
    assert!(matches!(
        provider
            .restore_binding_owned(&descriptor, &context(&owner))
            .await,
        Err(ProviderBindingError::ResourcesUnsupported)
    ));
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
}

struct HeldAcquisition {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Arc<std::sync::Barrier>,
    panic: bool,
}
impl std::future::Future for HeldAcquisition {
    type Output = Result<BoundProvider, ProviderBindingError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Pending
    }
}
impl Drop for HeldAcquisition {
    fn drop(&mut self) {
        let _ = self.entered.take().unwrap().send(());
        self.release.wait();
        assert!(!self.panic, "acquisition disposal failed");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_acquisition_joins_actual_future_drop_and_preserves_drop_failure() {
    for panic in [false, true] {
        let owner = ToolResourceOwner::new();
        let context = context(&owner);
        let (entered, started) = tokio::sync::oneshot::channel();
        let release = Arc::new(std::sync::Barrier::new(2));
        let future = HeldAcquisition {
            entered: Some(entered),
            release: release.clone(),
            panic,
        };
        let acquisition = async move { context.acquire_binding(|| future).await };
        // Keep the scope and acquiring frame together in a retained owned task.
        let mut acquisition = Box::pin(acquisition);
        assert!(futures::poll!(acquisition.as_mut()).is_pending());
        let disposer = tokio::task::spawn_blocking(move || drop(acquisition));
        started.await.unwrap();
        assert_eq!(owner.drain(Duration::ZERO).await.pending, 1);
        release.wait();
        let result = disposer.await;
        if panic {
            assert!(result.unwrap_err().is_panic());
            assert_eq!(owner.drain(Duration::from_millis(10)).await.pending, 1);
        } else {
            result.unwrap();
            assert_eq!(owner.drain(Duration::from_secs(1)).await.pending, 0);
        }
    }
}
