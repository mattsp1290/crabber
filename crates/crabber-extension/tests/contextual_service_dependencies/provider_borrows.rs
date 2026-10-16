//! Real factory/member leases; the inherited host simulates native ACKs only.
use super::*;
use crabber_providers::{FakeProvider, ModelRequest, Resolver, Selection, StreamDelta};
struct BorrowFactory {
    inner: Factory,
    source: Arc<Mutex<Option<Arc<dyn crabber_providers::ProviderServiceSource>>>>,
}
#[async_trait]
impl ExtensionFactory for BorrowFactory {
    fn identity(&self) -> ComponentIdentity {
        self.inner.identity()
    }
    fn config_hash(&self) -> String {
        self.inner.config_hash()
    }
    fn scope(&self) -> FactoryScope {
        self.inner.scope()
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        *self.source.lock().unwrap() = Some(context.provider_service_source());
        self.inner.create(context).await
    }
}
#[tokio::test]
async fn provider_references_hold_real_member_and_exact_session_dependency() {
    let seen = Arc::new(Observed::default());
    let source = Arc::new(Mutex::new(None));
    let provider = provider(vec![Arc::new(BorrowFactory {
        inner: Factory {
            name: "borrowed-provider",
            scope: FactoryScope::Workspace,
            observed: seen.clone(),
        },
        source: source.clone(),
    })]);
    let a = context("provider-a", "borrowed-workspace");
    let b = context("provider-b", "borrowed-workspace");
    let host_a = Arc::new(Host::default());
    let host_b = Arc::new(Host::default());
    let (scope_a, resources_a) = preparation(&a, host_a.clone());
    let (scope_b, resources_b) = preparation(&b, host_b.clone());
    let plan_a = provider
        .acquire_plan_in_preparation(&scope_a)
        .await
        .unwrap();
    let plan_b = provider
        .acquire_plan_in_preparation(&scope_b)
        .await
        .unwrap();
    let erased = source.lock().unwrap().clone().unwrap();
    let source = erased
        .as_any()
        .downcast_ref::<ContextualServiceBorrowSource>()
        .unwrap();
    let (bound, streamer, stream) = borrowed(source, &scope_a, &a).await;
    let binding_clone = bound.clone();
    plan_a.release();
    drop(bound);
    assert!(
        provider
            .close_session(&a, Duration::from_millis(20))
            .await
            .is_err()
    );
    drop(binding_clone);
    assert!(
        provider
            .close_session(&a, Duration::from_millis(20))
            .await
            .is_err()
    );
    drop(streamer);
    assert!(
        provider
            .close_session(&a, Duration::from_millis(20))
            .await
            .is_err()
    );
    drop(stream);
    provider
        .close_session(&a, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        host_a
            .entries
            .lock()
            .await
            .values()
            .all(|entry| entry.joined.load(Ordering::SeqCst))
    );
    assert!(
        host_b
            .entries
            .lock()
            .await
            .values()
            .all(|entry| !entry.joined.load(Ordering::SeqCst))
    );
    assert_eq!(seen.created.load(Ordering::SeqCst), 1);
    assert!(seen.shutdown.lock().unwrap().is_empty());
    plan_b.release();
    resources_a.drain(Duration::from_secs(1)).await;
    resources_b.drain(Duration::from_secs(1)).await;
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    let calls = host_a.seen.lock().unwrap().len();
    assert!(
        source
            .borrow_in(&a, scope_a.service_dependencies())
            .await
            .is_err()
    );
    assert_eq!(
        host_a.seen.lock().unwrap().len(),
        calls,
        "closed producer must refuse before host registration"
    );
    assert_eq!(seen.shutdown.lock().unwrap().len(), 1);
}

async fn borrowed(
    source: &ContextualServiceBorrowSource,
    scope: &PreparationResourceContext,
    context: &SessionContext,
) -> (
    crabber_providers::BoundProvider,
    Arc<dyn crabber_providers::Streamer>,
    crabber_providers::DeltaStream,
) {
    let lease = source
        .borrow_in(context, scope.service_dependencies())
        .await
        .unwrap();
    let selection = Selection {
        provider_id: "fake".into(),
        model_id: "model".into(),
    };
    let fake = FakeProvider::scripted(vec![vec![StreamDelta::Completed]]);
    let bound = fake
        .bind(&selection)
        .await
        .unwrap()
        .retain_service_lease(Arc::new(lease));
    let streamer = bound.streamer();
    let stream = streamer
        .stream(ModelRequest {
            context: vec![],
            identity: crabber_providers::RequestIdentity {
                session_id: context.session_id().clone(),
                run_id: crabber_core::RunId::new(),
                turn_id: crabber_core::TurnId::new(),
            },
            selection,
            system: None,
            messages: vec![],
            tools: vec![],
            temperature: None,
            max_tokens: None,
            tool_choice: None,
        })
        .await
        .unwrap();
    (bound, streamer, stream)
}

#[tokio::test]
async fn physical_mount_shutdown_waits_for_borrowed_provider_lease() {
    let seen = Arc::new(Observed::default());
    let captured = Arc::new(Mutex::new(None));
    let provider = provider(vec![Arc::new(BorrowFactory {
        inner: Factory {
            name: "physical-provider",
            scope: FactoryScope::Workspace,
            observed: seen.clone(),
        },
        source: captured.clone(),
    })]);
    let context = context("physical-a", "physical-workspace");
    let (scope, resources) = preparation(&context, Arc::new(Host::default()));
    let plan = provider.acquire_plan_in_preparation(&scope).await.unwrap();
    let erased = captured.lock().unwrap().clone().unwrap();
    let source = erased
        .as_any()
        .downcast_ref::<ContextualServiceBorrowSource>()
        .unwrap();
    let (bound, streamer, stream) = borrowed(source, &scope, &context).await;
    plan.release();
    assert!(
        provider
            .close_workspace(&context, Duration::from_millis(30))
            .await
            .is_err()
    );
    assert!(
        seen.shutdown.lock().unwrap().is_empty(),
        "a dependency count alone cannot keep the physical mount alive"
    );
    drop((bound, streamer, stream));
    resources.drain(Duration::from_secs(1)).await;
    provider
        .close_workspace(&context, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(seen.shutdown.lock().unwrap().len(), 1);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}
