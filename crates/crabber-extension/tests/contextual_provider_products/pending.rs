use super::*;
use std::{future::Future, sync::Condvar};
use tokio::sync::Notify;
struct Pending {
    polled: Notify,
    disposing: Notify,
    release: Mutex<bool>,
    changed: Condvar,
    drops: AtomicUsize,
}
impl Pending {
    fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Pending>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct OriginalFuture(Arc<Pending>);
impl Future for OriginalFuture {
    type Output = Result<DeltaStream, crabber_providers::ProviderError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.polled.notify_one();
        Poll::Pending
    }
}
impl Drop for OriginalFuture {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let mut release = self.0.release.lock().unwrap();
        while !*release {
            release = self.0.changed.wait(release).unwrap();
        }
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Backend(Arc<Pending>);
impl Streamer for Backend {
    fn stream<'life0, 'async_trait>(
        &'life0 self,
        _: ModelRequest,
    ) -> BoxFuture<'async_trait, Result<DeltaStream, crabber_providers::ProviderError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(OriginalFuture(self.0.clone()))
    }
}
struct Body(Arc<Pending>);
impl ContextualProviderBindingBody for Body {
    fn bind_box(
        &self,
        _: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        Box::pin(futures::future::ready(Ok(ContextualStreamerProduct::new(
            descriptor(),
            Box::new(Backend(self.0.clone())),
        ))))
    }
}
struct Factory(Arc<Pending>);
impl ContextualProviderProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "pending-original-stream".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "fixed".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_provider(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualProviderProduct, ExtensionError>> {
        Box::pin(futures::future::ready(Ok(ContextualProviderProduct::new(
            ProviderInfo {
                id: "actual".into(),
                name: "Actual".into(),
            },
            vec![descriptor()],
            Box::new(Body(self.0.clone())),
        ))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_stream_creation_waiter_retains_original_disposal_and_both_parent_joins() {
    let seen = Arc::new(Pending {
        polled: Notify::new(),
        disposing: Notify::new(),
        release: Mutex::new(false),
        changed: Condvar::new(),
        drops: AtomicUsize::new(0),
    });
    let release = Release(seen.clone());
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_provider_products(
        Registry::new(),
        vec![Arc::new(Factory(seen.clone()))],
        Duration::from_secs(2),
        host.clone(),
    )
    .unwrap();
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let bound = with_callback_invocations(owner.capability(), adapter.bind(&selection()))
        .await
        .unwrap();
    let (bound, released) = bound.observe_release();
    let streamer = bound.streamer();
    let requested = request(&context);
    let capability = owner.capability();
    let caller = tokio::spawn(with_callback_invocations(capability, async move {
        streamer.stream(requested).await
    }));
    seen.polled.notified().await;
    drop(bound);
    drop(adapter);
    drop(plan);
    caller.abort();
    let cancelled = match caller.await {
        Err(error) => error.is_cancelled(),
        Ok(result) => {
            drop(result);
            false
        }
    };
    seen.disposing.notified().await;
    drop(owner);
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    graph.request_close().unwrap();
    let before = resources.drain(Duration::from_millis(10)).await;
    let graph_before = graph.registered_graph_join(Duration::from_millis(10)).await;
    let released_before = released.is_released();
    drop(release);
    let after = resources.drain(Duration::from_secs(2)).await;
    graph
        .registered_graph_join(Duration::from_secs(2))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), released.released())
        .await
        .unwrap();
    assert!(cancelled && before.pending > 0 && graph_before.is_err() && !released_before);
    assert_eq!(after.pending, 0);
    assert_eq!(after.failures, [] as [ResourceFailure; 0]);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_binding_future_keeps_original_returned_box_release_timing() {
    let seen = Arc::new(Seen::default());
    let (provider, host) = setup(seen.clone());
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        let selected = selection();
        let mut original = Box::pin(adapter.bind(&selected));
        let bound = futures::future::poll_fn(|cx| original.as_mut().poll(cx))
            .await
            .unwrap();
        let (bound, released) = bound.observe_release();
        drop(bound);
        let before_drop = tokio::time::timeout(Duration::from_secs(1), released.released())
            .await
            .is_ok();
        drop(original);
        assert!(
            before_drop,
            "a completed public binding future cannot pin the returned Box"
        );
    })
    .await;
    drop(owner);
    assert_eq!(resources.drain(Duration::from_secs(1)).await.pending, 0);
    drop(adapter);
    drop(plan);
    finish(&provider, &host, &context).await;
}
