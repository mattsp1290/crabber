use super::*;
use std::{future::Future, sync::Condvar};
use tokio::sync::Notify;
struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn block(&self) {
        self.entered.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Cross {
    future: Arc<Gate>,
    tail: Arc<Gate>,
    disposed: Notify,
}
struct Release(Arc<Cross>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.future.release();
        self.0.tail.release();
    }
}
struct Returned<T> {
    value: Option<T>,
    cross: Arc<Cross>,
}
impl<T: Unpin> Future for Returned<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        Poll::Ready(self.value.take().unwrap())
    }
}
impl<T> Drop for Returned<T> {
    fn drop(&mut self) {
        self.cross.future.block();
        self.cross.disposed.notify_one();
    }
}
struct CrossBackend(Arc<Cross>);
impl Drop for CrossBackend {
    fn drop(&mut self) {
        self.0.future.release();
        self.0.tail.block();
    }
}
#[async_trait]
impl Streamer for CrossBackend {
    async fn stream(
        &self,
        _: ModelRequest,
    ) -> Result<DeltaStream, crabber_providers::ProviderError> {
        unreachable!("binding-only handoff")
    }
}
struct CrossBody(Arc<Cross>);
impl ContextualProviderBindingBody for CrossBody {
    fn bind_box(
        &self,
        _: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        Box::pin(Returned {
            value: Some(Ok(ContextualStreamerProduct::new(
                descriptor(),
                Box::new(CrossBackend(self.0.clone())),
            ))),
            cross: self.0.clone(),
        })
    }
}
struct CrossFactory(Arc<Cross>);
impl ContextualProviderProductFactory for CrossFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "cross-unblock-box".into(),
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
            Box::new(CrossBody(self.0.clone())),
        ))))
    }
}
struct ResourceHost(
    Mutex<Vec<tokio::task::JoinHandle<Result<(), crabber_providers::ProviderResourceFailure>>>>,
);
impl crabber_providers::ProviderResourceHost for ResourceHost {
    fn register(
        &self,
        work: crabber_providers::ProviderResourceTask,
    ) -> Result<(), crabber_providers::ProviderResourceRegistrationError> {
        self.0.lock().unwrap().push(tokio::spawn(work));
        Ok(())
    }
    fn closing(&self) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_streamer_box_drop_unblocks_original_binding_future_before_join() {
    let cross = Arc::new(Cross {
        future: Gate::new(),
        tail: Gate::new(),
        disposed: Notify::new(),
    });
    let release = Release(cross.clone());
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_provider_products(
        Registry::new(),
        vec![Arc::new(CrossFactory(cross.clone()))],
        Duration::from_secs(2),
        host.clone(),
    )
    .unwrap();
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let tickets = Arc::new(ResourceHost(Mutex::new(vec![])));
    let acquisition =
        crabber_providers::ProviderResourceContext::from_host(context.clone(), tickets.clone());
    let original = adapter.clone();
    let capability = owner.capability();
    let caller = tokio::spawn(with_callback_invocations(capability, async move {
        acquisition
            .acquire_binding(move || async move { original.bind(&selection()).await })
            .await
    }));
    cross.future.entered.notified().await;
    drop(owner);
    let destructor_entered =
        tokio::time::timeout(Duration::from_secs(1), cross.tail.entered.notified())
            .await
            .is_ok();
    let constructor_disposed =
        tokio::time::timeout(Duration::from_secs(1), cross.disposed.notified())
            .await
            .is_ok();
    let before = resources.drain(Duration::from_millis(10)).await;
    let mut ticket = tickets.0.lock().unwrap().pop().unwrap();
    let ticket_pending = futures::poll!(&mut ticket).is_pending();
    drop(release);
    let result = caller.await.unwrap();
    let after = resources.drain(Duration::from_secs(2)).await;
    assert_eq!(ticket.await.unwrap(), Ok(()));
    drop(adapter);
    drop(plan);
    finish(&provider, &host, &context).await;
    assert!(
        destructor_entered && constructor_disposed,
        "actual returned Box must unblock original future Drop"
    );
    assert!(
        before.pending > 0 && ticket_pending,
        "both actual parent and original release ticket stay pending through Box Drop tail"
    );
    assert_eq!(after.pending, 0);
    assert_eq!(
        after.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    assert!(result.is_err());
}

struct ReturnedStream(Arc<Cross>);
impl Stream for ReturnedStream {
    type Item = StreamDelta;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
impl Drop for ReturnedStream {
    fn drop(&mut self) {
        self.0.future.release();
        self.0.tail.block();
    }
}
struct StreamBackend(Arc<Cross>);
impl Streamer for StreamBackend {
    fn stream<'life0, 'async_trait>(
        &'life0 self,
        _: ModelRequest,
    ) -> BoxFuture<'async_trait, Result<DeltaStream, crabber_providers::ProviderError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(Returned {
            value: Some(Ok(Box::pin(ReturnedStream(self.0.clone())) as DeltaStream)),
            cross: self.0.clone(),
        })
    }
}
struct StreamBody(Arc<Cross>);
impl ContextualProviderBindingBody for StreamBody {
    fn bind_box(
        &self,
        _: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        Box::pin(futures::future::ready(Ok(ContextualStreamerProduct::new(
            descriptor(),
            Box::new(StreamBackend(self.0.clone())),
        ))))
    }
}
struct StreamFactory(Arc<Cross>);
impl ContextualProviderProductFactory for StreamFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "actual-stream-cross-unblock".into(),
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
            Box::new(StreamBody(self.0.clone())),
        ))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returned_stream_drop_unblocks_creation_future_both_parents_wait_actual_tail() {
    let cross = Arc::new(Cross {
        future: Gate::new(),
        tail: Gate::new(),
        disposed: Notify::new(),
    });
    let release = Release(cross.clone());
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_provider_products(
        Registry::new(),
        vec![Arc::new(StreamFactory(cross.clone()))],
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
    drop(bound);
    cross.future.entered.notified().await;
    drop(owner);
    let destructor_entered =
        tokio::time::timeout(Duration::from_secs(1), cross.tail.entered.notified())
            .await
            .is_ok();
    let constructor_disposed =
        tokio::time::timeout(Duration::from_secs(1), cross.disposed.notified())
            .await
            .is_ok();
    let before = resources.drain(Duration::from_millis(10)).await;
    let observer_pending = !released.is_released();
    drop(release);
    let result = caller.await.unwrap();
    drop(result);
    let after = resources.drain(Duration::from_secs(2)).await;
    tokio::time::timeout(Duration::from_secs(2), released.released())
        .await
        .unwrap();
    drop(adapter);
    drop(plan);
    finish(&provider, &host, &context).await;
    assert!(destructor_entered && constructor_disposed);
    assert!(
        before.pending > 0 && observer_pending,
        "original actor/returned stream disposal cannot be replaced by waiter completion"
    );
    assert_eq!(after.pending, 0);
    assert_eq!(
        after.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
}
