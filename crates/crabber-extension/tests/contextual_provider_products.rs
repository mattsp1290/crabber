//! Actual fixed-route Box binding/stream products, not declaration coverage.
use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, RunId, SessionContext,
    SessionId, TurnId,
};
use crabber_extension::*;
use crabber_providers::{
    DeltaStream, ModelRequest, ProviderBindingError, ProviderInfo, Selection, StreamDelta, Streamer,
};
use futures::{Stream, StreamExt, future::BoxFuture};
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

#[derive(Default)]
struct Seen {
    panic_streamer: std::sync::atomic::AtomicBool,
    binds: AtomicUsize,
    creates: AtomicUsize,
    polls: AtomicUsize,
    roots: AtomicUsize,
    streamers: AtomicUsize,
    streams: AtomicUsize,
}
struct Body(Arc<Seen>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.roots.fetch_add(1, Ordering::SeqCst);
    }
}
impl ContextualProviderBindingBody for Body {
    fn bind_box(
        &self,
        _: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        self.0.binds.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::future::ready(Ok(ContextualStreamerProduct::new(
            descriptor(),
            Box::new(Backend(self.0.clone())),
        ))))
    }
}
struct Backend(Arc<Seen>);
impl Drop for Backend {
    fn drop(&mut self) {
        self.0.streamers.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.0.panic_streamer.swap(false, Ordering::SeqCst),
            "original returned streamer Drop panic"
        );
    }
}
#[async_trait]
impl Streamer for Backend {
    async fn stream(
        &self,
        _: ModelRequest,
    ) -> Result<DeltaStream, crabber_providers::ProviderError> {
        self.0.creates.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(Original {
            seen: self.0.clone(),
            yielded: false,
        }))
    }
}
struct Original {
    seen: Arc<Seen>,
    yielded: bool,
}
impl Stream for Original {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.seen.polls.fetch_add(1, Ordering::SeqCst);
        if self.yielded {
            Poll::Ready(None)
        } else {
            self.yielded = true;
            Poll::Ready(Some(StreamDelta::TextDelta("original".into())))
        }
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        self.seen.streams.fetch_add(1, Ordering::SeqCst);
    }
}
struct Factory(Arc<Seen>);
impl ContextualProviderProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "provider-product".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "fixed-v1".into()
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
#[derive(Default)]
struct Host(Mutex<Vec<ContextualGenerationObservation>>);
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        self.0.lock().unwrap().push(work.observation().unwrap());
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
fn descriptor() -> ProviderBindingDescriptor {
    ProviderBindingDescriptor::new(
        "actual",
        "model",
        "unique-box",
        "1",
        InputFingerprint::new("a".repeat(64)).unwrap(),
        ProviderBindingLimits::default(),
    )
    .unwrap()
}
fn selection() -> Selection {
    Selection {
        provider_id: "actual".into(),
        model_id: "model".into(),
    }
}
fn context() -> SessionContext {
    SessionContext::new(SessionId::new(), "workspace", "/workspace")
}
fn request(context: &SessionContext) -> ModelRequest {
    ModelRequest {
        context: vec![],
        identity: crabber_providers::RequestIdentity {
            session_id: context.session_id().clone(),
            run_id: RunId::new(),
            turn_id: TurnId::new(),
        },
        selection: selection(),
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        reasoning_effort: None,
        tool_choice: None,
    }
}
fn setup(seen: Arc<Seen>) -> (ContextualPlanProvider, Arc<Host>) {
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_provider_products(
        Registry::new(),
        vec![Arc::new(Factory(seen))],
        Duration::from_secs(2),
        host.clone(),
    )
    .unwrap();
    (provider, host)
}
async fn finish(provider: &ContextualPlanProvider, host: &Host, context: &SessionContext) {
    provider
        .close_session(context, Duration::from_secs(2))
        .await
        .unwrap();
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    graph
        .registered_graph_join(Duration::from_secs(2))
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_box_provider_stream_uses_actual_caller_and_drops_originals() {
    let seen = Arc::new(Seen::default());
    let (provider, host) = setup(seen.clone());
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        assert_eq!(
            adapter.binding_resource_contract(&selection()).unwrap(),
            None
        );
        let bound = adapter.bind(&selection()).await.unwrap();
        let (bound, released) = bound.observe_release();
        let mut stream = bound.streamer().stream(request(&context)).await.unwrap();
        assert_eq!(
            stream.next().await,
            Some(StreamDelta::TextDelta("original".into()))
        );
        assert_eq!(stream.next().await, None);
        drop(stream);
        drop(bound);
        tokio::time::timeout(Duration::from_secs(2), released.released())
            .await
            .unwrap();
    })
    .await;
    drop(owner);
    let drained = resources.drain(Duration::from_secs(2)).await;
    drop(adapter);
    drop(plan);
    finish(&provider, &host, &context).await;
    assert_eq!(drained.pending, 0);
    assert_eq!(
        drained.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    assert_eq!(seen.binds.load(Ordering::SeqCst), 1);
    assert_eq!(seen.creates.load(Ordering::SeqCst), 1);
    assert_eq!(seen.streams.load(Ordering::SeqCst), 1);
    assert_eq!(seen.streamers.load(Ordering::SeqCst), 1);
    assert_eq!(seen.roots.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_owner_and_closed_original_mount_refuse_before_binding_effect() {
    let seen = Arc::new(Seen::default());
    let (provider, host) = setup(seen.clone());
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let missing = adapter.bind(&selection()).await;
    host.0
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .request_close()
        .unwrap();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let closed = with_callback_invocations(owner.capability(), adapter.bind(&selection())).await;
    drop(owner);
    resources.drain(Duration::from_secs(2)).await;
    drop(adapter);
    drop(plan);
    // request_close seals the slot; original graph is still the same graph.
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    graph
        .registered_graph_join(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        missing,
        Err(ProviderBindingError::ResourcesUnsupported)
    ));
    assert!(closed.is_err());
    assert_eq!(seen.binds.load(Ordering::SeqCst), 0);
}
#[path = "contextual_provider_products/handoff.rs"]
mod handoff;
#[path = "contextual_provider_products/pending.rs"]
mod pending;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_actual_joins_reap_capacity_and_original_owner_cannot_be_replaced() {
    let seen = Arc::new(Seen::default());
    let (provider, host) = setup(seen.clone());
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let other_resources = ToolResourceOwner::new();
    let other = CallbackInvocationOwner::attach(other_resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        for index in 0..70 {
            let bound = adapter.bind(&selection()).await.unwrap();
            let (bound, released) = bound.observe_release();
            let mut stream = bound.streamer().stream(request(&context)).await.unwrap();
            if index == 0 {
                let changed = with_callback_invocations(other.capability(), stream.next()).await;
                assert!(matches!(changed, Some(StreamDelta::Error(_))));
                assert_eq!(seen.polls.load(Ordering::SeqCst), 0);
            }
            assert!(matches!(
                stream.next().await,
                Some(StreamDelta::TextDelta(_))
            ));
            assert_eq!(stream.next().await, None);
            drop(stream);
            drop(bound);
            tokio::time::timeout(Duration::from_secs(2), released.released())
                .await
                .unwrap();
        }
    })
    .await;
    drop(other);
    drop(owner);
    let other = other_resources.drain(Duration::from_secs(2)).await;
    let drained = resources.drain(Duration::from_secs(2)).await;
    drop(adapter);
    drop(plan);
    finish(&provider, &host, &context).await;
    assert_eq!(other.pending, 0);
    assert_eq!(
        other.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    assert_eq!(drained.pending, 0);
    assert_eq!(
        drained.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    assert_eq!(seen.binds.load(Ordering::SeqCst), 70);
    assert_eq!(seen.streams.load(Ordering::SeqCst), 70);
    assert_eq!(seen.streamers.load(Ordering::SeqCst), 70);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_streamer_drop_retains_original_release_after_graph_metadata_drops() {
    use futures::FutureExt;
    let seen = Arc::new(Seen::default());
    let (provider, host) = setup(seen.clone());
    let context = context();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let released = with_callback_invocations(owner.capability(), async {
        let (first, released) = adapter.bind(&selection()).await.unwrap().observe_release();
        // This genuine second registration reaps the first successful original
        // constructor actor, independently of the still-live returned Box.
        let (second, second_release) = adapter.bind(&selection()).await.unwrap().observe_release();
        drop(second);
        tokio::time::timeout(Duration::from_secs(2), second_release.released())
            .await
            .unwrap();
        seen.panic_streamer.store(true, Ordering::SeqCst);
        drop(first);
        released
    })
    .await;
    drop(adapter);
    drop(plan);
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    graph.request_close().unwrap();
    let joined = graph.registered_graph_join(Duration::from_secs(2)).await;
    drop(owner);
    let report = resources.drain(Duration::from_secs(2)).await;
    drop(resources);
    drop(graph);
    drop(provider);
    drop(host);
    assert!(
        joined.is_err(),
        "the original Box destructor actually failed"
    );
    assert_eq!(report.pending, 0);
    assert_eq!(seen.streamers.load(Ordering::SeqCst), 2);
    assert!(
        released.released().now_or_never().is_none(),
        "failed original Box disposal must never acknowledge release merely because its manager/graph metadata disappeared"
    );
}
