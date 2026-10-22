//! Actual original `DeltaStream` destructor and panic-payload context.
use super::*;
use crate::*;
use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingLimits, RunId, SessionContext, SessionId, TurnId,
};
use crabber_providers::{DeltaStream, ModelRequest, ProviderAdapter, StreamDelta};
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Notify;
struct Sink {
    session: SessionId,
    values: Mutex<BTreeMap<String, String>>,
}
#[async_trait]
impl StateSink for Sink {
    fn session_id(&self) -> &SessionId {
        &self.session
    }
    async fn snapshot(&self, _: &str) -> Result<BTreeMap<String, String>, String> {
        Ok(self.values.lock().unwrap().clone())
    }
    async fn apply(&self, _: &str, changes: Vec<(String, Option<String>)>) -> Result<(), String> {
        for (key, value) in changes {
            self.values.lock().unwrap().insert(key, value.unwrap());
        }
        Ok(())
    }
}
type Check = (
    bool,
    bool,
    Result<(), ExtensionError>,
    Result<Value, ExtensionError>,
);
struct Seen {
    sink: Arc<dyn StateSink>,
    context: SessionContext,
    owner: Mutex<Option<CallbackInvocationCapability>>,
    provider: Mutex<Option<ContextualPlanProvider>>,
    adapter: Mutex<Option<Weak<dyn ProviderAdapter>>>,
    next: Mutex<Option<Next>>,
    checks: Mutex<Vec<Check>>,
    terminal: AtomicUsize,
    payload: Notify,
    panic: bool,
}
impl Seen {
    fn inspect(&self, key: &str) {
        let sink = current_state_sink();
        let same_sink = sink
            .as_ref()
            .is_some_and(|sink| Arc::ptr_eq(sink, &self.sink));
        if let Some(sink) = sink {
            futures::executor::block_on(sink.apply(
                "literal-stream",
                vec![(key.into(), Some("observed".into()))],
            ))
            .unwrap();
        }
        let same_owner = self
            .owner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(CallbackInvocationCapability::matches_current);
        let provider = self.provider.lock().unwrap().clone().unwrap();
        let self_close =
            futures::executor::block_on(provider.close_session(&self.context, Duration::ZERO));
        let next = self.next.lock().unwrap().clone().unwrap();
        let called = futures::executor::block_on(next.call(Value::Null));
        self.checks
            .lock()
            .unwrap()
            .push((same_sink, same_owner, self_close, called));
    }
}
struct Original(Arc<Seen>);
impl Stream for Original {
    type Item = StreamDelta;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        self.0.inspect("original");
        if self.0.panic {
            std::panic::panic_any(Payload(self.0.clone()));
        }
    }
}
struct Payload(Arc<Seen>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.inspect("payload");
        self.0.payload.notify_one();
    }
}
struct Backend(Arc<Seen>);
#[async_trait]
impl Streamer for Backend {
    async fn stream(
        &self,
        _: ModelRequest,
    ) -> Result<DeltaStream, crabber_providers::ProviderError> {
        Ok(Box::pin(Original(self.0.clone())))
    }
}
struct Body(Arc<Seen>);
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
struct Factory(Arc<Seen>);
impl ContextualProviderProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "framed-provider".into(),
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
struct Host;
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
struct Around(Arc<Seen>);
#[async_trait]
impl Extension for Around {
    fn id(&self) -> &'static str {
        "provider-frame"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let seen = self.0.clone();
        registrar.on_around(
            ModelStream::ID,
            0,
            "actual-around",
            Arc::new(move |_, next| {
                *seen.next.lock().unwrap() = Some(next);
                let seen = seen.clone();
                Box::pin(async move {
                    let adapter = seen
                        .adapter
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .upgrade()
                        .unwrap();
                    let bound = adapter
                        .bind(&selection())
                        .await
                        .map_err(|error| ExtensionError::Plan(error.to_string()))?;
                    let (bound, released) = bound.observe_release();
                    let mut stream = bound
                        .streamer()
                        .stream(request(&seen.context))
                        .await
                        .map_err(|error| ExtensionError::Plan(error.to_string()))?;
                    assert_eq!(stream.next().await, None);
                    drop(stream);
                    drop(bound);
                    released.released().await;
                    Ok(Value::Bool(true))
                })
            }),
        );
        Ok(())
    }
}
fn descriptor() -> ProviderBindingDescriptor {
    ProviderBindingDescriptor::new(
        "actual",
        "model",
        "frames",
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
async fn exercise(panic: bool) {
    let context = SessionContext::new(SessionId::new(), "workspace", "/workspace");
    let sink: Arc<dyn StateSink> = Arc::new(Sink {
        session: context.session_id().clone(),
        values: Mutex::new(BTreeMap::new()),
    });
    let seen = Arc::new(Seen {
        sink: sink.clone(),
        context: context.clone(),
        owner: Mutex::new(None),
        provider: Mutex::new(None),
        adapter: Mutex::new(None),
        next: Mutex::new(None),
        checks: Mutex::new(Vec::new()),
        terminal: AtomicUsize::new(0),
        payload: Notify::new(),
        panic,
    });
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Around(seen.clone())), Scope::Global)
        .await
        .unwrap();
    let provider = ContextualPlanProvider::new_with_provider_products(
        registry,
        vec![Arc::new(Factory(seen.clone()))],
        Duration::from_secs(1),
        Arc::new(Host),
    )
    .unwrap();
    *seen.provider.lock().unwrap() = Some(provider.clone());
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let adapter = plan.providers[0].clone();
    *seen.adapter.lock().unwrap() = Some(Arc::downgrade(&adapter));
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    *seen.owner.lock().unwrap() = Some(owner.capability());
    let terminal = seen.clone();
    let dispatcher = plan.dispatcher.clone();
    let work = tokio::spawn(with_callback_invocations(
        owner.capability(),
        with_state_sink(sink.clone(), async move {
            dispatcher
                .around::<ModelStream>(
                    Value::Null,
                    Arc::new(move |value| {
                        terminal.terminal.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async move { Ok(value) })
                    }),
                )
                .await
        }),
    ));
    if panic {
        tokio::time::timeout(Duration::from_secs(2), seen.payload.notified())
            .await
            .unwrap();
        drop(owner);
    } else {
        assert_eq!(work.await.unwrap(), Ok(Value::Bool(true)));
        drop(owner);
        let drained = resources.drain(Duration::from_secs(1)).await;
        assert_eq!(drained.failures, [] as [ResourceFailure; 0]);
        drop(adapter);
        drop(plan);
        provider
            .close_session(&context, Duration::from_secs(1))
            .await
            .unwrap();
        mount.close(Duration::from_secs(1)).await.unwrap();
        check(&seen, false).await;
        return;
    }
    assert!(work.await.unwrap().is_err());
    let drained = resources.drain(Duration::from_secs(1)).await;
    assert_ne!(drained.failures, [] as [ResourceFailure; 0]);
    drop(adapter);
    drop(plan);
    assert!(
        provider
            .close_session(&context, Duration::from_millis(10))
            .await
            .is_err()
    );
    let _ = mount.close(Duration::from_millis(10)).await;
    check(&seen, true).await;
}
async fn check(seen: &Seen, panic: bool) {
    {
        let checks = seen.checks.lock().unwrap();
        assert_eq!(checks.len(), if panic { 2 } else { 1 });
        for (sink, owner, close, _) in &*checks {
            assert!(*sink && *owner);
            assert_eq!(*close, Err(ExtensionError::SelfClose));
        }
        assert_eq!(checks[0].3, Ok(Value::Null));
        if panic {
            assert_eq!(checks[1].3, Err(ExtensionError::NextCalledTwice));
        }
    }
    assert_eq!(seen.terminal.load(Ordering::SeqCst), 1);
    let state = seen.sink.snapshot("literal-stream").await.unwrap();
    assert_eq!(state.get("original").map(String::as_str), Some("observed"));
    if panic {
        assert_eq!(state.get("payload").map(String::as_str), Some("observed"));
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_stream_drop_preserves_sink_next_self_close_and_actual_caller() {
    exercise(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_stream_panic_payload_keeps_same_context_and_failure_sticky() {
    exercise(true).await;
}
