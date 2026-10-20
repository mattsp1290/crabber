//! Actual Box Tool references, borrowed invocations and original destructor joins.
use async_trait::async_trait;
use crabber_core::{SessionContext, SessionId, ToolInfo, ToolOutput};
use crabber_extension::*;
use futures::future::BoxFuture;
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Notify;
struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            open: Mutex::new(open),
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
struct Release(Vec<Arc<Gate>>);
impl Drop for Release {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.release();
        }
    }
}
#[derive(Clone, Copy)]
enum Call {
    Ready,
    Pending,
}
struct Seen {
    literal: AtomicBool,
    literal_adapter: AtomicUsize,
    literal_poll_panic: AtomicBool,
    semantic_error: AtomicBool,
    effects: AtomicUsize,
    drops: AtomicUsize,
    called: Notify,
    creating: Notify,
    ready: Arc<tokio::sync::Semaphore>,
    tool_drop: Arc<Gate>,
    callback_drop: Arc<Gate>,
    create_drop: Arc<Gate>,
    call: Call,
    callback_panic: AtomicBool,
    tool_panic: AtomicBool,
    declaration: AtomicBool,
    capability: AtomicBool,
    provider: Mutex<Option<ContextualPlanProvider>>,
    self_close: Mutex<Vec<Result<(), ExtensionError>>>,
    cleanup_enabled: AtomicBool,
    cleanup_release: Arc<tokio::sync::Semaphore>,
    cleanup_facade: Mutex<Option<Arc<dyn ToolExecutor>>>,
    cleanup_started: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    cleanup_graph: Mutex<Option<ContextualGenerationObservation>>,
    drop_complete: Arc<Gate>,
}
impl Seen {
    fn new(call: Call) -> Arc<Self> {
        Arc::new(Self {
            literal: AtomicBool::new(false),
            literal_adapter: AtomicUsize::new(0),
            literal_poll_panic: AtomicBool::new(false),
            semantic_error: AtomicBool::new(false),
            effects: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            called: Notify::new(),
            creating: Notify::new(),
            ready: Arc::new(tokio::sync::Semaphore::new(1)),
            tool_drop: Gate::new(true),
            callback_drop: Gate::new(true),
            create_drop: Gate::new(true),
            call,
            callback_panic: AtomicBool::new(false),
            tool_panic: AtomicBool::new(false),
            declaration: AtomicBool::new(false),
            capability: AtomicBool::new(false),
            provider: Mutex::new(None),
            self_close: Mutex::new(vec![]),
            cleanup_enabled: AtomicBool::new(false),
            cleanup_release: Arc::new(tokio::sync::Semaphore::new(0)),
            cleanup_facade: Mutex::new(None),
            cleanup_started: Mutex::new(None),
            cleanup_graph: Mutex::new(None),
            drop_complete: Gate::new(false),
        })
    }
}
struct Tool(Arc<Seen>);
impl Drop for Tool {
    fn drop(&mut self) {
        self.0.tool_drop.block();
        self.0.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(provider) = self.0.provider.lock().unwrap().clone() {
            let result = futures::executor::block_on(
                provider.close_session(&context(), Duration::from_millis(100)),
            );
            self.0.self_close.lock().unwrap().push(result);
        }
        assert!(
            !self.0.tool_panic.load(Ordering::SeqCst),
            "original Tool destructor panic"
        );
        self.0.drop_complete.release();
    }
}
struct OriginalCall(Arc<Seen>);
impl Future for OriginalCall {
    type Output = Result<ToolOutput, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.0.literal_poll_panic.load(Ordering::SeqCst),
            "literal Tool poll panic"
        );
        if self.0.semantic_error.load(Ordering::SeqCst) {
            return Poll::Ready(Err(ExtensionError::Tool("literal semantic error".into())));
        }
        match self.0.call {
            Call::Ready => Poll::Ready(Ok(ToolOutput::from_json(&Value::Null).unwrap())),
            Call::Pending => Poll::Pending,
        }
    }
}
impl Drop for OriginalCall {
    fn drop(&mut self) {
        self.0.callback_drop.block();
        assert!(
            !self.0.callback_panic.load(Ordering::SeqCst),
            "original borrowed Tool future destructor panic"
        );
    }
}
#[async_trait]
impl ToolExecutor for Tool {
    fn resumable(&self) -> Option<&dyn ResumableTool> {
        self.0
            .capability
            .load(Ordering::SeqCst)
            .then_some(self as &dyn ResumableTool)
    }
    fn resumable_identity(&self) -> Option<crabber_core::ResumableToolIdentity> {
        self.0.declaration.load(Ordering::SeqCst).then(identity)
    }
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        self.0.called.notify_one();
        let provider = { self.0.provider.lock().unwrap().clone() };
        if let Some(provider) = provider {
            let result = provider
                .close_session(&context(), Duration::from_millis(100))
                .await;
            self.0.self_close.lock().unwrap().push(result);
        }
        OriginalCall(self.0.clone()).await
    }
}
struct Factory {
    seen: Arc<Seen>,
    scope: FactoryScope,
}
struct Returned {
    product: Option<ContextualToolProduct>,
    seen: Arc<Seen>,
    ready: BoxFuture<'static, ()>,
}
impl Future for Returned {
    type Output = Result<ContextualToolProduct, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ready.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(Ok(self.product.take().unwrap()))
    }
}
impl Drop for Returned {
    fn drop(&mut self) {
        self.seen.create_drop.block();
    }
}
impl ContextualToolProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "actual-tool-product".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        format!(
            "ordinary-counter-v1:cleanup={}",
            self.seen.cleanup_enabled.load(Ordering::SeqCst)
        )
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    fn create_tool(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolProduct, ExtensionError>> {
        if self.seen.cleanup_enabled.load(Ordering::SeqCst) {
            let ready = self.seen.cleanup_release.clone();
            context
                .cleanup()
                .spawn_checked(scheduling::Cleanup {
                    ready: Box::pin(async move {
                        let _ = ready.acquire_owned().await.unwrap();
                    }),
                    seen: self.seen.clone(),
                })
                .unwrap();
        }
        self.seen.creating.notify_one();
        let ready = self.seen.ready.clone();
        Box::pin(Returned {
            ready: Box::pin(async move {
                let _ = ready.acquire_owned().await.unwrap();
            }),
            product: Some(ContextualToolProduct::new(
                ToolInfo {
                    name: "count".into(),
                    description: String::new(),
                    parameters: Value::Null,
                    retry_safe: false,
                    required_permissions: vec![],
                },
                if self.seen.literal_adapter.load(Ordering::SeqCst) != 0 {
                    defaults::tool(self.seen.clone())
                } else if self.seen.literal.load(Ordering::SeqCst) {
                    Box::new(literal::Tool(Tool(self.seen.clone())))
                } else {
                    Box::new(Tool(self.seen.clone()))
                },
            )),
            seen: self.seen.clone(),
        })
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
fn context() -> SessionContext {
    SessionContext::new(SessionId("tool-product".into()), "workspace", "/workspace")
}
fn setup(seen: Arc<Seen>, scope: FactoryScope) -> (ContextualPlanProvider, Arc<Host>) {
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_tool_products(
        Registry::new(),
        vec![Arc::new(Factory { seen, scope })],
        Duration::from_secs(3),
        host.clone(),
    )
    .unwrap();
    (provider, host)
}
fn graph(host: &Host) -> ContextualGenerationObservation {
    host.0.lock().unwrap().last().unwrap().clone()
}
async fn entered(gate: &Gate) {
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_executor_clone_release_gates_original_box_disposal() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let seen = Seen::new(Call::Ready);
        let (provider, host) = setup(seen.clone(), scope);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        let executor = plan.tools[0].executor.clone();
        let last = executor.clone();
        drop(plan);
        let graph = graph(&host);
        let node = graph.tool_product(0).unwrap();
        graph.request_close().unwrap();
        let refused = executor.execute(Value::Null).await;
        let first = node.disposed(Duration::from_millis(20)).await;
        drop(executor);
        let second = node.disposed(Duration::from_millis(20)).await;
        let before = seen.drops.load(Ordering::SeqCst);
        drop(last);
        let disposed = node.disposed(Duration::from_secs(3)).await;
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert!(refused.is_err() && first.is_err() && second.is_err());
        assert_eq!(before, 0);
        assert!(disposed.is_ok());
        assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exposure_capacity_releases_only_on_final_clone_and_rolls_back_plans() {
    let seen = Seen::new(Call::Ready);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let mut facades = Vec::new();
    for _ in 0..64 {
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        facades.push(plan.tools[0].executor.clone());
        drop(plan);
    }
    let full = provider.acquire_plan_in(&context()).await;
    assert!(full.is_err());
    let last = facades.pop().unwrap();
    let clone = last.clone();
    drop(last);
    assert!(provider.acquire_plan_in(&context()).await.is_err());
    drop(clone);
    drop(provider.acquire_plan_in(&context()).await.unwrap());
    drop(facades);
    for _ in 0..80 {
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        plan.tools[0].executor.execute(Value::Null).await.unwrap();
        drop(plan);
    }
    let graph = graph(&host);
    graph.request_close().unwrap();
    let disposed = graph
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(disposed.is_ok());
    assert_eq!(seen.effects.load(Ordering::SeqCst), 80);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[path = "contextual_tool_products/retention.rs"]
mod retention;

#[path = "contextual_tool_products/literal.rs"]
mod literal;

#[path = "contextual_tool_products/defaults.rs"]
mod defaults;

fn identity() -> crabber_core::ResumableToolIdentity {
    crabber_core::ResumableToolIdentity {
        component_id: "mutable".into(),
        component_version: "1".into(),
        config_fingerprint: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        checkpoint_schema_id: "mutable".into(),
        checkpoint_schema_version: "1".into(),
    }
}
#[path = "contextual_tool_products/failures.rs"]
mod failures;

#[async_trait]
impl ResumableTool for Tool {
    fn identity(&self) -> crabber_core::ResumableToolIdentity {
        identity()
    }
    async fn start(
        &self,
        _: ToolContext,
        _: Value,
    ) -> Result<crabber_core::ToolPhaseOutcome, ExtensionError> {
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        Err(ExtensionError::Tool(
            "must not start resumable product".into(),
        ))
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: crabber_core::ToolCheckpoint,
        _: Vec<crabber_core::AcceptedToolResponse>,
    ) -> Result<crabber_core::ToolPhaseOutcome, ExtensionError> {
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        Err(ExtensionError::Tool(
            "must not resume ordinary product".into(),
        ))
    }
}

#[path = "contextual_tool_products/scheduling.rs"]
mod scheduling;
