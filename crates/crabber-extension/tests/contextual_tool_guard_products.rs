//! Original synchronous guard/reference/destructor observations, not policy credit.
use async_trait::async_trait;
use crabber_core::{RunId, SessionContext, SessionId, ToolCallId, ToolInfo};
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
const BOUND: Duration = Duration::from_secs(3);
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
    async fn entered(&self) {
        tokio::time::timeout(BOUND, self.entered.notified())
            .await
            .unwrap();
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
struct Seen {
    made: AtomicUsize,
    ids: AtomicUsize,
    ordinary: AtomicUsize,
    contextual: AtomicUsize,
    drops: AtomicUsize,
    decision: AtomicUsize,
    identity: AtomicUsize,
    mode: AtomicUsize,
    block_id: AtomicBool,
    cross_unblock: AtomicBool,
    box_panic: AtomicBool,
    call: Arc<Gate>,
    id_gate: Arc<Gate>,
    box_drop: Arc<Gate>,
    future_drop: Arc<Gate>,
    registry: Mutex<Option<Registry>>,
    hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    payload_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    drop_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl Seen {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            made: AtomicUsize::new(0),
            ids: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
            contextual: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            decision: AtomicUsize::new(0),
            identity: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            block_id: AtomicBool::new(false),
            cross_unblock: AtomicBool::new(false),
            box_panic: AtomicBool::new(false),
            call: Gate::new(true),
            id_gate: Gate::new(true),
            box_drop: Gate::new(true),
            future_drop: Gate::new(true),
            registry: Mutex::new(None),
            hook: Mutex::new(None),
            payload_hook: Mutex::new(None),
            drop_hook: Mutex::new(None),
        })
    }
    fn decision(&self) -> GuardDecision {
        match self.mode.load(Ordering::SeqCst) {
            1 => self.call.block(),
            2 => {
                let hook = self.payload_hook.lock().unwrap().clone();
                std::panic::panic_any(Payload(self.call.clone(), hook));
            }
            _ => {}
        }
        match self.decision.load(Ordering::SeqCst) {
            0 => GuardDecision::Allow,
            1 => GuardDecision::Ask,
            2 => GuardDecision::Abstain,
            _ => GuardDecision::Deny,
        }
    }
}
struct Payload(Arc<Gate>, Option<Arc<dyn Fn() + Send + Sync>>);
impl Drop for Payload {
    fn drop(&mut self) {
        if let Some(hook) = &self.1 {
            hook();
        }
        self.0.block();
    }
}
struct Body(Arc<Seen>);
impl ToolGuard for Body {
    fn id(&self) -> &str {
        self.0.ids.fetch_add(1, Ordering::SeqCst);
        let registry = self.0.registry.lock().unwrap().clone();
        if let Some(registry) = registry {
            drop(registry.try_acquire(&SessionId::new()));
        }
        if self.0.block_id.load(Ordering::SeqCst) {
            self.0.id_gate.block();
        }
        match self.0.identity.load(Ordering::SeqCst) {
            1 => "changed",
            2 => panic!("original guard metadata panic"),
            3 => "",
            _ => "guard",
        }
    }
    fn check(&self, name: &str, arguments: &Value) -> GuardDecision {
        assert_eq!(name, "tool");
        assert_eq!(arguments, &Value::Null);
        self.0.ordinary.fetch_add(1, Ordering::SeqCst);
        let hook = self.0.hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        self.0.decision()
    }
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        assert_eq!(context.tool.name, "tool");
        assert_eq!(context.arguments, &Value::Null);
        assert_eq!(context.session_id.0, "guard-session");
        assert_eq!(context.run_id.0, "guard-run");
        assert_eq!(context.call_id.0, "guard-call");
        self.0.contextual.fetch_add(1, Ordering::SeqCst);
        let hook = self.0.hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        self.0.decision()
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        let hook = self.0.drop_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        if self.0.cross_unblock.load(Ordering::SeqCst) {
            self.0.future_drop.release();
        }
        self.0.box_drop.block();
        self.0.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.0.box_panic.load(Ordering::SeqCst),
            "original guard Box Drop panic"
        );
    }
}
struct Created {
    product: Option<ContextualToolGuardProduct>,
    seen: Arc<Seen>,
}
impl Future for Created {
    type Output = Result<ContextualToolGuardProduct, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self.product.take().unwrap()))
    }
}
impl Drop for Created {
    fn drop(&mut self) {
        self.seen.future_drop.block();
    }
}
struct Factory(Arc<Seen>, FactoryScope);
impl ContextualToolGuardProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "unique-guard".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        self.1
    }
    fn create_tool_guard(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolGuardProduct, ExtensionError>> {
        self.0.made.fetch_add(1, Ordering::SeqCst);
        Box::pin(Created {
            product: Some(ContextualToolGuardProduct::new(Box::new(Body(
                self.0.clone(),
            )))),
            seen: self.0.clone(),
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
    SessionContext::new(SessionId("guard-session".into()), "workspace", "/workspace")
}
fn setup(seen: &Arc<Seen>, scope: FactoryScope) -> (ContextualPlanProvider, Arc<Host>) {
    let host = Arc::new(Host::default());
    let registry = Registry::new();
    *seen.registry.lock().unwrap() = Some(registry.clone());
    let provider = ContextualPlanProvider::new_with_tool_guard_products(
        registry,
        vec![Arc::new(Factory(seen.clone(), scope))],
        BOUND,
        host.clone(),
    )
    .unwrap();
    (provider, host)
}
fn graph(host: &Host) -> ContextualGenerationObservation {
    host.0.lock().unwrap().last().unwrap().clone()
}
fn contextual(guard: &dyn ToolGuard) -> GuardDecision {
    guard.check_with_context(GuardContext {
        tool: &ToolInfo {
            name: "tool".into(),
            description: String::new(),
            parameters: Value::Null,
            retry_safe: false,
            required_permissions: vec![],
        },
        arguments: &Value::Null,
        call_id: &ToolCallId("guard-call".into()),
        session_id: &SessionId("guard-session".into()),
        run_id: &RunId("guard-run".into()),
    })
}
async fn drain(owner: CallbackInvocationOwner, resources: &ToolResourceOwner) {
    drop(owner);
    resources.close();
    let joined = resources.drain(BOUND).await;
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn last_real_guard_reference_gates_box_and_closed_checks_never_touch_original() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen, FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let guard = plan.guards[0].clone();
    drop(plan);
    let last = guard.clone();
    assert_eq!(seen.ids.load(Ordering::SeqCst), 1);
    assert_eq!(guard.check("tool", &Value::Null), GuardDecision::Deny);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        assert_eq!(guard.check("tool", &Value::Null), GuardDecision::Allow);
        assert_eq!(contextual(guard.as_ref()), GuardDecision::Allow);
        graph(&host).request_close().unwrap();
        assert_eq!(guard.check("tool", &Value::Null), GuardDecision::Deny);
        assert_eq!(contextual(guard.as_ref()), GuardDecision::Deny);
    })
    .await;
    assert_eq!(guard.id(), "guard");
    let ids = seen.ids.load(Ordering::SeqCst);
    drop(guard);
    let observed = graph(&host).tool_guard_product(0).unwrap();
    let held = observed.disposed(Duration::from_millis(20)).await;
    let drops = seen.drops.load(Ordering::SeqCst);
    drop(last);
    drain(owner, &resources).await;
    observed.disposed(BOUND).await.unwrap();
    provider.close_all(BOUND).await.unwrap();
    seen.registry.lock().unwrap().take();
    assert!(held.is_err());
    assert_eq!(drops, 0);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(ids, 3);
    assert_eq!(seen.ordinary.load(Ordering::SeqCst), 1);
    assert_eq!(seen.contextual.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_overrides_decisions_and_successful_inline_reaping_exceed_lifetime_capacity() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen, FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let guard = plan.guards[0].clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        for index in 0..130 {
            seen.decision.store(index % 4, Ordering::SeqCst);
            let expected = [
                GuardDecision::Allow,
                GuardDecision::Ask,
                GuardDecision::Abstain,
                GuardDecision::Deny,
            ][index % 4];
            assert_eq!(guard.check("tool", &Value::Null), expected);
            assert_eq!(contextual(guard.as_ref()), expected);
        }
        seen.identity.store(1, Ordering::SeqCst);
        assert_eq!(guard.check("tool", &Value::Null), GuardDecision::Deny);
        assert_eq!(contextual(guard.as_ref()), GuardDecision::Deny);
    })
    .await;
    drop(guard);
    drain(owner, &resources).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    seen.registry.lock().unwrap().take();
    assert_eq!(seen.ordinary.load(Ordering::SeqCst), 130);
    assert_eq!(seen.contextual.load(Ordering::SeqCst), 130);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_box_drop_can_unblock_same_literal_ready_constructor_future_drop() {
    let seen = Seen::new();
    *seen.future_drop.open.lock().unwrap() = false;
    seen.cross_unblock.store(true, Ordering::SeqCst);
    let _release = Release(vec![seen.future_drop.clone()]);
    let (provider, host) = setup(&seen, FactoryScope::Session);
    let actual = provider.clone();
    let waiter = tokio::spawn(async move { actual.acquire_plan_in(&context()).await });
    seen.future_drop.entered().await;
    waiter.abort();
    let _ = waiter.await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    seen.registry.lock().unwrap().take();
    assert_eq!(seen.made.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_capture_failure_keeps_original_box_tracked_without_publishing_template() {
    for mode in [2, 3] {
        let seen = Seen::new();
        seen.identity.store(mode, Ordering::SeqCst);
        let (provider, host) = setup(&seen, FactoryScope::Session);
        let acquired = provider.acquire_plan_in(&context()).await;
        graph(&host).request_close().unwrap();
        let disposed = graph(&host)
            .tool_guard_product(0)
            .unwrap()
            .disposed(BOUND)
            .await;
        let closed = provider.close_all(BOUND).await;
        seen.registry.lock().unwrap().take();
        assert!(acquired.is_err() && disposed.is_err() && closed.is_err());
        assert_eq!(seen.made.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        assert_eq!(seen.ordinary.load(Ordering::SeqCst), 0);
    }
}
#[path = "contextual_tool_guard_products/legacy.rs"]
mod legacy;
#[path = "contextual_tool_guard_products/retention.rs"]
mod retention;

#[path = "contextual_tool_guard_products/frames.rs"]
mod frames;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspace_guard_survives_first_session_release_until_real_sibling_reference_release() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen, FactoryScope::Workspace);
    let first = provider.acquire_plan_in(&context()).await.unwrap();
    let a = first.guards[0].clone();
    drop(first);
    let second_context =
        SessionContext::new(SessionId("sibling".into()), "workspace", "/workspace");
    let second = provider.acquire_plan_in(&second_context).await.unwrap();
    let b = second.guards[0].clone();
    drop(second);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        assert_eq!(a.check("tool", &Value::Null), GuardDecision::Allow);
    })
    .await;
    drop(a);
    drain(owner, &resources).await;
    provider.close_session(&context(), BOUND).await.unwrap();
    let before = seen.drops.load(Ordering::SeqCst);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        assert_eq!(b.check("tool", &Value::Null), GuardDecision::Allow);
    })
    .await;
    drop(b);
    drain(owner, &resources).await;
    provider.close_all(BOUND).await.unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    seen.registry.lock().unwrap().take();
    assert_eq!(before, 0);
    assert_eq!(seen.made.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_guard_box_disposal_is_actual_and_failure_remains_sticky_without_calls() {
    for panic in [false, true] {
        let seen = Seen::new();
        seen.box_panic.store(panic, Ordering::SeqCst);
        *seen.box_drop.open.lock().unwrap() = false;
        let _release = Release(vec![seen.box_drop.clone()]);
        let (provider, host) = setup(&seen, FactoryScope::Session);
        drop(provider.acquire_plan_in(&context()).await.unwrap());
        graph(&host).request_close().unwrap();
        seen.box_drop.entered().await;
        let observed = graph(&host).tool_guard_product(0).unwrap();
        let held = observed.disposed(Duration::from_millis(20)).await;
        seen.box_drop.release();
        let first = observed.disposed(BOUND).await;
        let second = observed.disposed(BOUND).await;
        let closed = provider.close_all(BOUND).await;
        seen.registry.lock().unwrap().take();
        assert!(held.is_err());
        assert_eq!(first.is_err(), panic);
        assert_eq!(second.is_err(), panic);
        assert_eq!(closed.is_err(), panic);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        assert_eq!(seen.ordinary.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guard_projection_capacity_refuses_without_calling_original_metadata_under_locks() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen, FactoryScope::Session);
    let mut plans = Vec::new();
    for _ in 0..64 {
        plans.push(provider.acquire_plan_in(&context()).await.unwrap());
    }
    let refused = provider.acquire_plan_in(&context()).await;
    let calls = seen.ids.load(Ordering::SeqCst);
    drop(plans);
    provider.close_all(BOUND).await.unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    seen.registry.lock().unwrap().take();
    assert!(refused.is_err());
    assert_eq!(calls, 1);
    assert_eq!(seen.ordinary.load(Ordering::SeqCst), 0);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
