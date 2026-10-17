//! Actual Box subprocess reference, borrowed-call and destructor ownership. OS child processes remain unknown.
use async_trait::async_trait;
use crabber_core::{SessionContext, SessionId};
use crabber_extension::*;
use futures::future::BoxFuture;
use serde_json::{Value, json};
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
struct Seen {
    effects: AtomicUsize,
    drops: AtomicUsize,
    called: Notify,
    pending: AtomicBool,
    call_drop: Arc<Gate>,
    body_drop: Arc<Gate>,
    create_drop: Arc<Gate>,
    create_ready: Arc<tokio::sync::Semaphore>,
    call_panic: AtomicBool,
    body_panic: AtomicBool,
    frames: Mutex<Option<Arc<context::Frames>>>,
    unblock_constructor: Mutex<Option<Arc<Gate>>>,
}
impl Seen {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            effects: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            called: Notify::new(),
            pending: AtomicBool::new(false),
            call_drop: Gate::new(true),
            body_drop: Gate::new(true),
            create_drop: Gate::new(true),
            create_ready: Arc::new(tokio::sync::Semaphore::new(1)),
            call_panic: AtomicBool::new(false),
            body_panic: AtomicBool::new(false),
            frames: Mutex::new(None),
            unblock_constructor: Mutex::new(None),
        })
    }
}
struct Process(Arc<Seen>);
impl Drop for Process {
    fn drop(&mut self) {
        self.0.body_drop.block();
        let unblock = self.0.unblock_constructor.lock().unwrap().clone();
        if let Some(gate) = unblock {
            gate.release();
        }
        self.0.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.0.body_panic.load(Ordering::SeqCst),
            "original process Drop panic"
        );
    }
}
struct Run {
    seen: Arc<Seen>,
    value: Value,
}
impl Future for Run {
    type Output = Result<Value, String>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.seen.pending.load(Ordering::SeqCst) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(self.value.clone()))
        }
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        if let Some(frames) = self.seen.frames.lock().unwrap().clone() {
            frames.disposed();
        }
        self.seen.call_drop.block();
        assert!(
            !self.seen.call_panic.load(Ordering::SeqCst),
            "original subprocess run future Drop panic"
        );
    }
}
#[async_trait]
impl Subprocess for Process {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        let frames = self.0.frames.lock().unwrap().clone();
        if let Some(frames) = frames {
            frames.read().await;
        }
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        self.0.called.notify_one();
        Run {
            seen: self.0.clone(),
            value: json!([program, arguments]),
        }
        .await
    }
}
struct Factory {
    seen: Arc<Seen>,
    scope: FactoryScope,
}
struct Created {
    product: Option<ContextualSubprocessProduct>,
    ready: Pin<Box<dyn Future<Output = ()> + Send>>,
    seen: Arc<Seen>,
}
impl Future for Created {
    type Output = Result<ContextualSubprocessProduct, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        std::task::ready!(self.ready.as_mut().poll(context));
        Poll::Ready(Ok(self.product.take().unwrap()))
    }
}
impl Drop for Created {
    fn drop(&mut self) {
        self.seen.create_drop.block();
    }
}
impl ContextualSubprocessProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "box-subprocess".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    fn create_subprocess(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualSubprocessProduct, ExtensionError>> {
        let ready = self.seen.create_ready.clone();
        Box::pin(Created {
            product: Some(ContextualSubprocessProduct::new(Box::new(Process(
                self.seen.clone(),
            )))),
            ready: Box::pin(async move {
                ready.acquire_owned().await.unwrap().forget();
            }),
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
    SessionContext::new(
        SessionId("subprocess-product".into()),
        "workspace",
        "/workspace",
    )
}
fn setup(seen: Arc<Seen>, scope: FactoryScope) -> (ContextualPlanProvider, Arc<Host>) {
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_subprocess_products(
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

struct Legacy(Arc<dyn Subprocess>);
#[async_trait]
impl Extension for Legacy {
    fn id(&self) -> &'static str {
        "legacy-fs"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.host_services(HostServices {
            subprocess: Some(self.0.clone()),
            ..HostServices::default()
        });
        Ok(())
    }
}
#[tokio::test]
async fn legacy_arc_reference_has_no_object_disposal_evidence() {
    let seen = Seen::new();
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Legacy(Arc::new(Process(seen.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let process = plan.host_services.subprocess.clone().unwrap();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
    let refused = process
        .run("after-close", &["original argument".into()])
        .await;
    let observed = seen.drops.load(Ordering::SeqCst);
    drop(process);
    drop(mount);
    drop(registry);
    assert!(refused.is_err());
    assert_eq!(
        observed, 0,
        "legacy invocation join never certified object disposal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_process_clone_release_gates_original_box_disposal() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let seen = Seen::new();
        let (provider, host) = setup(seen.clone(), scope);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        let process = plan.host_services.subprocess.clone().unwrap();
        let last = process.clone();
        drop(plan);
        let original = graph(&host);
        let node = original.subprocess_product(0).unwrap();
        original.request_close().unwrap();
        let refused = process
            .run("after-close", &["original argument".into()])
            .await;
        let first = node.disposed(Duration::from_millis(20)).await;
        drop(process);
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
async fn actual_phase_owner_retains_cancelled_original_read_disposal() {
    let seen = Seen::new();
    seen.pending.store(true, Ordering::SeqCst);
    *seen.call_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.call_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let process = plan.host_services.subprocess.clone().unwrap();
    drop(plan);
    // Missing actual caller ownership refuses before the original read starts.
    assert!(
        process
            .run("unowned", &["original argument".into()])
            .await
            .is_err()
    );
    assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let call = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        process
            .run("held/path", &["original argument".into()])
            .await
    }));
    seen.called.notified().await;
    call.abort();
    let _ = call.await;
    entered(&seen.call_drop).await;
    drop(owner);
    resources.close();
    let pending = resources.drain(Duration::from_millis(20)).await;
    let original = graph(&host);
    original.request_close().unwrap();
    let node = original.subprocess_product(0).unwrap();
    let before = node.disposed(Duration::from_millis(20)).await;
    let drops = seen.drops.load(Ordering::SeqCst);
    seen.call_drop.release();
    let joined = resources.drain(Duration::from_secs(3)).await;
    let disposed = node.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(pending.pending > 0 && before.is_err());
    assert_eq!(drops, 0);
    assert_eq!(joined.pending, 0);
    assert!(joined.failures.is_empty() && disposed.is_ok());
    assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn facade_capacity_rollback_and_sequential_original_join_reaping() {
    let seen = Seen::new();
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let mut facades = Vec::new();
    for _ in 0..64 {
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        facades.push(plan.host_services.subprocess.clone().unwrap());
        drop(plan);
    }
    assert!(provider.acquire_plan_in(&context()).await.is_err());
    let last = facades.pop().unwrap();
    let clone = last.clone();
    drop(last);
    assert!(provider.acquire_plan_in(&context()).await.is_err());
    drop(clone);
    drop(provider.acquire_plan_in(&context()).await.unwrap());
    drop(facades);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        for index in 0..80 {
            let plan = provider.acquire_plan_in(&context()).await.unwrap();
            let path = format!("directory/{index}");
            assert_eq!(
                plan.host_services
                    .subprocess
                    .as_ref()
                    .unwrap()
                    .run(&path, &["original argument".into()])
                    .await
                    .unwrap(),
                json!([path, ["original argument"]])
            );
            drop(plan);
        }
    })
    .await;
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    let original = graph(&host);
    original.request_close().unwrap();
    let disposed = original
        .subprocess_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert_eq!(joined.pending, 0);
    assert!(joined.failures.is_empty() && disposed.is_ok());
    assert_eq!(seen.effects.load(Ordering::SeqCst), 80);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_box_drop_starts_before_original_create_future_disposal_finishes() {
    let seen = Seen::new();
    *seen.create_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.create_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context()).await }
    });
    entered(&seen.create_drop).await;
    let close = tokio::spawn({
        let provider = provider.clone();
        async move { provider.close_all(Duration::from_secs(3)).await }
    });
    let started =
        tokio::time::timeout(Duration::from_secs(1), seen.body_drop.entered.notified()).await;
    let original = graph(&host);
    let node = original.subprocess_product(0).unwrap();
    let pending = node.disposed(Duration::from_millis(20)).await;
    seen.create_drop.release();
    drop(acquire.await.unwrap());
    close.await.unwrap().unwrap();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    assert!(started.is_ok() && pending.is_err() && disposed.is_ok());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_read_disposal_failure_is_sticky_for_both_actual_parents() {
    let seen = Seen::new();
    seen.call_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let process = plan.host_services.subprocess.clone().unwrap();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_callback_invocations(
        owner.capability(),
        process.run("panic", &["original argument".into()]),
    )
    .await;
    drop(process);
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    let original = graph(&host);
    original.request_close().unwrap();
    let disposed = original
        .subprocess_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    assert!(result.is_err() && !joined.failures.is_empty() && disposed.is_err() && closed.is_err());
    assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
}

#[path = "contextual_subprocess_products/context.rs"]
mod context;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_box_destructor_can_unblock_original_constructor_future_drop() {
    let seen = Seen::new();
    *seen.create_drop.open.lock().unwrap() = false;
    *seen.unblock_constructor.lock().unwrap() = Some(seen.create_drop.clone());
    let _release = Release(vec![seen.create_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context()).await }
    });
    entered(&seen.create_drop).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    drop(acquire.await.unwrap());
    graph(&host)
        .subprocess_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_original_box_drop_never_produces_disposed_evidence() {
    let seen = Seen::new();
    seen.body_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    drop(provider.acquire_plan_in(&context()).await.unwrap());
    let original = graph(&host);
    let node = original.subprocess_product(0).unwrap();
    original.request_close().unwrap();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    assert!(disposed.is_err() && closed.is_err());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert!(node.disposed(Duration::from_secs(3)).await.is_err());
}
