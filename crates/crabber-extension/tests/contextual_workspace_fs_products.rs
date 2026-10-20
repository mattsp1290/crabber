//! Actual Box filesystem reference, borrowed-call and destructor ownership.
#[path = "contextual_workspace_fs_products/literal.rs"]
mod literal;
use async_trait::async_trait;
use crabber_core::{SessionContext, SessionId};
use crabber_extension::*;
use futures::future::BoxFuture;
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
    literal: AtomicBool,
    literal_poll_panic: AtomicBool,
    semantic_error: AtomicBool,
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
            literal: AtomicBool::new(false),
            literal_poll_panic: AtomicBool::new(false),
            semantic_error: AtomicBool::new(false),
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
struct Filesystem(Arc<Seen>);
impl Drop for Filesystem {
    fn drop(&mut self) {
        self.0.body_drop.block();
        let unblock = self.0.unblock_constructor.lock().unwrap().clone();
        if let Some(gate) = unblock {
            gate.release();
        }
        self.0.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.0.body_panic.load(Ordering::SeqCst),
            "original filesystem Drop panic"
        );
    }
}
struct Read {
    seen: Arc<Seen>,
    path: Vec<u8>,
}
impl Future for Read {
    type Output = Result<Vec<u8>, String>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.seen.literal_poll_panic.load(Ordering::SeqCst),
            "literal filesystem poll panic"
        );
        if self.seen.semantic_error.load(Ordering::SeqCst) {
            return Poll::Ready(Err("literal semantic error".into()));
        }
        if self.seen.pending.load(Ordering::SeqCst) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(self.path.clone()))
        }
    }
}
impl Drop for Read {
    fn drop(&mut self) {
        if let Some(frames) = self.seen.frames.lock().unwrap().clone() {
            frames.disposed();
        }
        self.seen.call_drop.block();
        assert!(
            !self.seen.call_panic.load(Ordering::SeqCst),
            "original filesystem read future Drop panic"
        );
    }
}
#[async_trait]
impl WorkspaceFs for Filesystem {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        let frames = self.0.frames.lock().unwrap().clone();
        if let Some(frames) = frames {
            frames.read().await;
        }
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        self.0.called.notify_one();
        Read {
            seen: self.0.clone(),
            path: path.as_bytes().to_vec(),
        }
        .await
    }
}
struct Factory {
    seen: Arc<Seen>,
    scope: FactoryScope,
}
struct Created {
    product: Option<ContextualWorkspaceFsProduct>,
    ready: Pin<Box<dyn Future<Output = ()> + Send>>,
    seen: Arc<Seen>,
}
impl Future for Created {
    type Output = Result<ContextualWorkspaceFsProduct, ExtensionError>;
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
impl ContextualWorkspaceFsProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "box-fs".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    fn create_workspace_fs(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualWorkspaceFsProduct, ExtensionError>> {
        let ready = self.seen.create_ready.clone();
        Box::pin(Created {
            product: Some(ContextualWorkspaceFsProduct::new(
                if self.seen.literal.load(Ordering::SeqCst) {
                    Box::new(literal::Filesystem(Filesystem(self.seen.clone())))
                } else {
                    Box::new(Filesystem(self.seen.clone()))
                },
            )),
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
    SessionContext::new(SessionId("fs-product".into()), "workspace", "/workspace")
}
fn setup(seen: Arc<Seen>, scope: FactoryScope) -> (ContextualPlanProvider, Arc<Host>) {
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_workspace_fs_products(
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

struct Legacy(Arc<dyn WorkspaceFs>);
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
            workspace_fs: Some(self.0.clone()),
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
            Arc::new(Legacy(Arc::new(Filesystem(seen.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let filesystem = plan.host_services.workspace_fs.clone().unwrap();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
    let refused = filesystem.read("after-close").await;
    let observed = seen.drops.load(Ordering::SeqCst);
    drop(filesystem);
    drop(mount);
    drop(registry);
    assert!(refused.is_err());
    assert_eq!(
        observed, 0,
        "legacy invocation join never certified object disposal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_filesystem_clone_release_gates_original_box_disposal() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let seen = Seen::new();
        let (provider, host) = setup(seen.clone(), scope);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        let filesystem = plan.host_services.workspace_fs.clone().unwrap();
        let last = filesystem.clone();
        drop(plan);
        let original = graph(&host);
        let node = original.workspace_fs_product(0).unwrap();
        original.request_close().unwrap();
        let refused = filesystem.read("after-close").await;
        let first = node.disposed(Duration::from_millis(20)).await;
        drop(filesystem);
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
    let filesystem = plan.host_services.workspace_fs.clone().unwrap();
    drop(plan);
    // Missing actual caller ownership refuses before the original read starts.
    assert!(filesystem.read("unowned").await.is_err());
    assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let call = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        filesystem.read("held/path").await
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
    let node = original.workspace_fs_product(0).unwrap();
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
        facades.push(plan.host_services.workspace_fs.clone().unwrap());
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
                    .workspace_fs
                    .as_ref()
                    .unwrap()
                    .read(&path)
                    .await
                    .unwrap(),
                path.as_bytes()
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
        .workspace_fs_product(0)
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
    let node = original.workspace_fs_product(0).unwrap();
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
    let filesystem = plan.host_services.workspace_fs.clone().unwrap();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_callback_invocations(owner.capability(), filesystem.read("panic")).await;
    drop(filesystem);
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    let original = graph(&host);
    original.request_close().unwrap();
    let disposed = original
        .workspace_fs_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    assert!(result.is_err() && !joined.failures.is_empty() && disposed.is_err() && closed.is_err());
    assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
}

#[path = "contextual_workspace_fs_products/context.rs"]
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
        .workspace_fs_product(0)
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
    let node = original.workspace_fs_product(0).unwrap();
    original.request_close().unwrap();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    assert!(disposed.is_err() && closed.is_err());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert!(node.disposed(Duration::from_secs(3)).await.is_err());
}
