//! Actual returned Box and original destructor ownership, no physical policy.
use crabber_core::{SessionContext, SessionId};
use crabber_extension::*;
use futures::future::BoxFuture;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
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
    created: AtomicUsize,
    dropped: AtomicUsize,
    root_drop: Arc<Gate>,
    future_drop: Arc<Gate>,
    panic_root: bool,
    panic_payload: std::sync::atomic::AtomicBool,
    panic_future: bool,
    provider: Mutex<Option<ContextualPlanProvider>>,
    self_close: Mutex<Vec<Result<(), ExtensionError>>>,
}
#[derive(Clone, Copy)]
enum Failure {
    None,
    Root,
    Future,
}
impl Seen {
    fn new(held_root: bool, held_future: bool, failure: Failure) -> Arc<Self> {
        Arc::new(Self {
            created: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            root_drop: Gate::new(!held_root),
            future_drop: Gate::new(!held_future),
            panic_root: matches!(failure, Failure::Root),
            panic_payload: std::sync::atomic::AtomicBool::new(false),
            panic_future: matches!(failure, Failure::Future),
            provider: Mutex::new(None),
            self_close: Mutex::new(vec![]),
        })
    }
}
struct Root(Arc<Seen>);
impl Drop for Root {
    fn drop(&mut self) {
        self.0.root_drop.block();
        self.0.close_self();
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
        if self.0.panic_root {
            if self.0.panic_payload.load(Ordering::SeqCst) {
                std::panic::panic_any(Payload(self.0.clone()));
            }
            panic!("original root destructor panic");
        }
    }
}
impl Seen {
    fn close_self(&self) {
        let provider = self.provider.lock().unwrap().clone();
        if let Some(provider) = provider {
            let result = futures::executor::block_on(
                provider.close_session(&context(), Duration::from_millis(100)),
            );
            self.self_close.lock().unwrap().push(result);
        }
    }
}
struct Payload(Arc<Seen>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.close_self();
        panic!("panic payload destructor");
    }
}
struct Factory {
    seen: Arc<Seen>,
    scope: FactoryScope,
    config: &'static str,
}
impl ContextualProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "product-root-fixture".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        self.config.into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    fn create_root(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<Box<dyn ContextualProductRoot>, ExtensionError>> {
        self.seen.created.fetch_add(1, Ordering::SeqCst);
        Box::pin(ReadyRoot {
            root: Some(Box::new(Root(self.seen.clone()))),
            seen: self.seen.clone(),
        })
    }
}
struct ReadyRoot {
    root: Option<Box<dyn ContextualProductRoot>>,
    seen: Arc<Seen>,
}
impl Future for ReadyRoot {
    type Output = Result<Box<dyn ContextualProductRoot>, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self.root.take().unwrap()))
    }
}
impl Drop for ReadyRoot {
    fn drop(&mut self) {
        self.seen.future_drop.block();
        assert!(
            !self.seen.panic_future,
            "original create-future destructor panic"
        );
    }
}
#[derive(Default)]
struct Host {
    observations: Mutex<Vec<ContextualGenerationObservation>>,
}
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        self.observations
            .lock()
            .unwrap()
            .push(work.observation().unwrap());
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
fn provider(host: Arc<Host>, seen: Arc<Seen>, scope: FactoryScope) -> ContextualPlanProvider {
    ContextualPlanProvider::new_with_product_roots(
        Registry::new(),
        vec![Arc::new(Factory {
            seen,
            scope,
            config: "v1",
        })],
        Duration::from_secs(3),
        host,
    )
    .unwrap()
}
fn context() -> SessionContext {
    SessionContext::new(SessionId("root".into()), "workspace", "/workspace")
}
fn graph(host: &Host) -> ContextualGenerationObservation {
    host.observations.lock().unwrap().last().unwrap().clone()
}
async fn entered(gate: &Gate) {
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_blocking_drop_survives_waiter_abort_and_repeated_observation() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let seen = Seen::new(true, false, Failure::None);
        let _release = Release(vec![seen.root_drop.clone(), seen.future_drop.clone()]);
        let host = Arc::new(Host::default());
        let provider = provider(host.clone(), seen.clone(), scope);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        assert!(
            plan.tools.is_empty()
                && plan.providers.is_empty()
                && plan.host_services.subprocess.is_none()
        );
        assert!(
            plan.components
                .iter()
                .any(|component| component.id == "contextual.product-root-cell")
        );
        drop(plan);
        let graph = graph(&host);
        let root = graph.product_root(0).unwrap();
        graph.request_close().unwrap();
        entered(&seen.root_drop).await;
        let pending = root.disposed(Duration::from_millis(20)).await;
        let waiter = tokio::spawn({
            let root = root.clone();
            async move { root.disposed(Duration::from_secs(3)).await }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        let aborted = waiter.await;
        seen.root_drop.release();
        let first = root.disposed(Duration::from_secs(3)).await.unwrap();
        let second = root.disposed(Duration::from_secs(3)).await.unwrap();
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert!(pending.is_err());
        assert!(matches!(aborted, Err(error) if error.is_cancelled()));
        assert_eq!(first.generation(), second.generation());
        assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_box_drop_can_unblock_original_create_future_disposal() {
    let seen = Seen::new(true, true, Failure::None);
    let _release = Release(vec![seen.root_drop.clone(), seen.future_drop.clone()]);
    let host = Arc::new(Host::default());
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context()).await }
    });
    entered(&seen.future_drop).await;
    let graph = graph(&host);
    let root = graph.product_root(0).unwrap();
    graph.request_close().unwrap();
    entered(&seen.root_drop).await;
    seen.root_drop.release();
    while seen.dropped.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let pending = root.disposed(Duration::from_millis(20)).await;
    seen.future_drop.release();
    let result = acquire.await.unwrap();
    let disposed = root.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(
        pending.is_err(),
        "root Drop alone cannot replace original create-future join"
    );
    assert!(result.is_err());
    assert!(disposed.is_ok());
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
}

#[path = "contextual_product_roots/failures.rs"]
mod failures;
#[path = "contextual_product_roots/legacy.rs"]
mod legacy;
#[path = "contextual_product_roots/teardown.rs"]
mod teardown;

#[path = "contextual_product_roots/handoff.rs"]
mod handoff;
