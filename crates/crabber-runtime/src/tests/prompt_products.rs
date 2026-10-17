//! Actual attempt callback ownership; no surrogate Run owner or policy credit.
use super::*;
use crabber_extension::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationObservation, ContextualGenerationWork, ContextualOwnedProductFactory,
    ContextualPlanProvider, ContextualPromptProduct, ContextualPromptProductFactory,
    FactoryContext, FactoryScope, MiddlewareDescriptor, ModelAttemptContext, PromptAttemptContext,
    Registry, SystemPromptMiddleware,
};
use futures::future::BoxFuture;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
struct Gate {
    entered: Notify,
    disposing: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            disposing: Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn dispose(&self) {
        self.disposing.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.open.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
struct Seen {
    mode: usize,
    gate: Arc<Gate>,
    observed: Mutex<Vec<(SessionId, RunId)>>,
    drops: AtomicUsize,
}
struct Body(Arc<Seen>);
impl Body {
    fn native(
        &self,
        context: &PromptAttemptContext,
    ) -> BoxFuture<'static, Result<Option<String>, ExtensionError>> {
        self.0
            .observed
            .lock()
            .unwrap()
            .push((context.session_id().clone(), context.run_id().clone()));
        Box::pin(Original {
            seen: self.0.clone(),
            semantic: || ExtensionError::Plan("semantic prompt failure".into()),
        })
    }
}
impl SystemPromptMiddleware for Body {
    fn contribute<'life0, 'async_trait>(
        &'life0 self,
        context: ModelAttemptContext,
    ) -> BoxFuture<'async_trait, Result<Option<String>, String>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.0
            .observed
            .lock()
            .unwrap()
            .push((context.session_id().clone(), context.run_id().clone()));
        Box::pin(Original {
            seen: self.0.clone(),
            semantic: || "semantic prompt failure".into(),
        })
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Original<E> {
    seen: Arc<Seen>,
    semantic: fn() -> E,
}
impl<E> Future for Original<E> {
    type Output = Result<Option<String>, E>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.seen.gate.entered.notify_one();
        if self.seen.mode == 2 {
            Poll::Pending
        } else if self.seen.mode == 1 {
            Poll::Ready(Err((self.semantic)()))
        } else {
            Poll::Ready(Ok(Some("actual Box prompt".into())))
        }
    }
}
impl<E> Drop for Original<E> {
    fn drop(&mut self) {
        if self.seen.mode == 2 {
            self.seen.gate.dispose();
        }
        assert_ne!(self.seen.mode, 3, "literal prompt future disposal panic");
    }
}
struct Factory(Arc<Seen>, bool);
impl ContextualPromptProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "runtime-prompt-product".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        format!("typed={}:mode={}", self.1, self.0.mode)
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Workspace
    }
    fn create_prompt(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualPromptProduct, ExtensionError>> {
        let body = Body(self.0.clone());
        let product = if self.1 {
            ContextualPromptProduct::system_prompt(
                "actual".into(),
                0,
                MiddlewareDescriptor::new("actual", "1", "0".repeat(64)).unwrap(),
                Box::new(body),
            )
        } else {
            ContextualPromptProduct::native(
                "actual".into(),
                0,
                Box::new(move |context| body.native(&context)),
            )
        };
        Box::pin(async move { Ok(product) })
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
async fn case(store: Arc<dyn Store>, typed: bool, mode: usize) {
    let seen = Arc::new(Seen {
        mode,
        gate: Gate::new(),
        observed: Mutex::new(Vec::new()),
        drops: AtomicUsize::new(0),
    });
    let release = Release(seen.gate.clone());
    let host = Arc::new(Host::default());
    let plans = Arc::new(
        ContextualPlanProvider::new_with_owned_products(
            Registry::new(),
            vec![ContextualOwnedProductFactory::Prompt(Arc::new(Factory(
                seen.clone(),
                typed,
            )))],
            Duration::from_secs(3),
            host.clone(),
        )
        .unwrap(),
    );
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(plans.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    let session = handle.session_id().clone();
    if mode == 2 {
        assert_pending(&runtime, store.as_ref(), &handle, &seen).await;
    }
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    let joined = runtime.drain_execution_tasks(Duration::from_secs(3)).await;
    let closed = plans.close_all(Duration::from_secs(3)).await;
    let node = host.0.lock().unwrap()[0].prompt_product(0).unwrap();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    let attempt = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(*seen.observed.lock().unwrap(), [(session, run)]);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(joined.pending, [] as [RunId; 0]);
    match mode {
        0 => {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
            assert_eq!(fake.requests().len(), 1);
            assert!(
                fake.requests()[0]
                    .system
                    .as_ref()
                    .unwrap()
                    .contains("actual Box prompt")
            );
        }
        1 => {
            assert!(result.is_err());
            assert_eq!(attempt.state, crabber_session::AttemptState::Failed);
            assert_eq!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        }
        2 => {
            assert!(matches!(
                result,
                Err(RuntimeError::AttemptCleanupIncomplete { .. })
            ));
            assert_eq!(attempt.state, crabber_session::AttemptState::Started);
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        }
        _ => {
            assert!(result.is_err());
            assert_eq!(attempt.state, crabber_session::AttemptState::Started);
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        }
    }
    if mode != 0 {
        assert_eq!(fake.requests().len(), 0);
    }
    assert_eq!(closed.is_err(), mode == 3);
    assert_eq!(disposed.is_err(), mode == 3);
}
async fn assert_pending(
    runtime: &Orchestrator,
    store: &dyn Store,
    handle: &crate::RunHandle,
    seen: &Seen,
) {
    tokio::time::timeout(Duration::from_secs(3), seen.gate.entered.notified())
        .await
        .unwrap();
    handle.interrupt();
    tokio::time::timeout(Duration::from_secs(3), seen.gate.disposing.notified())
        .await
        .unwrap();
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    let attempt = store
        .latest_execution_attempt(handle.run_id())
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert!(pending.pending.contains(handle.run_id()));
    assert_eq!(attempt.state, crabber_session::AttemptState::Started);
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 0);
}
async fn contract(store: Arc<dyn Store>) {
    for typed in [false, true] {
        for mode in 0..4 {
            case(store.clone(), typed, mode).await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_box_prompt_product_runtime() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_box_prompt_product_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prompt-product.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_box_prompt_product_runtime() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    contract(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
