//! Actual Runtime callback owners consume the opt-in Box provider family.
use super::*;
use crabber_core::{InputFingerprint, SessionContext};
use crabber_extension::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationWork, ContextualPlanProvider, ContextualProviderBindingBody,
    ContextualProviderProduct, ContextualProviderProductFactory, ContextualStreamerProduct,
    FactoryContext, FactoryScope, Registry,
};
use crabber_providers::{ProviderBindingError, ProviderInfo};
use futures::{Stream, future::BoxFuture};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Default)]
struct Seen {
    disposal_panic: std::sync::atomic::AtomicBool,
    semantic_error: std::sync::atomic::AtomicBool,
    binds: AtomicUsize,
    creates: AtomicUsize,
    dropped: AtomicUsize,
    entered: Notify,
    disposing: Notify,
    release: (Mutex<bool>, Condvar),
}
struct Release(Arc<Seen>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
struct PendingStream(Arc<Seen>);
impl Stream for PendingStream {
    type Item = StreamDelta;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl Drop for PendingStream {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let mut release = self.0.release.0.lock().unwrap();
        while !*release {
            release = self.0.release.1.wait(release).unwrap();
        }
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Backend {
    seen: Arc<Seen>,
    fake: FakeProvider,
    blocked: bool,
}
#[async_trait]
impl Streamer for Backend {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.seen.creates.fetch_add(1, Ordering::SeqCst);
        if self.seen.disposal_panic.load(Ordering::SeqCst) {
            return Ok(Box::pin(DisposalPanic {
                seen: self.seen.clone(),
                text: false,
            }));
        }
        if self.seen.semantic_error.load(Ordering::SeqCst) {
            return Err(ProviderError {
                kind: crabber_providers::ProviderErrorKind::Invalid,
                message: "genuine semantic refusal".into(),
                retryable: false,
            });
        }
        if self.blocked {
            Ok(Box::pin(PendingStream(self.seen.clone())))
        } else {
            self.fake.stream(request).await
        }
    }
}
struct Body {
    seen: Arc<Seen>,
    fake: FakeProvider,
    blocked: bool,
}
impl ContextualProviderBindingBody for Body {
    fn bind_box(
        &self,
        selection: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        self.seen.binds.fetch_add(1, Ordering::SeqCst);
        let descriptor = Resolver::binding_descriptor(&self.fake, &selection).unwrap();
        Box::pin(async move {
            Ok(ContextualStreamerProduct::new(
                descriptor,
                Box::new(Backend {
                    seen: self.seen.clone(),
                    fake: self.fake.clone(),
                    blocked: self.blocked,
                }),
            ))
        })
    }
}
struct Factory {
    seen: Arc<Seen>,
    fake: FakeProvider,
    blocked: bool,
}
impl ContextualProviderProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "runtime-provider-product".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "fixed-route".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_provider(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualProviderProduct, ExtensionError>> {
        Box::pin(async move {
            Ok(ContextualProviderProduct::new(
                ProviderInfo {
                    id: "fake".into(),
                    name: "Actual Box".into(),
                },
                vec![Resolver::binding_descriptor(&self.fake, &request().selection).unwrap()],
                Box::new(Body {
                    seen: self.seen.clone(),
                    fake: self.fake.clone(),
                    blocked: self.blocked,
                }),
            ))
        })
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
fn runtime(
    store: Arc<dyn Store>,
    blocked: bool,
) -> (
    Orchestrator,
    Arc<ContextualPlanProvider>,
    Arc<Seen>,
    FakeProvider,
) {
    let seen = Arc::new(Seen::default());
    let fake = FakeProvider::scripted(vec![text_script("actual Box result")]);
    let plans = Arc::new(
        ContextualPlanProvider::new_with_provider_products(
            Registry::new(),
            vec![Arc::new(Factory {
                seen: seen.clone(),
                fake: fake.clone(),
                blocked,
            })],
            Duration::from_secs(2),
            Arc::new(Host),
        )
        .unwrap(),
    );
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(plans.clone())
        .build()
        .unwrap();
    (runtime, plans, seen, fake)
}
async fn signal(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(5), notify.notified())
        .await
        .unwrap();
}
async fn contract(store: Arc<dyn Store>) {
    for blocked in [false, true] {
        let (runtime, plans, seen, fake) = runtime(store.clone(), blocked);
        let release = Release(seen.clone());
        let handle = runtime.start(request()).await.unwrap();
        let context = SessionContext::new(handle.session_id().clone(), "test", "/tmp");
        let run = handle.run_id().clone();
        if blocked {
            signal(&seen.entered).await;
            handle.interrupt();
            signal(&seen.disposing).await;
            let report = runtime
                .drain_execution_tasks(Duration::from_millis(20))
                .await;
            assert!(report.pending.contains(&run));
            let attempt = store
                .latest_execution_attempt(&run)
                .await
                .unwrap()
                .unwrap()
                .attempt;
            assert_eq!(attempt.state, crabber_session::AttemptState::Started);
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        }
        drop(release);
        let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
            .await
            .unwrap();
        runtime
            .drain_session_execution_tasks(context.session_id(), Duration::from_secs(2))
            .await;
        plans
            .close_session(&context, Duration::from_secs(2))
            .await
            .unwrap();
        if blocked {
            assert_eq!(seen.dropped.load(Ordering::SeqCst), 1, "{result:?}");
        } else {
            result.unwrap();
            assert_eq!(fake.requests().len(), 1);
        }
        assert_eq!(seen.binds.load(Ordering::SeqCst), 1);
        assert_eq!(seen.creates.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_actual_box_provider_owner() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_actual_box_provider_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider-product.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_actual_box_provider_owner() {
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

struct DisposalPanic {
    seen: Arc<Seen>,
    text: bool,
}
impl Stream for DisposalPanic {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.text {
            Poll::Ready(Some(StreamDelta::Completed))
        } else {
            self.text = true;
            Poll::Ready(Some(StreamDelta::TextDelta("before disposal".into())))
        }
    }
}
impl Drop for DisposalPanic {
    fn drop(&mut self) {
        self.seen.dropped.fetch_add(1, Ordering::SeqCst);
        panic!("actual original provider stream disposal failure");
    }
}
async fn failure_contract(store: Arc<dyn Store>) {
    for panic in [false, true] {
        let (runtime, plans, seen, _) = runtime(store.clone(), false);
        seen.disposal_panic.store(panic, Ordering::SeqCst);
        seen.semantic_error.store(!panic, Ordering::SeqCst);
        let handle = runtime.start(request()).await.unwrap();
        let run = handle.run_id().clone();
        let context = SessionContext::new(handle.session_id().clone(), "test", "/tmp");
        let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
            .await
            .unwrap();
        let attempt = store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt;
        runtime
            .drain_session_execution_tasks(context.session_id(), Duration::from_secs(2))
            .await;
        let closed = plans.close_session(&context, Duration::from_secs(2)).await;
        if panic {
            assert!(
                matches!(result, Err(RuntimeError::AttemptCleanupIncomplete { .. })),
                "{result:?}"
            );
            assert_eq!(attempt.state, crabber_session::AttemptState::Started);
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
            assert!(closed.is_err());
            assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
        } else {
            assert!(
                matches!(result, Err(RuntimeError::Provider(_))),
                "{result:?}"
            );
            assert_eq!(attempt.state, crabber_session::AttemptState::Failed);
            assert_eq!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
            closed.unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_box_provider_disposal_failure() {
    failure_contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_box_provider_disposal_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("failure.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    failure_contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_box_provider_disposal_failure() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    failure_contract(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
#[path = "provider_products/preparation.rs"]
mod preparation;
