use super::*;
use crabber_extension::{MiddlewareDescriptor, ModelAttemptContext, SystemPromptMiddleware};
use futures::future::BoxFuture;

#[derive(Clone)]
enum Mode {
    Semantic,
    DisposalPanic,
    Blocked(Arc<Gate>),
}
struct PendingPrompt(Arc<Gate>);
impl Future for PendingPrompt {
    type Output = Result<Option<String>, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl Drop for PendingPrompt {
    fn drop(&mut self) {
        drop(Pending(self.0.clone()));
    }
}
struct ReadyPrompt;
impl Future for ReadyPrompt {
    type Output = Result<Option<String>, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(None))
    }
}
impl Drop for ReadyPrompt {
    fn drop(&mut self) {
        panic!("original prompt disposal panic");
    }
}
fn original(mode: Mode) -> BoxFuture<'static, Result<Option<String>, ExtensionError>> {
    match mode {
        Mode::Semantic => Box::pin(async { Err(ExtensionError::Plan("prompt rejected".into())) }),
        Mode::DisposalPanic => Box::pin(ReadyPrompt),
        Mode::Blocked(gate) => Box::pin(PendingPrompt(gate)),
    }
}
struct Prompts {
    typed: bool,
    mode: Mode,
}
#[async_trait]
impl SystemPromptMiddleware for Prompts {
    async fn contribute(&self, _: ModelAttemptContext) -> Result<Option<String>, String> {
        original(self.mode.clone())
            .await
            .map_err(|_| "failed".into())
    }
}
#[async_trait]
impl Extension for Prompts {
    fn id(&self) -> &'static str {
        "runtime-owned-prompts"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("typed={}", self.typed)
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        if self.typed {
            registrar.system_prompt_middleware(
                "prompt",
                0,
                MiddlewareDescriptor::new("prompt", "1", "0".repeat(64)).unwrap(),
                Arc::new(Self {
                    typed: true,
                    mode: self.mode.clone(),
                }),
            );
        } else {
            let mode = self.mode.clone();
            registrar.prompt_contributor(0, "prompt", Arc::new(move |_| original(mode.clone())));
        }
        Ok(())
    }
}
async fn runtime(store: Arc<dyn Store>, typed: bool, mode: Mode) -> (Orchestrator, Arc<Registry>) {
    let registry = Arc::new(Registry::new());
    registry
        .mount_owned(Arc::new(Prompts { typed, mode }), Scope::Global)
        .await
        .unwrap();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script(
            "unused",
        )])))
        .plan_provider(registry.clone())
        .build()
        .unwrap();
    (runtime, registry)
}
async fn failure(store: Arc<dyn Store>, typed: bool) {
    for bad_drop in [false, true] {
        let mode = if bad_drop {
            Mode::DisposalPanic
        } else {
            Mode::Semantic
        };
        let (runtime, registry) = runtime(store.clone(), typed, mode).await;
        let handle = runtime.start(request()).await.unwrap();
        let run = handle.run_id().clone();
        let result = handle.done().await;
        let attempt = store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt;
        if bad_drop {
            assert_eq!(
                attempt.state,
                crabber_session::AttemptState::Started,
                "{result:?}"
            );
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
            assert!(matches!(
                result,
                Err(RuntimeError::AttemptCleanupIncomplete { .. })
            ));
            assert!(registry.close_all().await.is_err());
        } else {
            assert_eq!(attempt.state, crabber_session::AttemptState::Failed);
            assert_eq!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
            assert!(result.is_err());
            registry.close_all().await.unwrap();
        }
    }
}
async fn blocked(store: Arc<dyn Store>, typed: bool) {
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let (runtime, registry) = runtime(store.clone(), typed, Mode::Blocked(gate.clone())).await;
    let handle = runtime.start(request()).await.unwrap();
    signal(&gate.entered).await;
    handle.interrupt();
    signal(&gate.disposing).await;
    let report = runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    assert_eq!(
        store
            .latest_execution_attempt(handle.run_id())
            .await
            .unwrap()
            .unwrap()
            .attempt
            .state,
        crabber_session::AttemptState::Started,
    );
    assert!(report.pending.contains(handle.run_id()));
    assert!(report.background_tasks.iter().any(|scope| scope.kind
        == crate::WorkerResourceKind::ExtensionCallbackInvocations
        && scope.resources.pending > 0));
    drop(release);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    registry.close_all().await.unwrap();
}
async fn contract(store: Arc<dyn Store>) {
    for typed in [false, true] {
        failure(store.clone(), typed).await;
        blocked(store.clone(), typed).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_prompt_quiescence() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_prompt_quiescence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prompts.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_prompt_quiescence() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        contract(Arc::new(
            crabber_session::PostgresStore::connect(&url).await.unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}
