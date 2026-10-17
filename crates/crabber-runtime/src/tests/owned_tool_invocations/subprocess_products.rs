//! Actual `ToolContext` consumers of unique subprocess nodes, without policy credit.
use super::*;
use crabber_extension::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationObservation, ContextualGenerationWork, ContextualPlanProvider,
    ContextualSubprocessProduct, ContextualSubprocessProductFactory, FactoryContext, FactoryScope,
    Subprocess, ToolContext, ToolExecutor,
};
use futures::future::BoxFuture;
struct Process {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: Option<Arc<cancellation::Gate>>,
}
impl Drop for Process {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Subprocess for Process {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        assert_eq!(program, "original-program");
        assert_eq!(arguments, ["actual/path", "original-second"]);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.pending {
            cancellation::Pending(gate.clone())
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(json!([program, arguments]))
    }
}
struct Factory {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: Option<Arc<cancellation::Gate>>,
}
impl ContextualSubprocessProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "runtime-box-subprocess".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        if self.pending.is_some() {
            "blocked"
        } else {
            "normal"
        }
        .into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_subprocess(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualSubprocessProduct, ExtensionError>> {
        Box::pin(async {
            Ok(ContextualSubprocessProduct::new(Box::new(Process {
                calls: self.calls.clone(),
                drops: self.drops.clone(),
                pending: self.pending.clone(),
            })))
        })
    }
}
struct RunTool;
#[async_trait]
impl ToolExecutor for RunTool {
    async fn execute(&self, _: Value) -> Result<crabber_core::ToolOutput, ExtensionError> {
        panic!("requires actual ToolContext")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<crabber_core::ToolOutput, ExtensionError> {
        let path = arguments["text"].as_str().unwrap();
        let output = context
            .host
            .subprocess
            .as_ref()
            .unwrap()
            .run(
                "original-program",
                &[path.to_owned(), "original-second".into()],
            )
            .await
            .map_err(ExtensionError::Tool)?;
        Ok(crabber_core::ToolOutput::from_json(&output).unwrap())
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
async fn setup(
    store: Arc<dyn Store>,
    pending: Option<Arc<cancellation::Gate>>,
) -> (
    Orchestrator,
    Arc<ContextualPlanProvider>,
    Arc<Host>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    ToolCallId,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let host = Arc::new(Host::default());
    let registry = Registry::new();
    registry
        .mount_owned(
            Arc::new(Tools(vec![tool(Arc::new(RunTool))])),
            Scope::Global,
        )
        .await
        .unwrap();
    let provider = Arc::new(
        ContextualPlanProvider::new_with_subprocess_products(
            registry,
            vec![Arc::new(Factory {
                calls: calls.clone(),
                drops: drops.clone(),
                pending,
            })],
            Duration::from_secs(3),
            host.clone(),
        )
        .unwrap(),
    );
    let call = ToolCallId::new();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(call.clone(), r#"{"text":"actual/path"}"#),
            text_script("done"),
        ])))
        .plan_provider(provider.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    (runtime, provider, host, calls, drops, call)
}
async fn contract(store: Arc<dyn Store>) {
    let (runtime, provider, host, calls, drops, _) = setup(store.clone(), None).await;
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let drained = runtime.drain_execution_tasks(Duration::from_secs(3)).await;
    assert_eq!(drained.pending, [] as [crabber_core::RunId; 0]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "Run completion is not subprocess object disposal"
    );
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    graph
        .subprocess_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .cleanup,
        crabber_session::AttemptCleanup::Complete
    );
    for panic in [false, true] {
        cancellation_contract(store.clone(), panic).await;
    }
}
async fn cancellation_contract(store: Arc<dyn Store>, panic: bool) {
    let gate = Arc::new(cancellation::Gate {
        panic,
        ..Default::default()
    });
    let release = cancellation::Release(gate.clone());
    let (runtime, provider, host, calls, drops, call) =
        setup(store.clone(), Some(gate.clone())).await;
    let handle = runtime.start(request()).await.unwrap();
    cancellation::signal(&gate.entered).await;
    handle.interrupt();
    cancellation::signal(&gate.disposing).await;
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(30))
        .await;
    let attempt = store
        .latest_execution_attempt(handle.run_id())
        .await
        .unwrap()
        .unwrap()
        .attempt;
    let first_close = provider.close_all(Duration::from_millis(30)).await;
    let before = drops.load(Ordering::SeqCst);
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    let joined = runtime.drain_execution_tasks(Duration::from_secs(3)).await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    let disposed = graph
        .subprocess_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    assert!(
        pending.resources.iter().any(|scope| scope.call_id == call
            && scope.phase.is_some()
            && scope.resources.pending > 0),
        "{pending:?}"
    );
    assert_eq!(attempt.state, crabber_session::AttemptState::Started);
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    assert!(first_close.is_err());
    assert_eq!(before, 0);
    assert_eq!(joined.pending, [] as [crabber_core::RunId; 0]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    if panic {
        assert!(result.is_err() && closed.is_err() && disposed.is_err());
    } else {
        assert!(
            matches!(
                result,
                Err(RuntimeError::PhaseOutcomeUnknown {
                    reason: crabber_session::PhaseError::Uncertain,
                    ..
                })
            ),
            "{result:?}"
        );
        assert!(closed.is_ok() && disposed.is_ok());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_box_subprocess_runtime() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_box_subprocess_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("box-subprocess.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_box_subprocess_runtime() {
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
