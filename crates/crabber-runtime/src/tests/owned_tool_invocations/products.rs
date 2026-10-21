//! The real Runtime consumes a typed original Box Tool, without policy promotion.
use super::*;
use crabber_core::ToolOutput;
use crabber_extension::ComponentIdentity;
use crabber_extension::{
    ContextualGenerationAdmission, ContextualGenerationHost, ContextualGenerationObservation,
    ContextualGenerationWork, ContextualPlanProvider, ContextualToolProduct,
    ContextualToolProductFactory, FactoryContext, FactoryScope,
};
use futures::future::BoxFuture;
struct Original {
    effects: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Original {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl crabber_extension::ToolExecutor for Original {
    async fn execute(&self, arguments: Value) -> Result<ToolOutput, ExtensionError> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::from_json(&arguments).unwrap())
    }
}
struct Factory {
    effects: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl ContextualToolProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "runtime-box-tool".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "echo-v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_tool(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolProduct, ExtensionError>> {
        Box::pin(async {
            Ok(ContextualToolProduct::new(
                info(),
                Box::new(Original {
                    effects: self.effects.clone(),
                    drops: self.drops.clone(),
                }),
            ))
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
async fn contract(store: Arc<dyn Store>) {
    let effects = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let host = Arc::new(Host::default());
    let provider = Arc::new(
        ContextualPlanProvider::new_with_tool_products(
            Registry::new(),
            vec![Arc::new(Factory {
                effects: effects.clone(),
                drops: drops.clone(),
            })],
            Duration::from_secs(3),
            host.clone(),
        )
        .unwrap(),
    );
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"one actual Box"}"#),
            text_script("done"),
        ])))
        .plan_provider(provider.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let drained = runtime
        .drain_session_execution_tasks(&session, Duration::from_secs(3))
        .await;
    assert_eq!(drained.pending, [] as [crabber_core::RunId; 0]);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "ordinary Run completion is not service/object disposal"
    );
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    let disposed = graph
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    assert!(disposed.is_ok());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    cancellation_contract(store.clone()).await;
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
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_box_tool_product_runtime() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_box_tool_product_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("box-tools.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_box_tool_product_runtime() {
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

struct Blocked {
    gate: Arc<cancellation::Gate>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Blocked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl crabber_extension::ToolExecutor for Blocked {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        cancellation::Pending(self.gate.clone()).await
    }
}
struct BlockedFactory {
    gate: Arc<cancellation::Gate>,
    drops: Arc<AtomicUsize>,
}
impl ContextualToolProductFactory for BlockedFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "blocked-runtime-box-tool".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "blocked-box-native-v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_tool(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolProduct, ExtensionError>> {
        Box::pin(async {
            Ok(ContextualToolProduct::new(
                info(),
                Box::new(Blocked {
                    gate: self.gate.clone(),
                    drops: self.drops.clone(),
                }),
            ))
        })
    }
}
async fn cancellation_contract(store: Arc<dyn Store>) {
    for panic in [false, true] {
        let gate = Arc::new(cancellation::Gate {
            panic,
            ..Default::default()
        });
        let release = cancellation::Release(gate.clone());
        let drops = Arc::new(AtomicUsize::new(0));
        let host = Arc::new(Host::default());
        let provider = Arc::new(
            ContextualPlanProvider::new_with_tool_products(
                Registry::new(),
                vec![Arc::new(BlockedFactory {
                    gate: gate.clone(),
                    drops: drops.clone(),
                })],
                Duration::from_secs(3),
                host.clone(),
            )
            .unwrap(),
        );
        let call = ToolCallId::new();
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store.clone())
            .resolver(Arc::new(FakeProvider::scripted(vec![call_script(
                call.clone(),
                r#"{"text":"blocked Box"}"#,
            )])))
            .plan_provider(provider.clone())
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .build()
            .unwrap();
        let handle = start(&runtime, &provider, panic).await;
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
            .tool_product(0)
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
        assert_eq!(joined.pending, [] as [crabber_core::RunId; 0]);
        assert!(first_close.is_err());
        assert_eq!(before, 0);
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
}

fn info() -> crabber_core::ToolInfo {
    crabber_core::ToolInfo {
        name: "echo".into(),
        description: "Echo".into(),
        parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
        retry_safe: true,
        required_permissions: vec![],
    }
}

async fn start(
    runtime: &Orchestrator,
    provider: &ContextualPlanProvider,
    panic: bool,
) -> crate::RunHandle {
    match runtime.start(request()).await {
        Ok(handle) => handle,
        Err(error) => {
            let cleanup = match error.preparation_reconciliation() {
                Some(descriptor) => Some(runtime.cleanup_retained_preparation(descriptor).await),
                None => None,
            };
            let closed = provider.close_all(Duration::from_secs(3)).await;
            let drained = runtime.drain_execution_tasks(Duration::from_secs(3)).await;
            panic!(
                "Box cancellation fixture panic={panic} start={error:?}; cleanup={cleanup:?}; close={closed:?}; drain={drained:?}"
            );
        }
    }
}
