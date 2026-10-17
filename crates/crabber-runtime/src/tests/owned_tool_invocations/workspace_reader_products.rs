//! Actual typed prompt middleware consumes the selected original reader facade.
use super::*;
use crabber_extension::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationObservation, ContextualGenerationWork, ContextualPlanProvider,
    ContextualWorkspaceReaderProduct, ContextualWorkspaceReaderResolverBody,
    ContextualWorkspaceReaderResolverProduct, ContextualWorkspaceReaderResolverProductFactory,
    FactoryContext, FactoryScope, MiddlewareDescriptor, ModelAttemptContext,
    SystemPromptMiddleware, WorkspaceContext, WorkspaceReadError, WorkspaceReadErrorKind,
    WorkspaceReader, WorkspaceReaderResolver,
};
use futures::future::BoxFuture;
struct Reader {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: Option<Arc<cancellation::Gate>>,
}
impl Drop for Reader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl WorkspaceReader for Reader {
    async fn read_limited(
        &self,
        path: &str,
        maximum: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(path, "AGENTS.md");
        assert_eq!(maximum, 128);
        if let Some(gate) = &self.pending {
            cancellation::Pending(gate.clone())
                .await
                .map_err(|_| WorkspaceReadError::new(WorkspaceReadErrorKind::Io))?;
        }
        Ok(b"actual-reader-product".to_vec())
    }
}
struct Body {
    resolves: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: Option<Arc<cancellation::Gate>>,
}
#[async_trait]
impl ContextualWorkspaceReaderResolverBody for Body {
    async fn resolve_box(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<ContextualWorkspaceReaderProduct, WorkspaceReadError> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        assert_eq!(workspace, &WorkspaceContext::from_persisted("test", "/tmp"));
        Ok(ContextualWorkspaceReaderProduct::new(Box::new(Reader {
            calls: self.calls.clone(),
            drops: self.drops.clone(),
            pending: self.pending.clone(),
        })))
    }
}
struct Factory {
    scope: FactoryScope,
    resolves: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: Option<Arc<cancellation::Gate>>,
}
impl ContextualWorkspaceReaderResolverProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "runtime-box-reader".into(),
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
        self.scope
    }
    fn create_workspace_reader_resolver(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualWorkspaceReaderResolverProduct, ExtensionError>> {
        Box::pin(async move {
            Ok(ContextualWorkspaceReaderResolverProduct::new(Box::new(
                Body {
                    resolves: self.resolves.clone(),
                    calls: self.calls.clone(),
                    drops: self.drops.clone(),
                    pending: self.pending.clone(),
                },
            )))
        })
    }
}
struct Middleware(bool);
#[async_trait]
impl SystemPromptMiddleware for Middleware {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        let resolver = context
            .workspace_reader_resolver()
            .ok_or("no original resolver")?;
        let workspace = if self.0 {
            WorkspaceContext::from_persisted("forged", "/forged")
        } else {
            context.workspace().clone()
        };
        let reader = resolver
            .resolve(&workspace)
            .await
            .map_err(|error| error.to_string())?;
        let bytes = reader
            .read_limited("AGENTS.md", 128)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Some(String::from_utf8(bytes).unwrap()))
    }
}
struct Prompt(bool);
#[async_trait]
impl Extension for Prompt {
    fn id(&self) -> &'static str {
        "runtime-reader-prompt"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("forged={}", self.0)
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.system_prompt_middleware(
            "reader",
            0,
            MiddlewareDescriptor::new("reader-product-test", "1", "a".repeat(64)).unwrap(),
            Arc::new(Middleware(self.0)),
        );
        Ok(())
    }
}
struct RefusedFallback(Arc<AtomicUsize>);
#[async_trait]
impl WorkspaceReaderResolver for RefusedFallback {
    async fn resolve(
        &self,
        _: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(WorkspaceReadError::new(WorkspaceReadErrorKind::Denied))
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
    for mode in 0..4 {
        case(store.clone(), mode).await;
    }
}
async fn case(store: Arc<dyn Store>, mode: usize) {
    let gate = (mode == 1 || mode == 2).then(|| {
        Arc::new(cancellation::Gate {
            panic: mode == 2,
            ..Default::default()
        })
    });
    let release = gate
        .as_ref()
        .map(|gate| cancellation::Release(gate.clone()));
    let resolves = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let fallback = Arc::new(AtomicUsize::new(0));
    let host = Arc::new(Host::default());
    let registry = Registry::new();
    registry
        .mount_owned(Arc::new(Prompt(mode == 3)), Scope::Global)
        .await
        .unwrap();
    let plans = Arc::new(
        ContextualPlanProvider::new_with_workspace_reader_resolver_products(
            registry,
            vec![Arc::new(Factory {
                scope: if mode == 0 {
                    FactoryScope::Workspace
                } else {
                    FactoryScope::Session
                },
                resolves: resolves.clone(),
                calls: calls.clone(),
                drops: drops.clone(),
                pending: gate.clone(),
            })],
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
        .workspace_reader_resolver(Arc::new(RefusedFallback(fallback.clone())))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    if let Some(gate) = &gate {
        assert_pending(&runtime, &plans, &store, &fake, &handle, &run, gate).await;
    }
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    let joined = runtime.drain_execution_tasks(Duration::from_secs(3)).await;
    let closed = plans.close_all(Duration::from_secs(3)).await;
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    let disposed = graph
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    assert_eq!(resolves.load(Ordering::SeqCst), usize::from(mode != 3));
    assert_eq!(calls.load(Ordering::SeqCst), usize::from(mode != 3));
    assert_eq!(drops.load(Ordering::SeqCst), usize::from(mode != 3));
    assert_eq!(fallback.load(Ordering::SeqCst), 0);
    assert_eq!(joined.pending, [] as [RunId; 0]);
    if mode == 0 {
        assert_eq!(result.unwrap().status, RunStatus::Completed);
        assert!(closed.is_ok() && disposed.is_ok());
        assert_eq!(fake.requests().len(), 1);
        assert!(format!("{:?}", fake.requests()).contains("actual-reader-product"));
    } else if mode == 3 {
        assert!(result.is_err());
        assert!(closed.is_ok() && disposed.is_ok());
        assert_eq!(fake.requests(), [] as [crabber_providers::ModelRequest; 0]);
    } else if mode == 1 {
        assert!(result.is_err());
        assert!(closed.is_ok() && disposed.is_ok());
    } else {
        assert!(result.is_err() && closed.is_err() && disposed.is_err());
    }
}
async fn assert_pending(
    runtime: &Orchestrator,
    plans: &ContextualPlanProvider,
    store: &Arc<dyn Store>,
    fake: &FakeProvider,
    handle: &crate::RunHandle,
    run: &RunId,
    gate: &cancellation::Gate,
) {
    cancellation::signal(&gate.entered).await;
    handle.interrupt();
    cancellation::signal(&gate.disposing).await;
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    let attempt = store
        .latest_execution_attempt(run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    let closed = plans.close_all(Duration::from_millis(20)).await;
    assert_ne!(pending.pending, [] as [RunId; 0]);
    assert_eq!(attempt.state, crabber_session::AttemptState::Started);
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    assert!(closed.is_err());
    assert_eq!(fake.requests().len(), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_box_workspace_reader_runtime() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_box_workspace_reader_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("box-reader.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_box_workspace_reader_runtime() {
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
