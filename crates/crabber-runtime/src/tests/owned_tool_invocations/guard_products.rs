//! Actual Runtime permission consumer, without a surrogate caller owner.
use super::*;
use crabber_extension::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationObservation, ContextualGenerationWork, ContextualPlanProvider,
    ContextualToolGuardProduct, ContextualToolGuardProductFactory, FactoryContext, FactoryScope,
    GuardContext, GuardDecision, ToolGuard,
};
use futures::future::BoxFuture;
struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn block(&self) {
        tokio::task::block_in_place(|| {
            self.entered.notify_one();
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        });
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.open.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
struct Payload(Arc<Gate>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.block();
    }
}
type Identity = (SessionId, RunId, ToolCallId);
struct Guard {
    mode: usize,
    gate: Option<Arc<Gate>>,
    observed: Arc<Mutex<Vec<Identity>>>,
    drops: Arc<AtomicUsize>,
}
impl ToolGuard for Guard {
    fn id(&self) -> &'static str {
        "runtime-box-guard"
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        panic!("actual contextual method required")
    }
    fn check_with_context(&self, c: GuardContext<'_>) -> GuardDecision {
        assert_eq!(c.tool.name, "echo");
        assert_eq!(c.arguments, &json!({"text":"guard"}));
        self.observed.lock().unwrap().push((
            c.session_id.clone(),
            c.run_id.clone(),
            c.call_id.clone(),
        ));
        match self.mode {
            2 => self.gate.as_ref().unwrap().block(),
            3 => std::panic::panic_any(Payload(self.gate.as_ref().unwrap().clone())),
            _ => {}
        }
        if self.mode == 1 {
            GuardDecision::Deny
        } else {
            GuardDecision::Allow
        }
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Factory {
    mode: usize,
    gate: Option<Arc<Gate>>,
    observed: Arc<Mutex<Vec<Identity>>>,
    drops: Arc<AtomicUsize>,
}
impl ContextualToolGuardProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "runtime-guard-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        format!("mode={}", self.mode)
    }
    fn scope(&self) -> FactoryScope {
        if self.mode == 0 {
            FactoryScope::Workspace
        } else {
            FactoryScope::Session
        }
    }
    fn create_tool_guard(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolGuardProduct, ExtensionError>> {
        Box::pin(async {
            Ok(ContextualToolGuardProduct::new(Box::new(Guard {
                mode: self.mode,
                gate: self.gate.clone(),
                observed: self.observed.clone(),
                drops: self.drops.clone(),
            })))
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
    for mode in 0..4 {
        case(store.clone(), mode).await;
    }
}
async fn case(store: Arc<dyn Store>, mode: usize) {
    let effects = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let gate = (mode >= 2).then(Gate::new);
    let release = gate.as_ref().map(|g| Release(g.clone()));
    let call = ToolCallId::new();
    let registry = Registry::new();
    registry
        .mount_owned(
            Arc::new(Tools(vec![tool(Arc::new(EchoTool(effects.clone())))])),
            Scope::Global,
        )
        .await
        .unwrap();
    let host = Arc::new(Host::default());
    let plans = Arc::new(
        ContextualPlanProvider::new_with_tool_guard_products(
            registry,
            vec![Arc::new(Factory {
                mode,
                gate: gate.clone(),
                observed: observed.clone(),
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
            call_script(call.clone(), r#"{"text":"guard"}"#),
            text_script("done"),
        ])))
        .plan_provider(plans.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    if let Some(gate) = &gate {
        cancellation::signal(&gate.entered).await;
        handle.interrupt();
        assert_pending(&runtime, store.as_ref(), &run, &effects, &drops).await;
    }
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    let joined = runtime.drain_execution_tasks(Duration::from_secs(3)).await;
    let closed = plans.close_all(Duration::from_secs(3)).await;
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    let disposed = graph
        .tool_guard_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    {
        let seen = observed.lock().unwrap();
        assert_ne!(*seen, [] as [Identity; 0]);
        assert!(
            seen.iter()
                .all(|id| id == &(session.clone(), run.clone(), call.clone()))
        );
    }
    assert_eq!(joined.pending, [] as [RunId; 0]);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    match mode {
        0 => {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert!(closed.is_ok() && disposed.is_ok());
        }
        1 => {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert!(closed.is_ok() && disposed.is_ok());
        }
        2 => {
            assert_eq!(result.unwrap().status, RunStatus::Interrupted);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert!(closed.is_ok() && disposed.is_ok());
        }
        _ => {
            assert!(result.is_err() && closed.is_err() && disposed.is_err());
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            let attempt = store
                .latest_execution_attempt(&run)
                .await
                .unwrap()
                .unwrap()
                .attempt;
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        }
    }
}
async fn assert_pending(
    runtime: &Orchestrator,
    store: &dyn Store,
    run: &RunId,
    effects: &AtomicUsize,
    drops: &AtomicUsize,
) {
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    let attempt = store
        .latest_execution_attempt(run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(attempt.state, crabber_session::AttemptState::Started);
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    assert_ne!(pending.pending, [] as [RunId; 0]);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_box_tool_guard_runtime() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_box_tool_guard_runtime() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("box-guard.sqlite");
    crabber_session::SqliteStore::migrate(&p).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&p).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_box_tool_guard_runtime() {
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
