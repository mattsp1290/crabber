//! Actual callback disposal must precede Runtime phase/worker quiescence.
use super::*;
#[path = "callback_ownership/failure.rs"]
mod failure;
#[path = "callback_ownership/nested.rs"]
mod nested;
use crabber_extension::{Extension, Point, Registrar, Registry, Scope};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Default)]
struct Gate {
    entered: Notify,
    disposing: Notify,
    release: (Mutex<bool>, Condvar),
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
struct Pending(Arc<Gate>);
impl Future for Pending {
    type Output = Result<Value, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let (lock, changed) = &self.0.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }
}
struct Callbacks {
    point: &'static str,
    gate: Option<Arc<Gate>>,
    settled: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for Callbacks {
    fn id(&self) -> &'static str {
        "runtime-callback-owner"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.tool(tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0))))));
        if let Some(gate) = &self.gate {
            let gate = gate.clone();
            r.on_around(
                self.point,
                0,
                "blocked",
                Arc::new(move |_, _| Box::pin(Pending(gate.clone()))),
            );
        }
        let settled = self.settled.clone();
        r.on_notify(
            crabber_extension::ToolSettled::ID,
            0,
            "settled",
            Arc::new(move |value| {
                settled.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(value) })
            }),
        );
        Ok(())
    }
}
async fn runtime(
    store: Arc<dyn Store>,
    point: &'static str,
    gate: Option<Arc<Gate>>,
    scripts: Vec<Vec<StreamDelta>>,
) -> (
    Orchestrator,
    Arc<dyn Store>,
    Arc<Registry>,
    Arc<AtomicUsize>,
) {
    let registry = Arc::new(Registry::new());
    let settled = Arc::new(AtomicUsize::new(0));
    registry
        .mount_owned(
            Arc::new(Callbacks {
                point,
                gate,
                settled: settled.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(scripts)))
        .plan_provider(registry.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    (runtime, store, registry, settled)
}
async fn signal(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap();
}
async fn phase_contract(store: Arc<dyn Store>) {
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let call = ToolCallId::new();
    let (runtime, store, registry, _) = runtime(
        store,
        crabber_extension::ToolExecute::ID,
        Some(gate.clone()),
        vec![call_script(call.clone(), r#"{"text":"owned"}"#)],
    )
    .await;
    let handle = runtime.start(request()).await.unwrap();
    signal(&gate.entered).await;
    handle.interrupt();
    signal(&gate.disposing).await;
    // Let the actual phase drain seal. Its retained callback observer must
    // still report the original blocked future, independently of its waiter.
    let report = runtime
        .drain_execution_tasks(Duration::from_millis(50))
        .await;
    assert!(
        report.resources.iter().any(|scope| scope.call_id == call
            && scope.phase.is_some()
            && scope.resources.sealed
            && scope.resources.pending > 0),
        "phase declared quiescence before original callback disposal: {report:?}"
    );
    assert!(
        !store
            .list_events(handle.session_id(), None, 1000)
            .await
            .unwrap()
            .iter()
            .any(|event| event.kind == EventKind::ToolCallSettled)
    );
    drop(release);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    registry.close_all().await.unwrap();
}
async fn worker_contract(store: Arc<dyn Store>, exceed_join_bound: bool) {
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let (runtime, store, registry, _) = runtime(
        store,
        crabber_extension::ModelStream::ID,
        Some(gate.clone()),
        vec![text_script("unused")],
    )
    .await;
    let handle = runtime.start(request()).await.unwrap();
    signal(&gate.entered).await;
    handle.interrupt();
    signal(&gate.disposing).await;
    let report = runtime
        .drain_execution_tasks(Duration::from_millis(50))
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
        "native attempt settled before original model callback disposal",
    );
    assert!(
        report.pending.contains(handle.run_id()),
        "worker joined before original callback disposal: {report:?}"
    );
    assert!(report.background_tasks.iter().any(|scope| scope.kind
        == crate::WorkerResourceKind::ModelCallbackInvocations
        && scope.resources.pending > 0));
    if exceed_join_bound {
        let mut completion = handle.completion_signal();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !*completion.borrow_and_update() {
                completion.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .latest_execution_attempt(handle.run_id())
                .await
                .unwrap()
                .unwrap()
                .attempt
                .state,
            crabber_session::AttemptState::Started
        );
        assert_eq!(
            store
                .get_run(handle.run_id())
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Running
        );
        let pending = runtime
            .drain_execution_tasks(Duration::from_millis(10))
            .await;
        assert!(pending.pending.contains(handle.run_id()));
    }
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    if exceed_join_bound {
        assert!(matches!(
            result,
            Err(RuntimeError::AttemptCleanupIncomplete { .. })
        ));
    } else {
        assert_eq!(result.unwrap().status, RunStatus::Interrupted);
    }
    registry.close_all().await.unwrap();
}
#[tokio::test]
async fn settlement_notifications_use_live_worker_after_phase_seal() {
    let (runtime, _, registry, settled) = runtime(
        Arc::new(MemoryStore::new()),
        crabber_extension::ToolExecute::ID,
        None,
        vec![
            call_script(ToolCallId::new(), r#"{"text":"owned"}"#),
            text_script("done"),
        ],
    )
    .await;
    assert_eq!(
        runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(settled.load(Ordering::SeqCst), 1);
    registry.close_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_quiescence_retains_original_tool_callback_disposal() {
    phase_contract(Arc::new(MemoryStore::new())).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_join_retains_original_model_callback_disposal() {
    for exceed_join_bound in [false, true] {
        worker_contract(Arc::new(MemoryStore::new()), exceed_join_bound).await;
    }
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_callback_quiescence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("callbacks.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store: Arc<dyn Store> =
        Arc::new(crabber_session::SqliteStore::connect(&path).await.unwrap());
    phase_contract(store.clone()).await;
    failure::contract(store.clone()).await;
    for exceed_join_bound in [false, true] {
        worker_contract(store.clone(), exceed_join_bound).await;
    }
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_callback_quiescence() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    let store: Arc<dyn Store> =
        Arc::new(crabber_session::PostgresStore::connect(&url).await.unwrap());
    phase_contract(store.clone()).await;
    failure::contract(store.clone()).await;
    for exceed_join_bound in [false, true] {
        worker_contract(store.clone(), exceed_join_bound).await;
    }
}
