//! A returned outer callback cannot hide an already recorded nested main.
use super::*;
use futures::future::BoxFuture;

struct NestedPoint;
impl Point for NestedPoint {
    const ID: &'static str = "test/nested-kernel";
    const MODE: crabber_extension::Mode = crabber_extension::Mode::Hook;
}
type Waiter = BoxFuture<'static, Result<(), ExtensionError>>;
struct Nested {
    dispatcher: Arc<Mutex<Option<crabber_extension::Dispatcher>>>,
    escaped: Arc<Mutex<Option<Waiter>>>,
    gate: Arc<Gate>,
}
#[async_trait]
impl Extension for Nested {
    fn id(&self) -> &'static str {
        "nested-kernel"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let dispatcher = self.dispatcher.clone();
        let escaped = self.escaped.clone();
        let gate = self.gate.clone();
        r.on_hook(
            crabber_extension::TurnPrepare::ID,
            0,
            "outer",
            Arc::new(move |_| {
                let dispatcher = dispatcher.lock().unwrap().clone().unwrap();
                let escaped = escaped.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    let mut nested: Waiter =
                        Box::pin(async move { dispatcher.hook::<NestedPoint>(Value::Null).await });
                    assert!(futures::poll!(&mut nested).is_pending());
                    *escaped.lock().unwrap() = Some(nested);
                    gate.entered.notified().await;
                    Ok(Value::Null)
                })
            }),
        );
        let gate = self.gate.clone();
        r.on_hook(
            NestedPoint::ID,
            0,
            "inner",
            Arc::new(move |_| Box::pin(Pending(gate.clone()))),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returned_outer_hook_cannot_hide_recorded_pending_nested_callback() {
    let registry = Arc::new(Registry::new());
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let dispatcher = Arc::new(Mutex::new(None));
    let escaped = Arc::new(Mutex::new(None));
    registry
        .mount_owned(
            Arc::new(Nested {
                dispatcher: dispatcher.clone(),
                escaped: escaped.clone(),
                gate: gate.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    *dispatcher.lock().unwrap() = Some(registry.acquire(&SessionId::new()).dispatcher.clone());
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script("done")])))
        .plan_provider(registry.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    signal(&gate.disposing).await;
    let attempt = store
        .latest_execution_attempt(handle.run_id())
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(
        attempt.state,
        crabber_session::AttemptState::Started,
        "returned outer hook hid actual unjoined nested callback: {attempt:?}"
    );
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(50))
        .await;
    assert!(pending.pending.contains(handle.run_id()));
    drop(escaped.lock().unwrap().take());
    drop(release);
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::AttemptCleanupIncomplete { .. })
    ));
    registry.close_all().await.unwrap();
}

/// Shared genuine nested callback fixture for the first-two pending task bridge.
pub(in crate::tests) struct NestedFixture {
    pub registry: Arc<Registry>,
    gate: Arc<Gate>,
    release: Option<Release>,
    escaped: Arc<Mutex<Option<Waiter>>>,
}
impl NestedFixture {
    pub async fn new() -> Self {
        let registry = Arc::new(Registry::new());
        let gate = Arc::new(Gate::default());
        let dispatcher = Arc::new(Mutex::new(None));
        let escaped = Arc::new(Mutex::new(None));
        registry
            .mount_owned(
                Arc::new(Nested {
                    dispatcher: dispatcher.clone(),
                    escaped: escaped.clone(),
                    gate: gate.clone(),
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        *dispatcher.lock().unwrap() = Some(registry.acquire(&SessionId::new()).dispatcher.clone());
        Self {
            registry,
            release: Some(Release(gate.clone())),
            gate,
            escaped,
        }
    }
    pub async fn wait_disposing(&self) {
        signal(&self.gate.disposing).await;
    }
    pub fn release(&mut self) {
        drop(self.escaped.lock().unwrap().take());
        drop(self.release.take());
    }
}
