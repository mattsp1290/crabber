use super::*;
use crabber_extension::{Point, ToolExecute};
use serde_json::Value;
use std::collections::BTreeMap;
struct Sink {
    session: SessionId,
    calls: AtomicUsize,
}
#[async_trait]
impl StateSink for Sink {
    fn session_id(&self) -> &SessionId {
        &self.session
    }
    async fn snapshot(&self, _: &str) -> Result<BTreeMap<String, String>, String> {
        Ok(BTreeMap::new())
    }
    async fn apply(&self, _: &str, _: Vec<(String, Option<String>)>) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
pub(super) struct Frames {
    sink: Arc<dyn StateSink>,
    provider: Mutex<Option<ContextualPlanProvider>>,
    next: Mutex<Option<Next>>,
    disposed: AtomicUsize,
}
impl Frames {
    fn inspect(&self) {
        let actual = current_state_sink().unwrap();
        assert!(Arc::ptr_eq(&actual, &self.sink));
        let provider = self.provider.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                provider.close_session(&context(), Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
    }
    pub(super) async fn read(&self) {
        self.inspect();
        current_state_sink()
            .unwrap()
            .apply("fs", vec![("read".into(), Some("seen".into()))])
            .await
            .unwrap();
    }
    pub(super) fn disposed(&self) {
        self.inspect();
        let next = self.next.lock().unwrap().take().unwrap();
        assert_eq!(
            futures::executor::block_on(next.call(Value::Bool(true))).unwrap(),
            Value::Bool(true)
        );
        self.disposed.fetch_add(1, Ordering::SeqCst);
    }
}
struct Around {
    resolver: Arc<Mutex<Option<Arc<dyn WorkspaceReaderResolver>>>>,
    frames: Arc<Frames>,
}
#[async_trait]
impl Extension for Around {
    fn id(&self) -> &'static str {
        "fs-caller"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let resolver = self.resolver.clone();
        let frames = self.frames.clone();
        registrar.on_around(
            ToolExecute::ID,
            0,
            "fs",
            Arc::new(move |value, next| {
                let resolver = resolver.lock().unwrap().clone().unwrap();
                let frames = frames.clone();
                Box::pin(async move {
                    *frames.next.lock().unwrap() = Some(next);
                    assert_eq!(
                        resolver
                            .resolve(&workspace())
                            .await
                            .unwrap()
                            .read_limited("preserved/path", 64)
                            .await
                            .unwrap(),
                        b"preserved/path"
                    );
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_read_and_disposal_keep_state_sink_next_and_self_close_frames() {
    let seen = Seen::new();
    let sink = Arc::new(Sink {
        session: context().session_id().clone(),
        calls: AtomicUsize::new(0),
    });
    let frames = Arc::new(Frames {
        sink: sink.clone(),
        provider: Mutex::new(None),
        next: Mutex::new(None),
        disposed: AtomicUsize::new(0),
    });
    *seen.frames.lock().unwrap() = Some(frames.clone());
    let resolver = Arc::new(Mutex::new(None));
    let registry = Registry::new();
    registry
        .mount_owned(
            Arc::new(Around {
                resolver: resolver.clone(),
                frames: frames.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_workspace_reader_resolver_products(
        registry,
        vec![Arc::new(Factory(seen.clone(), FactoryScope::Session))],
        Duration::from_secs(3),
        host.clone(),
    )
    .unwrap();
    *frames.provider.lock().unwrap() = Some(provider.clone());
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    *resolver.lock().unwrap() = plan.workspace_reader_resolver().cloned();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_state_sink(
        sink.clone(),
        with_callback_invocations(
            owner.capability(),
            plan.dispatcher.around::<ToolExecute>(
                Value::Bool(true),
                Arc::new(|value| Box::pin(async move { Ok(value) })),
            ),
        ),
    )
    .await;
    frames.provider.lock().unwrap().take();
    resolver.lock().unwrap().take();
    drop(plan);
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(result.unwrap(), Value::Bool(true));
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
    assert_eq!(frames.disposed.load(Ordering::SeqCst), 1);
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
}
