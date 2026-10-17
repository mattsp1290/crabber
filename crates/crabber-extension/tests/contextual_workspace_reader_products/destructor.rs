use super::*;
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
    registry: Mutex<Option<Registry>>,
    next: Mutex<Option<Next>>,
    ambient: Mutex<Option<Arc<dyn WorkspaceReader>>>,
    done: Notify,
    panics: bool,
    payloads: AtomicUsize,
}
impl Frames {
    fn inspect(&self) {
        assert!(Arc::ptr_eq(&current_state_sink().unwrap(), &self.sink));
        let probe = self.ambient.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(probe.read_limited("ambient", 64)).unwrap(),
            b"ambient"
        );
        let provider = self.provider.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                provider.close_session(&context(), Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
        let registry = self.registry.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(registry.close_all()),
            Err(ExtensionError::SelfClose)
        );
        futures::executor::block_on(current_state_sink().unwrap().apply("reader-drop", vec![]))
            .unwrap();
    }
    pub(super) fn original_drop(&self) {
        self.inspect();
        if !self.panics {
            let next = self.next.lock().unwrap().clone().unwrap();
            assert_eq!(
                futures::executor::block_on(next.call(Value::Bool(true))).unwrap(),
                Value::Bool(true)
            );
            self.done.notify_one();
        }
    }
}
pub(super) struct Payload(pub(super) Arc<Frames>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.inspect();
        let next = self.0.next.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(next.call(Value::Bool(true))).unwrap(),
            Value::Bool(true)
        );
        self.0.payloads.fetch_add(1, Ordering::SeqCst);
        self.0.done.notify_one();
    }
}

struct Around {
    resolver: Arc<Mutex<Option<Arc<dyn WorkspaceReaderResolver>>>>,
    frames: Arc<Frames>,
}
#[async_trait]
impl Extension for Around {
    fn id(&self) -> &'static str {
        "reader-destructor-caller"
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
            "reader-destructor",
            Arc::new(move |value, next| {
                let resolver = resolver.lock().unwrap().clone().unwrap();
                let frames = frames.clone();
                Box::pin(async move {
                    *frames.next.lock().unwrap() = Some(next);
                    let reader = resolver.resolve(&workspace()).await.unwrap();
                    assert_eq!(
                        reader.read_limited("destructor", 64).await.unwrap(),
                        b"destructor"
                    );
                    drop(reader);
                    frames.done.notified().await;
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_reader_box_drop_and_panic_payload_keep_exact_callback_frames() {
    for panics in [false, true] {
        let seen = Seen::new();
        seen.box_panic.store(panics, Ordering::SeqCst);
        *seen.reader_drop.open.lock().unwrap() = false;
        let _release = Release(vec![seen.reader_drop.clone()]);
        let sink = Arc::new(Sink {
            session: context().session_id().clone(),
            calls: AtomicUsize::new(0),
        });
        let frames = Arc::new(Frames {
            sink: sink.clone(),
            provider: Mutex::new(None),
            registry: Mutex::new(None),
            next: Mutex::new(None),
            ambient: Mutex::new(None),
            done: Notify::new(),
            panics,
            payloads: AtomicUsize::new(0),
        });
        *seen.destructor_frames.lock().unwrap() = Some(frames.clone());
        let resolver = Arc::new(Mutex::new(None));
        let registry = Registry::new();
        *frames.registry.lock().unwrap() = Some(registry.clone());
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
            BOUND,
            host.clone(),
        )
        .unwrap();
        *frames.provider.lock().unwrap() = Some(provider.clone());
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        *resolver.lock().unwrap() = plan.workspace_reader_resolver().cloned();
        let resources = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
        // The original creator capability is tested through a second genuine
        // returned-reader facade, never a public host assertion or token getter.
        let probe_seen = Seen::new();
        let (probe_provider, probe_host) = setup(&probe_seen);
        let probe_plan = probe_provider.acquire_plan_in(&context()).await.unwrap();
        let probe_resolver = probe_plan.workspace_reader_resolver().unwrap().clone();
        drop(probe_plan);
        *frames.ambient.lock().unwrap() = Some(
            with_callback_invocations(owner.capability(), probe_resolver.resolve(&workspace()))
                .await
                .unwrap(),
        );
        let dispatcher = plan.dispatcher.clone();
        drop(plan);
        let call = tokio::spawn(with_state_sink(
            sink.clone(),
            with_callback_invocations(owner.capability(), async move {
                dispatcher
                    .around::<ToolExecute>(
                        Value::Bool(true),
                        Arc::new(|value| Box::pin(async move { Ok(value) })),
                    )
                    .await
            }),
        ));
        seen.reader_drop.entered().await;
        let observation = graph(&host).workspace_reader_resolver_product(0).unwrap();
        graph(&host).request_close().unwrap();
        resolver.lock().unwrap().take();
        let pending = observation.disposed(Duration::from_millis(20)).await;
        seen.reader_drop.release();
        let result = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
        frames.provider.lock().unwrap().take();
        frames.registry.lock().unwrap().take();
        frames.ambient.lock().unwrap().take();
        drop(probe_resolver);
        drain(owner, &resources).await;
        probe_provider.close_all(BOUND).await.unwrap();
        graph(&probe_host)
            .workspace_reader_resolver_product(0)
            .unwrap()
            .disposed(BOUND)
            .await
            .unwrap();
        let disposed = observation.disposed(BOUND).await;
        let closed = provider.close_all(BOUND).await;
        assert!(pending.is_err());
        assert_eq!(result.unwrap(), Value::Bool(true));
        assert_eq!(
            sink.calls.load(Ordering::SeqCst),
            if panics { 2 } else { 1 }
        );
        assert_eq!(frames.payloads.load(Ordering::SeqCst), usize::from(panics));
        assert_eq!(disposed.is_err(), panics);
        assert_eq!(closed.is_err(), panics);
    }
}
