use super::*;
use std::collections::BTreeMap;
struct Sink(SessionId, AtomicUsize);
#[async_trait]
impl StateSink for Sink {
    fn session_id(&self) -> &SessionId {
        &self.0
    }
    async fn snapshot(&self, _: &str) -> Result<BTreeMap<String, String>, String> {
        Ok(BTreeMap::new())
    }
    async fn apply(&self, _: &str, _: Vec<(String, Option<String>)>) -> Result<(), String> {
        self.1.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Frames {
    sink: Arc<dyn StateSink>,
    registry: Registry,
    provider: Mutex<Option<ContextualPlanProvider>>,
    next: Mutex<Option<Next>>,
    checks: AtomicUsize,
    drops: AtomicUsize,
}
impl Frames {
    fn inspect(&self) {
        assert!(Arc::ptr_eq(&current_state_sink().unwrap(), &self.sink));
        futures::executor::block_on(current_state_sink().unwrap().apply("guard", vec![])).unwrap();
        assert_eq!(
            futures::executor::block_on(self.registry.close_all()),
            Err(ExtensionError::SelfClose)
        );
        let provider = self.provider.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                provider.close_session(&context(), Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
        self.checks.fetch_add(1, Ordering::SeqCst);
    }
    fn next(&self) {
        let next = self.next.lock().unwrap().take().unwrap();
        assert_eq!(
            futures::executor::block_on(next.call(Value::Bool(true))).unwrap(),
            Value::Bool(true)
        );
    }
    fn disposed(&self) {
        let provider = self.provider.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                provider.close_session(&context(), Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Around {
    guard: Arc<Mutex<Option<Arc<dyn ToolGuard>>>>,
    frames: Arc<Frames>,
    panic: bool,
}
#[async_trait]
impl Extension for Around {
    fn id(&self) -> &'static str {
        "guard-around"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let guard = self.guard.clone();
        let frames = self.frames.clone();
        let panic = self.panic;
        r.on_around(
            ToolExecute::ID,
            0,
            "guard-around",
            Arc::new(move |value, next| {
                let guard = guard.lock().unwrap().clone().unwrap();
                let frames = frames.clone();
                Box::pin(async move {
                    *frames.next.lock().unwrap() = Some(next);
                    let decision = contextual(guard.as_ref());
                    assert_eq!(
                        decision,
                        if panic {
                            GuardDecision::Deny
                        } else {
                            GuardDecision::Allow
                        }
                    );
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_inline_and_payload_keep_state_sink_next_active_mount_and_destructor_owner() {
    for panic in [false, true] {
        let seen = Seen::new();
        seen.mode.store(if panic { 2 } else { 0 }, Ordering::SeqCst);
        let registry = Registry::new();
        let sink = Arc::new(Sink(context().session_id().clone(), AtomicUsize::new(0)));
        let frames = Arc::new(Frames {
            sink: sink.clone(),
            registry: registry.clone(),
            provider: Mutex::new(None),
            next: Mutex::new(None),
            checks: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        });
        let guard = Arc::new(Mutex::new(None));
        registry
            .mount_owned(
                Arc::new(Around {
                    guard: guard.clone(),
                    frames: frames.clone(),
                    panic,
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        let host = Arc::new(Host::default());
        let provider = ContextualPlanProvider::new_with_tool_guard_products(
            registry,
            vec![Arc::new(Factory(seen.clone(), FactoryScope::Session))],
            BOUND,
            host.clone(),
        )
        .unwrap();
        *frames.provider.lock().unwrap() = Some(provider.clone());
        let invoke = frames.clone();
        *seen.hook.lock().unwrap() = Some(Arc::new(move || {
            invoke.inspect();
            if !panic {
                invoke.next();
            }
        }));
        let payload = frames.clone();
        *seen.payload_hook.lock().unwrap() = Some(Arc::new(move || {
            payload.inspect();
            payload.next();
        }));
        let disposed = frames.clone();
        *seen.drop_hook.lock().unwrap() = Some(Arc::new(move || disposed.disposed()));
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        *guard.lock().unwrap() = Some(plan.guards[0].clone());
        let resources = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
        let result = with_state_sink(
            sink.clone(),
            with_callback_invocations(
                owner.capability(),
                plan.dispatcher.around::<ToolExecute>(
                    Value::Bool(true),
                    Arc::new(|v| Box::pin(async move { Ok(v) })),
                ),
            ),
        )
        .await;
        guard.lock().unwrap().take();
        drop(plan);
        drop(owner);
        resources.close();
        let joined = resources.drain(BOUND).await;
        let closed = provider.close_all(BOUND).await;
        let disposed = graph(&host)
            .tool_guard_product(0)
            .unwrap()
            .disposed(BOUND)
            .await;
        frames.provider.lock().unwrap().take();
        seen.hook.lock().unwrap().take();
        seen.payload_hook.lock().unwrap().take();
        seen.drop_hook.lock().unwrap().take();
        assert_eq!(result.unwrap(), Value::Bool(true));
        assert_eq!(joined.pending, 0);
        assert_eq!(joined.failures.is_empty(), !panic);
        assert_eq!(closed.is_err(), panic);
        assert_eq!(disposed.is_err(), panic);
        assert_eq!(
            frames.checks.load(Ordering::SeqCst),
            if panic { 2 } else { 1 }
        );
        assert_eq!(frames.drops.load(Ordering::SeqCst), 1);
    }
}
struct Initialize {
    guard: Arc<dyn ToolGuard>,
}
impl ContextualToolGuardProductFactory for Initialize {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "initializing-guard-caller".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_tool_guard(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolGuardProduct, ExtensionError>> {
        Box::pin(async move {
            let resources = ToolResourceOwner::new();
            let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
            let decision = with_callback_invocations(owner.capability(), async {
                contextual(self.guard.as_ref())
            })
            .await;
            drain(owner, &resources).await;
            assert_eq!(decision, GuardDecision::Allow);
            Ok(ContextualToolGuardProduct::new(Box::new(Body(Seen::new()))))
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_guard_called_by_actual_initializer_preserves_that_initializer_self_close() {
    let seen = Seen::new();
    let (first, host) = setup(&seen, FactoryScope::Workspace);
    let plan = first.acquire_plan_in(&context()).await.unwrap();
    let guard = plan.guards[0].clone();
    drop(plan);
    let parent = Arc::new(Mutex::new(None::<ContextualPlanProvider>));
    let observed = Arc::new(AtomicUsize::new(0));
    let trigger = Arc::new(Initialize { guard });
    let second = ContextualPlanProvider::new_with_tool_guard_products(
        Registry::new(),
        vec![trigger],
        BOUND,
        Arc::new(Host::default()),
    )
    .unwrap();
    *parent.lock().unwrap() = Some(second.clone());
    let target = parent.clone();
    let called = observed.clone();
    *seen.hook.lock().unwrap() = Some(Arc::new(move || {
        let second = target.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                second.close_session(&context(), Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
        called.fetch_add(1, Ordering::SeqCst);
    }));
    drop(second.acquire_plan_in(&context()).await.unwrap());
    second.close_all(BOUND).await.unwrap();
    parent.lock().unwrap().take();
    drop(second);
    seen.hook.lock().unwrap().take();
    first.close_all(BOUND).await.unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    seen.registry.lock().unwrap().take();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
}
