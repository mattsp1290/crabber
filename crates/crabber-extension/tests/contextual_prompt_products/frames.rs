use super::*;
use async_trait::async_trait;
use serde_json::Value;
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
    context: SessionContext,
    provider: Mutex<Option<ContextualPlanProvider>>,
    next: Mutex<Option<Next>>,
    checks: AtomicUsize,
    payloads: AtomicUsize,
    drops: AtomicUsize,
}
impl Frames {
    fn inspect(&self) {
        assert!(Arc::ptr_eq(&current_state_sink().unwrap(), &self.sink));
        futures::executor::block_on(current_state_sink().unwrap().apply("prompt", vec![])).unwrap();
        assert_eq!(
            futures::executor::block_on(self.registry.close_all()),
            Err(ExtensionError::SelfClose)
        );
        let provider = self.provider.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                provider.close_session(&self.context, Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
        if self.checks.fetch_add(1, Ordering::SeqCst) == 0 {
            let next = self.next.lock().unwrap().take().unwrap();
            assert_eq!(
                futures::executor::block_on(next.call(Value::Bool(true))).unwrap(),
                Value::Bool(true)
            );
        }
    }
    fn box_disposed(&self) {
        let provider = self.provider.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(
                provider.close_session(&self.context, Duration::from_millis(100))
            ),
            Err(ExtensionError::SelfClose)
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Around {
    callbacks: Arc<Mutex<Vec<MountedPromptContributor>>>,
    frames: Arc<Frames>,
    failed: bool,
}
#[async_trait]
impl Extension for Around {
    fn id(&self) -> &'static str {
        "prompt-around"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let callbacks = self.callbacks.clone();
        let frames = self.frames.clone();
        let failed = self.failed;
        r.on_around(
            ToolExecute::ID,
            0,
            "prompt-around",
            Arc::new(move |value, next| {
                let callbacks = callbacks.lock().unwrap().clone();
                let frames = frames.clone();
                Box::pin(async move {
                    *frames.next.lock().unwrap() = Some(next);
                    let outcome = collect_prompt_contributions(&callbacks, context()).await;
                    if failed {
                        assert!(matches!(outcome, PromptContributionOutcome::Failed { .. }));
                    } else {
                        completed(outcome);
                    }
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn original_callback_and_panic_payload_keep_invocation_frames_box_keeps_original_owner() {
    for (typed, failed) in [(false, false), (true, false), (false, true), (true, true)] {
        let seen = Seen::new();
        seen.mode
            .store(if failed { 7 } else { 0 }, Ordering::SeqCst);
        let registry = Registry::new();
        let context = session();
        let sink = Arc::new(Sink(context.session_id().clone(), AtomicUsize::new(0)));
        let frames = Arc::new(Frames {
            sink: sink.clone(),
            registry: registry.clone(),
            context: context.clone(),
            provider: Mutex::new(None),
            next: Mutex::new(None),
            checks: AtomicUsize::new(0),
            payloads: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        });
        let callbacks = Arc::new(Mutex::new(Vec::new()));
        registry
            .mount_owned(
                Arc::new(Around {
                    callbacks: callbacks.clone(),
                    frames: frames.clone(),
                    failed,
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        let host = Arc::new(Host::default());
        let provider = ContextualPlanProvider::new_with_owned_products(
            registry,
            vec![ContextualOwnedProductFactory::Prompt(Arc::new(Factory {
                seen: seen.clone(),
                typed,
                scope: FactoryScope::Session,
                name: "original".into(),
            }))],
            BOUND,
            host.clone(),
        )
        .unwrap();
        *frames.provider.lock().unwrap() = Some(provider.clone());
        let inspect = frames.clone();
        *seen.hook.lock().unwrap() = Some(Arc::new(move || inspect.inspect()));
        let payload = frames.clone();
        *seen.payload_hook.lock().unwrap() = Some(Arc::new(move || {
            payload.inspect();
            payload.payloads.fetch_add(1, Ordering::SeqCst);
        }));
        let disposed = frames.clone();
        *seen.drop_hook.lock().unwrap() = Some(Arc::new(move || disposed.box_disposed()));
        let plan = provider.acquire_plan_in(&context).await.unwrap();
        *callbacks.lock().unwrap() = plan.prompt_contributors.clone();
        let parent = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
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
        callbacks.lock().unwrap().clear();
        drop(plan);
        drop(owner);
        parent.close();
        let joined = parent.drain(BOUND).await;
        let closed = provider.close_all(BOUND).await;
        let node = host.0.lock().unwrap()[0].prompt_product(0).unwrap();
        let disposed = node.disposed(BOUND).await;
        seen.hook.lock().unwrap().take();
        seen.payload_hook.lock().unwrap().take();
        seen.drop_hook.lock().unwrap().take();
        frames.provider.lock().unwrap().take();
        assert_eq!(result.unwrap(), Value::Bool(true));
        assert_eq!(joined.pending, 0);
        assert_eq!(!joined.failures.is_empty(), failed);
        assert_eq!(closed.is_err(), failed);
        assert_eq!(disposed.is_err(), failed);
        assert_eq!(frames.drops.load(Ordering::SeqCst), 1);
        assert_eq!(frames.payloads.load(Ordering::SeqCst), usize::from(failed));
        assert!(frames.checks.load(Ordering::SeqCst) >= 3);
        assert!(sink.1.load(Ordering::SeqCst) >= 3);
    }
}
struct Initialize {
    callbacks: Vec<MountedPromptContributor>,
}
impl ContextualPromptProductFactory for Initialize {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "initializing-prompt".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_prompt(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualPromptProduct, ExtensionError>> {
        Box::pin(async {
            completed(call(&self.callbacks).await.0);
            Ok(ContextualPromptProduct::native(
                "new".into(),
                0,
                Box::new(|_| Box::pin(async { Ok(None) })),
            ))
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn genuine_initializer_invoking_existing_prompt_retains_exact_initializer_self_close() {
    let seen = Seen::new();
    let (first, _, _) = provider(&seen, false, FactoryScope::Workspace, "original");
    let plan = first.acquire_plan_in(&session()).await.unwrap();
    let callbacks = plan.prompt_contributors.clone();
    drop(plan);
    let parent = Arc::new(Mutex::new(None::<ContextualPlanProvider>));
    let second = ContextualPlanProvider::new_with_owned_products(
        Registry::new(),
        vec![ContextualOwnedProductFactory::Prompt(Arc::new(
            Initialize { callbacks },
        ))],
        BOUND,
        Arc::new(Host::default()),
    )
    .unwrap();
    *parent.lock().unwrap() = Some(second.clone());
    let context = session();
    let target = parent.clone();
    let checked = context.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    *seen.hook.lock().unwrap() = Some(Arc::new(move || {
        let second = target.lock().unwrap().clone().unwrap();
        assert_eq!(
            futures::executor::block_on(second.close_session(&checked, Duration::from_millis(100))),
            Err(ExtensionError::SelfClose)
        );
        calls.fetch_add(1, Ordering::SeqCst);
    }));
    drop(second.acquire_plan_in(&context).await.unwrap());
    second.close_all(BOUND).await.unwrap();
    parent.lock().unwrap().take();
    drop(second);
    seen.hook.lock().unwrap().take();
    first.close_all(BOUND).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
