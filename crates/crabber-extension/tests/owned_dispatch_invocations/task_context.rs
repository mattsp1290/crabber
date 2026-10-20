use super::*;
use crabber_core::SessionContext;
use crabber_extension::{
    ComponentIdentity, ContextualPlanProvider, Dispatcher, ExtensionFactory, FactoryContext,
    FactoryScope, RunPlanProvider, StateSink, current_state_sink, with_state_sink,
};
use std::{collections::BTreeMap, sync::Mutex};

struct Closing {
    provider: Arc<Mutex<Option<ContextualPlanProvider>>>,
    context: SessionContext,
    scope: FactoryScope,
    all: bool,
    observed: Arc<Mutex<Option<Result<(), ExtensionError>>>>,
}
#[async_trait]
impl Extension for Closing {
    fn id(&self) -> &'static str {
        "contextual-owned-callback"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let holder = self.provider.clone();
        let context = self.context.clone();
        let scope = self.scope;
        let all = self.all;
        let observed = self.observed.clone();
        r.on_hook(
            TurnPrepare::ID,
            0,
            "close-contextual",
            Arc::new(move |_| {
                let provider = holder.lock().unwrap().clone().unwrap();
                let context = context.clone();
                let observed = observed.clone();
                Box::pin(async move {
                    let bound = Duration::from_millis(20);
                    let result = if all {
                        provider.close_all(bound).await
                    } else {
                        match scope {
                            FactoryScope::Session => provider.close_session(&context, bound).await,
                            FactoryScope::Workspace => {
                                provider.close_workspace(&context, bound).await
                            }
                        }
                    };
                    *observed.lock().unwrap() = Some(result);
                    Ok(Value::Null)
                })
            }),
        );
        Ok(())
    }
}
struct Invokes {
    dispatcher: Dispatcher,
    scope: FactoryScope,
}
#[async_trait]
impl ExtensionFactory for Invokes {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "invoke-existing-dispatcher".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.dispatcher.hook::<TurnPrepare>(Value::Null).await?;
        Ok(Arc::new(Counter(Arc::new(AtomicUsize::new(0)))))
    }
}
#[tokio::test]
async fn owned_callback_in_actual_contextual_initializer_preserves_self_close_owner() {
    for scope in [FactoryScope::Session, FactoryScope::Workspace] {
        for all in [false, true] {
            let registry = Registry::new();
            let holder = Arc::new(Mutex::new(None));
            let observed = Arc::new(Mutex::new(None));
            let context = SessionContext::new(SessionId::new(), "workspace", "/workspace");
            registry
                .mount_owned(
                    Arc::new(Closing {
                        provider: holder.clone(),
                        context: context.clone(),
                        scope,
                        all,
                        observed: observed.clone(),
                    }),
                    Scope::Global,
                )
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            let dispatcher = plan.dispatcher.clone();
            drop(plan);
            let provider = ContextualPlanProvider::new(
                registry,
                vec![],
                vec![Arc::new(Invokes { dispatcher, scope })],
                Duration::from_secs(2),
            )
            .unwrap();
            *holder.lock().unwrap() = Some(provider.clone());
            let acquired = provider.acquire_plan_in(&context).await;
            holder.lock().unwrap().take();
            let succeeded = acquired.is_ok();
            if let Ok(plan) = acquired {
                plan.release();
            }
            provider.close_all(Duration::from_secs(2)).await.unwrap();
            assert_eq!(
                observed.lock().unwrap().take().unwrap(),
                Err(ExtensionError::SelfClose)
            );
            assert!(succeeded);
        }
    }
}

struct Sink {
    session: SessionId,
    state: Mutex<BTreeMap<String, String>>,
}
#[async_trait]
impl StateSink for Sink {
    fn session_id(&self) -> &SessionId {
        &self.session
    }
    async fn snapshot(&self, _: &str) -> Result<BTreeMap<String, String>, String> {
        Ok(self.state.lock().unwrap().clone())
    }
    async fn apply(&self, _: &str, changes: Vec<(String, Option<String>)>) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        for (key, value) in changes {
            if let Some(value) = value {
                state.insert(key, value);
            } else {
                state.remove(&key);
            }
        }
        Ok(())
    }
}
struct Stateful(Arc<dyn StateSink>);
struct StatefulFuture {
    future: futures::future::BoxFuture<'static, Result<Value, ExtensionError>>,
    sink: Arc<dyn StateSink>,
}
impl std::future::Future for StatefulFuture {
    type Output = Result<Value, ExtensionError>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
impl Drop for StatefulFuture {
    fn drop(&mut self) {
        let current = current_state_sink().expect("original state capability during disposal");
        assert!(Arc::ptr_eq(&current, &self.sink));
        futures::executor::block_on(
            current.apply("owned-state", vec![("disposed".into(), Some("yes".into()))]),
        )
        .unwrap();
    }
}
#[async_trait]
impl Extension for Stateful {
    fn id(&self) -> &'static str {
        "owned-state"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let expected = self.0.clone();
        r.on_hook(
            TurnPrepare::ID,
            0,
            "state",
            Arc::new(move |_| {
                let sink =
                    current_state_sink().expect("original state capability during construction");
                assert!(Arc::ptr_eq(&sink, &expected));
                let original = sink.clone();
                let future = Box::pin(async move {
                    assert!(Arc::ptr_eq(&current_state_sink().unwrap(), &sink));
                    sink.apply("owned-state", vec![("key".into(), Some("value".into()))])
                        .await
                        .unwrap();
                    assert_eq!(
                        sink.snapshot("owned-state")
                            .await
                            .unwrap()
                            .get("key")
                            .unwrap(),
                        "value"
                    );
                    Ok(Value::Null)
                });
                Box::pin(StatefulFuture {
                    future,
                    sink: original,
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn owned_callback_preserves_exact_original_fenced_state_sink() {
    let sink: Arc<dyn StateSink> = Arc::new(Sink {
        session: SessionId::new(),
        state: Mutex::new(BTreeMap::new()),
    });
    let registry = Registry::new();
    let handle = registry
        .mount_owned(Arc::new(Stateful(sink.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(sink.session_id());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    with_state_sink(sink.clone(), dispatcher.hook::<TurnPrepare>(Value::Null))
        .await
        .unwrap();
    assert_eq!(
        sink.snapshot("owned-state")
            .await
            .unwrap()
            .get("key")
            .unwrap(),
        "value"
    );
    assert_eq!(
        sink.snapshot("owned-state")
            .await
            .unwrap()
            .get("disposed")
            .unwrap(),
        "yes"
    );
    handle.close(Duration::from_secs(2)).await.unwrap();
    assert!(current_state_sink().is_none());
}
