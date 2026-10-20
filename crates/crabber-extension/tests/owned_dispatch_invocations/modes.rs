use super::*;
use crabber_extension::{Next, OwnedMountHandle, ToolExecute};
use std::sync::Mutex;

struct Around {
    id: &'static str,
    ancestor: Arc<Mutex<Option<OwnedMountHandle>>>,
    own: Arc<Mutex<Option<OwnedMountHandle>>>,
    escaped: Arc<Mutex<Option<Next>>>,
}
#[async_trait]
impl Extension for Around {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let ancestor = self.ancestor.clone();
        let own = self.own.clone();
        let escaped = self.escaped.clone();
        r.on_around(
            ToolExecute::ID,
            0,
            self.id,
            Arc::new(move |value, next| {
                let original = own.lock().unwrap().clone().unwrap();
                assert!(matches!(
                    futures::executor::block_on(original.close(Duration::ZERO)),
                    Err(ExtensionError::SelfClose)
                ));
                let ancestor = ancestor.clone();
                let own = own.clone();
                *escaped.lock().unwrap() = Some(next.clone());
                Box::pin(async move {
                    // Context must cover construction and the complete nested stack.
                    let ancestor = ancestor.lock().unwrap().clone();
                    let own = own.lock().unwrap().clone().unwrap();
                    if let Some(handle) = ancestor {
                        assert!(matches!(
                            handle.close(Duration::ZERO).await,
                            Err(ExtensionError::SelfClose)
                        ));
                    }
                    assert!(matches!(
                        own.close(Duration::ZERO).await,
                        Err(ExtensionError::SelfClose)
                    ));
                    let outside = next.clone();
                    assert_eq!(
                        tokio::spawn(async move { outside.call(Value::Null).await })
                            .await
                            .unwrap(),
                        Err(ExtensionError::NextOutsideCallback)
                    );
                    next.call(value).await
                })
            }),
        );
        Ok(())
    }
}

use crabber_core::{RunId, ToolCallId, ToolOutput};
use crabber_extension::{
    ContextAssemble, ModelStream, RunBeforeExecute, RunStarted, ToolInput, ToolOutcomeClass,
    ToolPrepare, ToolResultContext, ToolResultOutcome, TransformOutput,
};

struct AllModes(Arc<AtomicUsize>);
#[async_trait]
impl Extension for AllModes {
    fn id(&self) -> &'static str {
        "all-owned-modes"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let ordinary: crabber_extension::Callback = {
            let calls = self.0.clone();
            Arc::new(move |value| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(value) })
            })
        };
        r.on_notify(RunStarted::ID, 0, "notify", ordinary.clone());
        r.on_hook(TurnPrepare::ID, 0, "hook", ordinary.clone());
        r.on_gate(RunBeforeExecute::ID, 0, "gate", ordinary.clone());
        r.on_transform(ContextAssemble::ID, 0, "transform", ordinary.clone());
        r.on_transform(ToolPrepare::ID, 0, "pinned", ordinary.clone());
        r.on_transform("crabber/tool/result-transform", 0, "json-result", ordinary);
        for point in [ToolExecute::ID, ModelStream::ID] {
            let calls = self.0.clone();
            r.on_around(
                point,
                0,
                point,
                Arc::new(move |value, next| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move { next.call(value).await })
                }),
            );
        }
        let typed = {
            let calls = self.0.clone();
            Arc::new(move |_: ToolResultContext, value| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(TransformOutput::new(value)) })
                    as futures::future::BoxFuture<'static, _>
            })
        };
        r.on_result_transform(1, "typed", typed.clone());
        r.on_final_redaction(2, "final", typed);
        Ok(())
    }
}
fn result_context() -> ToolResultContext {
    ToolResultContext::new(
        "test".into(),
        true,
        ToolInput::Normalized(Value::Null),
        ToolCallId::new(),
        SessionId::new(),
        RunId::new(),
        ToolOutcomeClass::Succeeded,
    )
}
async fn result(dispatcher: &crabber_extension::Dispatcher) -> ToolResultOutcome {
    dispatcher
        .transform_tool_result(
            result_context(),
            TransformOutput::new(ToolOutput::from_json(&Value::Bool(true)).unwrap()),
        )
        .await
}
#[tokio::test]
async fn every_owned_dispatch_route_uses_actual_mount_admission_and_preserves_results() {
    let registry = Registry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .mount_owned(Arc::new(AllModes(calls.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    dispatcher.notify::<RunStarted>(Value::Null).await;
    dispatcher.hook::<TurnPrepare>(Value::Null).await.unwrap();
    dispatcher
        .gate::<RunBeforeExecute>(Value::Bool(true))
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .transform::<ContextAssemble>(Value::Bool(true))
            .await
            .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        dispatcher
            .transform_pinned::<ToolPrepare>(serde_json::json!({}), &[("id", Value::Bool(true))])
            .await
            .unwrap(),
        serde_json::json!({"id":true})
    );
    let terminal: crabber_extension::Callback =
        Arc::new(|value| Box::pin(async move { Ok(value) }));
    assert_eq!(
        dispatcher
            .around::<ToolExecute>(Value::Bool(true), terminal.clone())
            .await
            .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        dispatcher
            .around::<ModelStream>(Value::Bool(true), terminal.clone())
            .await
            .unwrap(),
        Value::Bool(true)
    );
    assert!(matches!(
        result(&dispatcher).await,
        ToolResultOutcome::Completed { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 10);
    handle.close(Duration::from_secs(2)).await.unwrap();
    dispatcher.notify::<RunStarted>(Value::Null).await;
    assert!(dispatcher.hook::<TurnPrepare>(Value::Null).await.is_err());
    assert!(
        dispatcher
            .gate::<RunBeforeExecute>(Value::Bool(true))
            .await
            .is_err()
    );
    assert!(
        dispatcher
            .transform::<ContextAssemble>(Value::Null)
            .await
            .is_err()
    );
    assert!(
        dispatcher
            .transform_pinned::<ToolPrepare>(serde_json::json!({}), &[])
            .await
            .is_err()
    );
    assert!(
        dispatcher
            .around::<ToolExecute>(Value::Null, terminal.clone())
            .await
            .is_err()
    );
    assert!(
        dispatcher
            .around::<ModelStream>(Value::Null, terminal)
            .await
            .is_err()
    );
    assert!(matches!(
        result(&dispatcher).await,
        ToolResultOutcome::Failed { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 10);
}
#[tokio::test]
async fn nested_owned_around_retains_ancestor_self_close_and_exact_next_context() {
    let registry = Registry::new();
    let outer = Arc::new(Mutex::new(None));
    let inner = Arc::new(Mutex::new(None));
    let escaped_outer = Arc::new(Mutex::new(None));
    let escaped_inner = Arc::new(Mutex::new(None));
    let a = registry
        .mount_owned(
            Arc::new(Around {
                id: "outer",
                ancestor: Arc::new(Mutex::new(None)),
                own: outer.clone(),
                escaped: escaped_outer.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    *outer.lock().unwrap() = Some(a.clone());
    let b = registry
        .mount_owned(
            Arc::new(Around {
                id: "inner",
                ancestor: outer.clone(),
                own: inner.clone(),
                escaped: escaped_inner.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    *inner.lock().unwrap() = Some(b.clone());
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    assert_eq!(
        dispatcher
            .around::<ToolExecute>(
                Value::Bool(true),
                Arc::new(|value| Box::pin(async move { Ok(value) }))
            )
            .await
            .unwrap(),
        Value::Bool(true)
    );
    let escaped = escaped_inner.lock().unwrap().clone().unwrap();
    assert_eq!(
        escaped.call(Value::Null).await,
        Err(ExtensionError::NextExpired)
    );
    a.close(Duration::from_secs(2)).await.unwrap();
    b.close(Duration::from_secs(2)).await.unwrap();
}
