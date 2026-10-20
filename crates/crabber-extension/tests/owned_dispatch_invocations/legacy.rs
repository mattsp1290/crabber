//! Legacy constructors keep their original per-route task-local visibility.
use super::*;
use crabber_core::{RunId, ToolCallId, ToolOutput};
use crabber_extension::{
    ContextAssemble, MountHandle, RunBeforeExecute, RunStarted, ToolExecute, ToolInput,
    ToolOutcomeClass, ToolResultContext, TransformOutput,
};
use std::{
    future::Future,
    sync::Mutex,
    task::{Context, Poll},
};

struct Legacy {
    mode: u8,
    handle: Arc<Mutex<Option<MountHandle>>>,
    seen: Arc<Mutex<Option<bool>>>,
}
fn observes_self_close(holder: &Mutex<Option<MountHandle>>) -> bool {
    let handle = holder.lock().unwrap().clone().unwrap();
    let mut close = Box::pin(handle.close());
    matches!(
        close
            .as_mut()
            .poll(&mut Context::from_waker(&futures::task::noop_waker())),
        Poll::Ready(Err(ExtensionError::SelfClose))
    )
}
#[async_trait]
impl Extension for Legacy {
    fn id(&self) -> &'static str {
        "legacy-constructor"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let handle = self.handle.clone();
        let seen = self.seen.clone();
        let callback: crabber_extension::Callback = Arc::new(move |value| {
            *seen.lock().unwrap() = Some(observes_self_close(&handle));
            Box::pin(async move { Ok(value) })
        });
        match self.mode {
            0 => r.on_notify(RunStarted::ID, 0, "notify", callback),
            1 => r.on_hook(TurnPrepare::ID, 0, "hook", callback),
            2 => r.on_transform(ContextAssemble::ID, 0, "transform", callback),
            3 => r.on_gate(RunBeforeExecute::ID, 0, "gate", callback),
            4 => {
                let handle = self.handle.clone();
                let seen = self.seen.clone();
                r.on_around(
                    ToolExecute::ID,
                    0,
                    "around",
                    Arc::new(move |value, next| {
                        *seen.lock().unwrap() = Some(observes_self_close(&handle));
                        Box::pin(async move { next.call(value).await })
                    }),
                );
            }
            5 => {
                let handle = self.handle.clone();
                let seen = self.seen.clone();
                r.on_result_transform(
                    0,
                    "result",
                    Arc::new(move |_, value| {
                        *seen.lock().unwrap() = Some(observes_self_close(&handle));
                        Box::pin(async move { Ok(TransformOutput::new(value)) })
                    }),
                );
            }
            6 => {
                let seen = self.seen.clone();
                r.on_around(
                    ToolExecute::ID,
                    0,
                    "next-constructor",
                    Arc::new(move |value, next| {
                        let mut call = Box::pin(next.call(Value::Null));
                        let outside = matches!(
                            call.as_mut()
                                .poll(&mut Context::from_waker(&futures::task::noop_waker())),
                            Poll::Ready(Err(ExtensionError::NextOutsideCallback))
                        );
                        drop(call);
                        *seen.lock().unwrap() = Some(outside);
                        Box::pin(async move { next.call(value).await })
                    }),
                );
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
async fn probe(modes: impl IntoIterator<Item = u8>) {
    for mode in modes {
        let registry = Registry::new();
        let holder = Arc::new(Mutex::new(None));
        let seen = Arc::new(Mutex::new(None));
        let handle = registry
            .mount(
                Arc::new(Legacy {
                    mode,
                    handle: holder.clone(),
                    seen: seen.clone(),
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        *holder.lock().unwrap() = Some(handle.clone());
        let plan = registry.acquire(&SessionId::new());
        let dispatcher = plan.dispatcher.clone();
        drop(plan);
        match mode {
            0 => dispatcher.notify::<RunStarted>(Value::Null).await,
            1 => dispatcher.hook::<TurnPrepare>(Value::Null).await.unwrap(),
            2 => {
                dispatcher
                    .transform::<ContextAssemble>(Value::Null)
                    .await
                    .unwrap();
            }
            3 => dispatcher
                .gate::<RunBeforeExecute>(Value::Bool(true))
                .await
                .unwrap(),
            4 | 6 => {
                dispatcher
                    .around::<ToolExecute>(
                        Value::Bool(true),
                        Arc::new(|value| Box::pin(async move { Ok(value) })),
                    )
                    .await
                    .unwrap();
            }
            5 => {
                let context = ToolResultContext::new(
                    "fixture".into(),
                    true,
                    ToolInput::Normalized(Value::Null),
                    ToolCallId::new(),
                    SessionId::new(),
                    RunId::new(),
                    ToolOutcomeClass::Succeeded,
                );
                dispatcher
                    .transform_tool_result(
                        context,
                        TransformOutput::new(ToolOutput::from_json(&Value::Null).unwrap()),
                    )
                    .await;
            }
            _ => unreachable!(),
        }
        let observed = seen.lock().unwrap().take();
        holder.lock().unwrap().take();
        handle.close().await.unwrap();
        assert_eq!(observed, Some(mode >= 5), "legacy constructor mode {mode}");
    }
}

#[tokio::test]
async fn actual_legacy_constructor_mount_frames_remain_compatible() {
    probe(0..6).await;
}
#[tokio::test]
async fn actual_legacy_around_constructor_remains_outside_next_frame() {
    probe([6]).await;
}
