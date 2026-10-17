use async_trait::async_trait;
use crabber_extension::{
    CallbackInvocationOwner, Extension, ExtensionError, MiddlewareDescriptor, ModelAttemptContext,
    PromptAttemptContext, Registrar, Registry, Scope, SystemPromptMiddleware, ToolResourceOwner,
    WorkspaceContext, collect_prompt_contributions, with_callback_invocations,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Gate {
    entered: Notify,
    disposing: Notify,
    release: (Mutex<bool>, Condvar),
    panic: bool,
    child: Mutex<Option<CancellationToken>>,
}
struct Pending(Arc<Gate>);
impl Future for Pending {
    type Output = Result<Option<String>, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let mut released = self.0.release.0.lock().unwrap();
        while !*released {
            released = self.0.release.1.wait(released).unwrap();
        }
        assert!(!self.0.panic, "original prompt future disposal panic");
    }
}
struct Prompts {
    gate: Arc<Gate>,
    typed: bool,
}
struct Middleware(Arc<Gate>);
#[async_trait]
impl SystemPromptMiddleware for Middleware {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        *self.0.child.lock().unwrap() = Some(context.cancellation().clone());
        Pending(self.0.clone()).await.map_err(|_| "failure".into())
    }
}
#[async_trait]
impl Extension for Prompts {
    fn id(&self) -> &'static str {
        "owned-prompts"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        if self.typed {
            registrar.system_prompt_middleware(
                "blocked",
                0,
                MiddlewareDescriptor::new("blocked", "1", "0".repeat(64)).unwrap(),
                Arc::new(Middleware(self.gate.clone())),
            );
        } else {
            let gate = self.gate.clone();
            registrar.prompt_contributor(
                0,
                "blocked",
                Arc::new(move |context| {
                    *gate.child.lock().unwrap() = Some(context.cancellation().clone());
                    Box::pin(Pending(gate.clone()))
                }),
            );
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collector_deadline_returns_before_original_disposal_and_keeps_child_cancelled() {
    let gate = Arc::new(Gate::default());
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Prompts {
                gate: gate.clone(),
                typed: true,
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    let contributors = plan.prompt_contributors.clone();
    drop(plan);
    let parent = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
    let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        collect_prompt_contributions(&contributors, context()).await
    }));
    gate.entered.notified().await;
    let outcome = tokio::time::timeout(Duration::from_secs(7), waiter).await;
    // Release even on a collector timeout, before asserting on captured facts.
    let pending = parent.drain(Duration::from_millis(20)).await;
    let cancelled = gate.child.lock().unwrap().as_ref().unwrap().is_cancelled();
    *gate.release.0.lock().unwrap() = true;
    gate.release.1.notify_all();
    drop(owner);
    let joined = parent.drain(Duration::from_secs(2)).await;
    mount.close(Duration::from_secs(2)).await.unwrap();
    assert_eq!(
        outcome.unwrap().unwrap(),
        crabber_extension::PromptContributionOutcome::Failed {
            contributor: "blocked".into()
        }
    );
    assert!(cancelled);
    assert_eq!(pending.pending, 1, "{pending:?}");
    assert_eq!(joined.pending, 0);
    assert_eq!(
        joined.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
}
fn context() -> PromptAttemptContext {
    PromptAttemptContext::new(
        "session".into(),
        "run".into(),
        "turn".into(),
        WorkspaceContext::from_persisted("workspace", "/workspace"),
        "provider".into(),
        "model".into(),
        1,
        false,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_collector_retains_original_prompt_disposal_in_mount_and_parent() {
    for (typed, panic) in [(false, false), (false, true), (true, false), (true, true)] {
        let gate = Arc::new(Gate {
            panic,
            ..Default::default()
        });
        let registry = Registry::new();
        let mount = registry
            .mount_owned(
                Arc::new(Prompts {
                    gate: gate.clone(),
                    typed,
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&"session".into());
        let contributors = plan.prompt_contributors.clone();
        drop(plan);
        let parent = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
        let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
            collect_prompt_contributions(&contributors, context()).await
        }));
        gate.entered.notified().await;
        waiter.abort();
        gate.disposing.notified().await;
        drop(owner);
        let pending = parent.drain(Duration::from_millis(20)).await;
        let closing = mount.close(Duration::from_millis(20)).await;
        *gate.release.0.lock().unwrap() = true;
        gate.release.1.notify_all();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let joined = parent.drain(Duration::from_secs(2)).await;
        let closed = mount.close(Duration::from_secs(2)).await;
        assert_eq!(pending.pending, 1, "{pending:?}");
        assert!(matches!(
            closing,
            Err(ExtensionError::MountCloseTimeout { .. })
        ));
        assert_eq!(joined.pending, 0);
        assert_eq!(!joined.failures.is_empty(), panic, "{joined:?}");
        assert_eq!(closed.is_err(), panic, "{closed:?}");
    }
}
