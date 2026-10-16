use super::*;
use crabber_extension::{CallbackInvocationOwner, ToolResourceOwner, with_callback_invocations};
use std::{
    future::Future,
    pin::Pin,
    sync::{Condvar, Mutex},
    task::{Context, Poll},
};
use tokio::sync::Notify;

#[derive(Default)]
struct Gate {
    entered: Notify,
    disposing: Notify,
    release: (Mutex<bool>, Condvar),
    panic: bool,
}
struct Pending(Arc<Gate>);
impl Future for Pending {
    type Output = Result<ToolOutput, ExtensionError>;
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
        assert!(!self.0.panic, "original executor future disposal panic");
    }
}
struct Blocked(Arc<Gate>);
#[async_trait]
impl ToolExecutor for Blocked {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        Pending(self.0.clone()).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_drop_keeps_same_executor_join_in_mount_and_real_phase_resources() {
    for panic in [false, true] {
        let gate = Arc::new(Gate {
            panic,
            ..Default::default()
        });
        let registry = Registry::new();
        let mount = registry
            .mount_owned(
                Arc::new(Tools(Arc::new(Blocked(gate.clone())))),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let executor = plan.tools[0].executor.clone();
        drop(plan);
        let phase = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(phase.resources()).unwrap();
        let capability = owner.capability();
        let waiter = tokio::spawn(with_callback_invocations(capability, async move {
            executor.execute_with_context(context(), Value::Null).await
        }));
        gate.entered.notified().await;
        waiter.abort();
        gate.disposing.notified().await;
        drop(owner);
        let pending = phase.drain(Duration::from_millis(20)).await;
        let closing = mount.close(Duration::from_millis(20)).await;
        *gate.release.0.lock().unwrap() = true;
        gate.release.1.notify_all();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let joined = phase.drain(Duration::from_secs(2)).await;
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

struct SelfCloser(Arc<Mutex<Option<crabber_extension::OwnedMountHandle>>>);
#[async_trait]
impl ToolExecutor for SelfCloser {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        let mount = self.0.lock().unwrap().clone().unwrap();
        assert!(matches!(
            mount.close(Duration::from_millis(100)).await,
            Err(ExtensionError::SelfClose)
        ));
        Ok(ToolOutput::from_json(&Value::Null).unwrap())
    }
}
#[tokio::test]
async fn original_executor_inherits_actual_mount_self_close_identity() {
    let holder = Arc::new(Mutex::new(None));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Tools(Arc::new(SelfCloser(holder.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    *holder.lock().unwrap() = Some(mount.clone());
    let plan = registry.acquire(&SessionId::new());
    plan.tools[0].executor.execute(Value::Null).await.unwrap();
    holder.lock().unwrap().take();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
}
