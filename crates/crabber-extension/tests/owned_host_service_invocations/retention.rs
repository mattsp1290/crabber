use super::*;
use crabber_extension::{CallbackInvocationOwner, ToolResourceOwner, with_callback_invocations};
use std::{
    future::Future,
    marker::PhantomData,
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
struct Pending<T>(Arc<Gate>, PhantomData<T>);
impl<T> Future for Pending<T> {
    type Output = Result<T, String>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let mut released = self.0.release.0.lock().unwrap();
        while !*released {
            released = self.0.release.1.wait(released).unwrap();
        }
        assert!(!self.0.panic, "original host service disposal panic");
    }
}
struct Blocked(Arc<Gate>);
#[async_trait]
impl Subprocess for Blocked {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        assert_eq!(program, "program");
        assert_eq!(arguments, &["argument"]);
        Pending(self.0.clone(), PhantomData).await
    }
}
#[async_trait]
impl WorkspaceFs for Blocked {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        assert_eq!(path, "file");
        Pending(self.0.clone(), PhantomData).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_service_waiters_keep_original_disposal_in_mount_and_caller_scope() {
    for filesystem in [false, true] {
        for panic in [false, true] {
            retained(filesystem, panic).await;
        }
    }
}
async fn retained(filesystem: bool, panic: bool) {
    let gate = Arc::new(Gate {
        panic,
        ..Default::default()
    });
    let service = Arc::new(Blocked(gate.clone()));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Services {
                subprocess: service.clone(),
                filesystem: service,
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let services = plan.host_services.clone();
    drop(plan);
    let phase = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(phase.resources()).unwrap();
    let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        if filesystem {
            services
                .workspace_fs
                .unwrap()
                .read("file")
                .await
                .map(|_| ())
        } else {
            services
                .subprocess
                .unwrap()
                .run("program", &["argument".into()])
                .await
                .map(|_| ())
        }
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

struct SelfCloser(Arc<Mutex<Option<crabber_extension::OwnedMountHandle>>>);
#[async_trait]
impl Subprocess for SelfCloser {
    async fn run(&self, _: &str, _: &[String]) -> Result<Value, String> {
        let mount = self.0.lock().unwrap().clone().unwrap();
        assert!(matches!(
            mount.close(Duration::from_millis(100)).await,
            Err(ExtensionError::SelfClose)
        ));
        Ok(Value::Null)
    }
}
#[async_trait]
impl WorkspaceFs for SelfCloser {
    async fn read(&self, _: &str) -> Result<Vec<u8>, String> {
        let mount = self.0.lock().unwrap().clone().unwrap();
        assert!(matches!(
            mount.close(Duration::from_millis(100)).await,
            Err(ExtensionError::SelfClose)
        ));
        Ok(vec![])
    }
}
#[tokio::test]
async fn original_host_services_keep_actual_mount_self_close_identity() {
    let holder = Arc::new(Mutex::new(None));
    let service = Arc::new(SelfCloser(holder.clone()));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Services {
                subprocess: service.clone(),
                filesystem: service,
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    *holder.lock().unwrap() = Some(mount.clone());
    let plan = registry.acquire(&SessionId::new());
    plan.host_services
        .subprocess
        .as_ref()
        .unwrap()
        .run("program", &[])
        .await
        .unwrap();
    plan.host_services
        .workspace_fs
        .as_ref()
        .unwrap()
        .read("file")
        .await
        .unwrap();
    holder.lock().unwrap().take();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
}
