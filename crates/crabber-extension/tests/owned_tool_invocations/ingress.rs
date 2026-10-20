use super::*;
use std::{
    future::Future,
    task::{Context, Poll},
};

#[tokio::test]
async fn seal_after_registration_before_original_body_has_zero_effects() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Tools(Arc::new(Counter(calls.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let mut call = Box::pin(plan.tools[0].executor.execute(Value::Null));
    // On this current-thread executor the parked actual task cannot poll until
    // we yield; its admission has already published the original JoinHandle.
    assert!(matches!(
        call.as_mut()
            .poll(&mut Context::from_waker(&futures::task::noop_waker())),
        Poll::Pending
    ));
    mount.deactivate();
    assert!(call.await.is_err());
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct Blocked(Arc<AtomicUsize>, Arc<tokio::sync::Notify>);
#[async_trait]
impl ToolExecutor for Blocked {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.notify_one();
        futures::future::pending().await
    }
}
#[tokio::test]
async fn active_capacity_refuses_before_original_constructor_and_close_joins_roster() {
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Tools(Arc::new(Blocked(calls.clone(), entered.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let executor = plan.tools[0].executor.clone();
    drop(plan);
    let mut waiters = Vec::new();
    for _ in 0..64 {
        let executor = executor.clone();
        waiters.push(tokio::spawn(
            async move { executor.execute(Value::Null).await },
        ));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) != 64 {
            entered.notified().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        executor.execute(Value::Null).await,
        Err(ExtensionError::PreparationRegistration(
            crabber_extension::ResourceRegistrationError::Capacity
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 64);
    mount.close(Duration::from_secs(2)).await.unwrap();
    for waiter in waiters {
        assert!(waiter.await.unwrap().is_err());
    }
}
