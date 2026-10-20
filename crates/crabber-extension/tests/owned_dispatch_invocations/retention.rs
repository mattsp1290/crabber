use super::*;
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
    stopped: AtomicUsize,
}
struct Pending(Arc<Gate>);
impl Future for Pending {
    type Output = Result<Value, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let (lock, notify) = &self.0.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = notify.wait(released).unwrap();
        }
    }
}
struct Blocked(Arc<Gate>);
#[async_trait]
impl Extension for Blocked {
    fn id(&self) -> &'static str {
        "blocked-callback"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let gate = self.0.clone();
        r.on_hook(
            TurnPrepare::ID,
            0,
            "blocked",
            Arc::new(move |_| Box::pin(Pending(gate.clone()))),
        );
        Ok(())
    }
    async fn shutdown(&self) {
        self.0.stopped.fetch_add(1, Ordering::SeqCst);
    }
}
struct BlockedResult(Arc<Gate>);
#[async_trait]
impl Extension for BlockedResult {
    fn id(&self) -> &'static str {
        "blocked-result-callback"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let gate = self.0.clone();
        r.on_result_transform(
            0,
            "blocked-result",
            Arc::new(move |_, value| {
                let pending = Pending(gate.clone());
                Box::pin(async move {
                    pending.await?;
                    Ok(crabber_extension::TransformOutput::new(value))
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_result_phase_quiescence_waits_same_original_callback_disposal_join() {
    use crabber_core::{RunId, ToolCallId, ToolOutput};
    use crabber_extension::{
        ToolInput, ToolOutcomeClass, ToolResourceOwner, ToolResultContext, ToolResultOutcome,
        TransformOutput,
    };
    let registry = Registry::new();
    let gate = Arc::new(Gate::default());
    let handle = registry
        .mount_owned(Arc::new(BlockedResult(gate.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    let phase = ToolResourceOwner::new();
    let token = tokio_util::sync::CancellationToken::new();
    let context = ToolResultContext::new(
        "fixture".into(),
        true,
        ToolInput::Normalized(Value::Null),
        ToolCallId::new(),
        SessionId::new(),
        RunId::new(),
        ToolOutcomeClass::Succeeded,
    )
    .with_cleanup(phase.cleanup_tracker())
    .with_cancellation(token.clone());
    let driver = tokio::spawn(async move {
        dispatcher
            .transform_tool_result(
                context,
                TransformOutput::new(ToolOutput::from_json(&Value::Null).unwrap()),
            )
            .await
    });
    gate.entered.notified().await;
    token.cancel();
    assert!(matches!(
        driver.await.unwrap(),
        ToolResultOutcome::Interrupted { .. }
    ));
    gate.disposing.notified().await;
    let while_blocked = phase.drain(Duration::from_millis(25)).await;
    let no_shutdown = gate.stopped.load(Ordering::SeqCst);
    *gate.release.0.lock().unwrap() = true;
    gate.release.1.notify_all();
    let joined = phase.drain(Duration::from_secs(2)).await;
    handle.close(Duration::from_secs(2)).await.unwrap();
    assert!(
        while_blocked.pending > 0,
        "phase must own the actual in-progress callback join: {while_blocked:?}"
    );
    assert_eq!(no_shutdown, 0);
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.joined, 1);
    assert_eq!(
        joined.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambient_actual_parent_resources_and_owner_drop_both_retain_original_joins() {
    use crabber_extension::{
        CallbackInvocationOwner, ToolResourceOwner, with_callback_invocations,
    };
    for owner_drop in [false, true] {
        let registry = Registry::new();
        let gate = Arc::new(Gate::default());
        let handle = registry
            .mount_owned(Arc::new(Blocked(gate.clone())), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let dispatcher = plan.dispatcher.clone();
        drop(plan);
        let parent = ToolResourceOwner::new();
        let mut owner = Some(CallbackInvocationOwner::attach(parent.resources()).unwrap());
        let capability = owner.as_ref().unwrap().capability();
        let retained = capability.clone();
        let caller = tokio::spawn(async move {
            with_callback_invocations(capability, dispatcher.hook::<TurnPrepare>(Value::Null)).await
        });
        gate.entered.notified().await;
        if owner_drop {
            drop(owner.take());
        } else {
            parent.close();
        }
        gate.disposing.notified().await;
        let while_blocked = parent.drain(Duration::from_millis(25)).await;
        let stopped = gate.stopped.load(Ordering::SeqCst);
        *gate.release.0.lock().unwrap() = true;
        gate.release.1.notify_all();
        assert!(caller.await.unwrap().is_err());
        let joined = parent.drain(Duration::from_secs(2)).await;
        handle.close(Duration::from_secs(2)).await.unwrap();
        assert_eq!(while_blocked.pending, 1);
        assert_eq!(stopped, 0);
        assert_eq!((joined.pending, joined.joined), (0, 1));
        assert_eq!(
            joined.failures,
            [] as [crabber_extension::ResourceFailure; 0]
        );
        // Retained capabilities never reopen the parent's sealed ingress.
        let calls = Arc::new(AtomicUsize::new(0));
        let other_registry = Registry::new();
        let other = other_registry
            .mount_owned(Arc::new(Counter(calls.clone())), Scope::Global)
            .await
            .unwrap();
        let other_plan = other_registry.acquire(&SessionId::new());
        assert!(
            with_callback_invocations(
                retained,
                other_plan.dispatcher.hook::<TurnPrepare>(Value::Null)
            )
            .await
            .is_err()
        );
        drop(other_plan);
        other.close(Duration::from_secs(2)).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn ambient_sequential_callbacks_consume_one_actual_parent_observer() {
    use crabber_extension::{
        CallbackInvocationOwner, ToolResourceOwner, with_callback_invocations,
    };
    assert!(matches!(
        CallbackInvocationOwner::attach(crabber_extension::ToolResources::default()),
        Err(crabber_extension::ResourceRegistrationError::Unsupported)
    ));
    let registry = Registry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .mount_owned(Arc::new(Counter(calls.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    let parent = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        for _ in 0..256 {
            dispatcher.hook::<TurnPrepare>(Value::Null).await.unwrap();
        }
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 256);
    drop(owner);
    let joined = parent.drain(Duration::from_secs(2)).await;
    assert_eq!((joined.pending, joined.joined), (0, 1));
    assert_eq!(
        joined.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    handle.close(Duration::from_secs(2)).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_caller_retains_actual_callback_disposal_before_shutdown() {
    let registry = Registry::new();
    let gate = Arc::new(Gate::default());
    let handle = registry
        .mount_owned(Arc::new(Blocked(gate.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    let caller = tokio::spawn(async move { dispatcher.hook::<TurnPrepare>(Value::Null).await });
    gate.entered.notified().await;
    caller.abort();
    let _ = caller.await;
    // The caller's cancellation must request disposal in the retained task.
    gate.disposing.notified().await;
    assert!(matches!(
        handle.close(Duration::from_millis(25)).await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    assert_eq!(gate.stopped.load(Ordering::SeqCst), 0);
    *gate.release.0.lock().unwrap() = true;
    gate.release.1.notify_all();
    handle.close(Duration::from_secs(2)).await.unwrap();
    assert_eq!(gate.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn successful_actual_joins_are_reaped_without_lifetime_capacity_exhaustion() {
    let registry = Registry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .mount_owned(Arc::new(Counter(calls.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    for _ in 0..256 {
        dispatcher.hook::<TurnPrepare>(Value::Null).await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 256);
    handle.close(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn close_after_registration_before_actual_task_body_prevents_constructor() {
    let registry = Registry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .mount_owned(Arc::new(Counter(calls.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    let call = dispatcher.hook::<TurnPrepare>(Value::Null);
    tokio::pin!(call);
    assert!(futures::poll!(&mut call).is_pending());
    // Single-thread test has not yielded to the registered actual main. Close
    // seals first, so its actual-body check must refuse callback construction.
    handle.close(Duration::from_secs(2)).await.unwrap();
    assert!(call.await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct Capacity {
    entered: Arc<AtomicUsize>,
    gate: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl Extension for Capacity {
    fn id(&self) -> &'static str {
        "invocation-capacity"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let entered = self.entered.clone();
        let gate = self.gate.clone();
        r.on_hook(
            TurnPrepare::ID,
            0,
            "capacity",
            Arc::new(move |_| {
                entered.fetch_add(1, Ordering::SeqCst);
                let gate = gate.clone();
                Box::pin(async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(Value::Null)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn active_capacity_refuses_before_construction_and_reaps_original_finished_joins() {
    let registry = Registry::new();
    let entered = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handle = registry
        .mount_owned(
            Arc::new(Capacity {
                entered: entered.clone(),
                gate: gate.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    let mut waiters = vec![];
    for _ in 0..64 {
        let dispatcher = dispatcher.clone();
        let mut call = Box::pin(async move { dispatcher.hook::<TurnPrepare>(Value::Null).await });
        assert!(futures::poll!(&mut call).is_pending());
        waiters.push(call);
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while entered.load(Ordering::SeqCst) != 64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        dispatcher.hook::<TurnPrepare>(Value::Null).await,
        Err(ExtensionError::PreparationRegistration(
            crabber_extension::ResourceRegistrationError::Capacity
        ))
    ));
    assert_eq!(entered.load(Ordering::SeqCst), 64);
    gate.add_permits(65);
    // Original output waiters remain unpolled; only actual handle joins may
    // permit the next construction, not their output-channel readiness alone.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    dispatcher.hook::<TurnPrepare>(Value::Null).await.unwrap();
    for waiter in waiters {
        waiter.await.unwrap();
    }
    assert_eq!(entered.load(Ordering::SeqCst), 65);
    handle.close(Duration::from_secs(2)).await.unwrap();
}

struct PanicDispose(u8);
struct PanicPayload;
impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("secondary panic payload disposal");
    }
}
impl Future for PanicDispose {
    type Output = Result<Value, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 == 1 {
            std::panic::panic_any(PanicPayload);
        }
        Poll::Ready(Ok(Value::Null))
    }
}
impl Drop for PanicDispose {
    fn drop(&mut self) {
        if self.0 == 2 {
            std::panic::panic_any(PanicPayload);
        }
    }
}
struct Panics(u8);
#[async_trait]
impl Extension for Panics {
    fn id(&self) -> &'static str {
        "panic-dispose"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let mode = self.0;
        r.on_hook(
            TurnPrepare::ID,
            0,
            "dispose",
            Arc::new(move |_| {
                if mode == 0 {
                    std::panic::panic_any(PanicPayload);
                }
                Box::pin(PanicDispose(mode))
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn actual_disposal_panic_is_sticky_and_cannot_return_successful_output_or_join() {
    for mode in 0..3 {
        let registry = Registry::new();
        let handle = registry
            .mount_owned(Arc::new(Panics(mode)), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        assert!(matches!(
            plan.dispatcher.hook::<TurnPrepare>(Value::Null).await,
            Err(ExtensionError::MountCleanupIncomplete { .. })
        ));
        drop(plan);
        for _ in 0..2 {
            assert!(matches!(
                handle.close(Duration::from_secs(2)).await,
                Err(ExtensionError::MountCleanupIncomplete { .. })
            ));
        }
    }
}
