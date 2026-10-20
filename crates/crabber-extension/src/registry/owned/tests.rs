use super::*;
use crate::{
    CleanupTracker, Extension, Registrar, Registry, Scope, ToolInput, ToolOutcomeClass,
    ToolResourceOwner, ToolResultContext, ToolResultOutcome, TransformOutput,
};
use async_trait::async_trait;
use crabber_core::{RunId, SessionId, ToolCallId, ToolOutput};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
const BOUND: Duration = Duration::from_secs(3);
#[derive(Default)]
struct Probe {
    tracker: Arc<Mutex<Option<CleanupTracker>>>,
    release: Arc<Notify>,
    failure: Option<ResourceFailure>,
    deferred_panic: bool,
    shutdown_panic: bool,
    shutdown_gate: Option<Arc<Notify>>,
    shutdown_started: Arc<Notify>,
    finished: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for Probe {
    fn id(&self) -> &'static str {
        "owned-mount"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let tracker = self.tracker.clone();
        let release = self.release.clone();
        let failure = self.failure;
        registrar.on_result_transform(
            0,
            "owned-handler",
            Arc::new(move |context, value| {
                *tracker.lock().unwrap() = Some(context.cleanup().clone());
                let release = release.clone();
                context
                    .cleanup()
                    .spawn_checked(async move {
                        release.notified().await;
                        failure.map_or(Ok(()), Err)
                    })
                    .unwrap();
                Box::pin(async move { Ok(TransformOutput::new(value)) })
            }),
        );
        let finished = self.finished.clone();
        registrar.defer(move || {
            finished.fetch_add(1, Ordering::SeqCst);
        });
        if self.deferred_panic {
            registrar.defer(|| panic!("contained deferred cleanup"));
        }
        Ok(())
    }
    async fn shutdown(&self) {
        self.shutdown_started.notify_one();
        if let Some(gate) = &self.shutdown_gate {
            gate.notified().await;
        }
        assert!(!self.shutdown_panic, "contained shutdown");
        self.finished.fetch_add(1, Ordering::SeqCst);
    }
}
fn context() -> ToolResultContext {
    ToolResultContext::new(
        "test".into(),
        true,
        ToolInput::Normalized(serde_json::Value::Null),
        ToolCallId::new(),
        SessionId::new(),
        RunId::new(),
        ToolOutcomeClass::Succeeded,
    )
}
async fn invoke(registry: &Registry, phase: Option<&ToolResourceOwner>) {
    let plan = registry.acquire(&SessionId::new());
    let mut context = context();
    if let Some(phase) = phase {
        context = context.with_cleanup(phase.cleanup_tracker());
    }
    let output = ToolOutput::from_json(&serde_json::Value::Null).unwrap();
    assert!(matches!(
        plan.dispatcher
            .transform_tool_result(context, TransformOutput::new(output))
            .await,
        ToolResultOutcome::Completed { .. }
    ));
}
#[tokio::test]
async fn owned_mount_and_phase_retain_the_same_join_and_reject_late_poll() {
    let registry = Registry::new();
    let probe = Arc::new(Probe::default());
    let handle = registry
        .mount_owned(probe.clone(), Scope::Global)
        .await
        .unwrap();
    let phase = ToolResourceOwner::new();
    invoke(&registry, Some(&phase)).await;
    // The actual callback-join observer is separate from its cleanup task.
    assert_eq!(phase.snapshot().pending, 2);
    assert_eq!(handle.cleanup_snapshot().pending, 1);
    assert!(matches!(
        handle.close(Duration::ZERO).await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    assert!(!phase.snapshot().sealed);
    let polls = Arc::new(AtomicUsize::new(0));
    let future_polls = polls.clone();
    let retained = probe.tracker.lock().unwrap().clone().unwrap();
    assert_eq!(
        retained.spawn_checked(async move {
            future_polls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
        Err(crate::ResourceRegistrationError::Sealed)
    );
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    probe.release.notify_one();
    let phase_join = phase.drain(BOUND).await;
    assert_eq!((phase_join.joined, phase_join.pending), (2, 0));
    let observed = handle.close(BOUND).await.unwrap();
    assert_eq!(
        (observed.cleanup().joined, observed.close_task().joined),
        (1, 1)
    );
    assert_eq!(probe.finished.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn owned_close_waits_for_actual_shutdown_join_and_keeps_failure_sticky() {
    for (deferred_panic, shutdown_panic) in [(false, false), (true, false), (false, true)] {
        let gate = Arc::new(Notify::new());
        let probe = Arc::new(Probe {
            deferred_panic,
            shutdown_panic,
            shutdown_gate: Some(gate.clone()),
            ..Probe::default()
        });
        let registry = Registry::new();
        let handle = registry
            .mount_owned(probe.clone(), Scope::Global)
            .await
            .unwrap();
        assert!(matches!(
            handle.close(Duration::ZERO).await,
            Err(ExtensionError::MountCloseTimeout { .. })
        ));
        probe.shutdown_started.notified().await;
        // Empty cleanup and closed/drained publication are never enough: the
        // actual shutdown/close task is still retained and has not joined.
        assert_eq!(handle.cleanup_snapshot().pending, 0);
        assert_eq!(handle.close_snapshot().pending, 1);
        assert!(matches!(
            handle.close(Duration::ZERO).await,
            Err(ExtensionError::MountCloseTimeout { .. })
        ));
        gate.notify_one();
        for _ in 0..2 {
            let result = handle.close(BOUND).await;
            if deferred_panic || shutdown_panic {
                assert!(matches!(
                    result,
                    Err(ExtensionError::MountCleanupIncomplete { .. })
                ));
            } else {
                assert_eq!(result.unwrap().close_task().joined, 1);
            }
        }
        assert_eq!(
            probe.finished.load(Ordering::SeqCst),
            if shutdown_panic { 1 } else { 2 }
        );
    }
}
#[tokio::test]
async fn close_all_routes_owned_cleanup_failure_and_seals_retained_tracker() {
    let registry = Registry::new().with_close_timeout(BOUND);
    let probe = Arc::new(Probe {
        failure: Some(ResourceFailure::CleanupFailed),
        ..Probe::default()
    });
    let handle = registry
        .mount_owned(probe.clone(), Scope::Global)
        .await
        .unwrap();
    invoke(&registry, None).await;
    probe.release.notify_one();
    assert!(matches!(
        registry.close_all().await,
        Err(ExtensionError::MountCleanupIncomplete { .. })
    ));
    assert_eq!(
        handle.cleanup_snapshot().failures,
        vec![ResourceFailure::CleanupFailed]
    );
    assert!(matches!(
        handle.close(BOUND).await,
        Err(ExtensionError::MountCleanupIncomplete { .. })
    ));
    let tracker = probe.tracker.lock().unwrap().clone().unwrap();
    assert_eq!(
        tracker.spawn_checked(async { Ok(()) }),
        Err(crate::ResourceRegistrationError::Sealed)
    );
    assert!(registry.try_acquire(&SessionId::new()).is_err());
}
#[tokio::test]
async fn owned_policy_changes_both_registration_and_plan_fingerprints() {
    let legacy = Registry::new();
    let owned = Registry::new();
    let a = legacy
        .mount(Arc::new(Probe::default()), Scope::Global)
        .await
        .unwrap();
    let b = owned
        .mount_owned(Arc::new(Probe::default()), Scope::Global)
        .await
        .unwrap();
    assert_ne!(
        legacy.registration_snapshot().unwrap().all_declarations(),
        owned.registration_snapshot().unwrap().all_declarations()
    );
    let old = legacy.acquire(&SessionId::new());
    let new = owned.acquire(&SessionId::new());
    assert_ne!(old.fingerprint, new.fingerprint);
    drop(old);
    drop(new);
    a.close().await.unwrap();
    b.close(BOUND).await.unwrap();
}

#[tokio::test]
async fn dropped_output_handle_keeps_cleanup_panic_and_late_spawn_is_unpolled() {
    let registry = Registry::new();
    let handle = registry
        .mount_owned(Arc::new(Probe::default()), Scope::Global)
        .await
        .unwrap();
    let tracker = handle.inner.mount.registrar.lock().unwrap().handlers[0]
        .cleanup
        .clone();
    drop(tracker.spawn(async {
        panic!("retained cleanup panic");
    }));
    assert!(matches!(
        handle.close(BOUND).await,
        Err(ExtensionError::MountCleanupIncomplete { .. })
    ));
    assert_eq!(
        handle.cleanup_snapshot().failures,
        vec![ResourceFailure::Panicked]
    );
    let polls = Arc::new(AtomicUsize::new(0));
    let future_polls = polls.clone();
    let late = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tracker.spawn(async move {
            future_polls.fetch_add(1, Ordering::SeqCst);
        })
    }));
    assert!(late.is_err());
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

struct BlockingDeferred {
    release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    started: Arc<Notify>,
}
#[async_trait]
impl Extension for BlockingDeferred {
    fn id(&self) -> &'static str {
        "blocking-deferred"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let release = self.release.lock().unwrap().take().unwrap();
        let started = self.started.clone();
        registrar.defer(move || {
            started.notify_one();
            release.recv().unwrap();
        });
        Ok(())
    }
}
#[tokio::test]
async fn owned_close_retains_blocking_deferred_join_under_caller_deadline() {
    let registry = Registry::new();
    let (release, wait) = std::sync::mpsc::channel();
    let started = Arc::new(Notify::new());
    let extension = Arc::new(BlockingDeferred {
        release: Mutex::new(Some(wait)),
        started: started.clone(),
    });
    let handle = registry
        .mount_owned(extension, Scope::Global)
        .await
        .unwrap();
    assert!(matches!(
        handle.close(Duration::ZERO).await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    started.notified().await;
    assert!(matches!(
        handle.close(Duration::from_millis(2)).await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    assert_eq!(handle.close_snapshot().pending, 1);
    release.send(()).unwrap();
    assert_eq!(handle.close(BOUND).await.unwrap().close_task().joined, 1);
}
#[tokio::test]
async fn owned_concurrent_close_retains_one_task_and_waits_for_actual_plan_release() {
    let registry = Registry::new();
    let handle = registry
        .mount_owned(Arc::new(Probe::default()), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let (a, b) = tokio::join!(handle.close(Duration::ZERO), handle.close(Duration::ZERO));
    assert!(matches!(a, Err(ExtensionError::MountCloseTimeout { .. })));
    assert!(matches!(b, Err(ExtensionError::MountCloseTimeout { .. })));
    assert_eq!(handle.close_snapshot().pending, 1);
    drop(plan);
    assert_eq!(handle.close(BOUND).await.unwrap().close_task().joined, 1);
}
