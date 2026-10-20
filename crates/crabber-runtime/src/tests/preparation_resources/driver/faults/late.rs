//! Close before main completion; finish cleanup after the actual main join.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn panic_cleanup_cannot_omit_a_late_registered_scope() {
    late_resource(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn interrupt_closes_resources_before_main_join_and_covers_late_scopes() {
    late_resource(false).await;
}
async fn late_resource(panic: bool) {
    let mut h = Harness::new();
    let observer = Arc::new(RunObservationBarrier::default());
    let release_main = ReleaseObservation(observer.clone());
    h.runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(Arc::new(DelayedTerminalStore {
            inner: h.memory.clone(),
            gate: h.gate.clone(),
        }))
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(h.provider.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    h.gate.driver_poll_panic.store(panic, Ordering::SeqCst);
    let handle = start_initial(&h, false).await;
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), observer.entered.notified())
        .await
        .unwrap();
    let early = handle
        .worker_resources_for_test()
        .tool_resources(crabber_core::ToolCallId::new(), None);
    if panic {
        tokio::time::timeout(
            Duration::from_secs(7),
            h.gate.driver_poll_entered.notified(),
        )
        .await
        .unwrap();
    } else {
        handle.interrupt();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while !early.snapshot().sealed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resource closure must not wait for the blocked main");
    // Allow the supervisor's independently spawned cleanup task to finish
    // its early empty drain while the actual main remains blocked. This delay
    // is scheduling only; all assertions below require actual retained joins.
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The earlier empty scope has physically drained, but the original main
    // can still acquire/register a later scope before it actually returns.
    let late = handle
        .worker_resources_for_test()
        .tool_resources(crabber_core::ToolCallId::new(), None);
    let released = Arc::new(Notify::new());
    let wait = released.clone();
    let returned = Arc::new(AtomicUsize::new(0));
    let finished = returned.clone();
    late.resources()
        .spawn(async move {
            wait.notified().await;
            finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    drop(release_main);
    let blocked = h
        .runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    assert_eq!(blocked.pending, vec![run.clone()]);
    assert_eq!(blocked.joined.len(), 0);
    assert_eq!(returned.load(Ordering::SeqCst), 0);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Active
    );
    released.notify_one();
    let joined = h
        .runtime
        .drain_execution_tasks(Duration::from_secs(2))
        .await;
    assert_eq!(joined.pending.len(), 0);
    assert_eq!(joined.joined.len(), 1);
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Joined
    );
    handle.done().await.unwrap();
    assert_eq!(h.fake.requests().len(), 1);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    h.provider.cleanup_release.notify_one();
}
