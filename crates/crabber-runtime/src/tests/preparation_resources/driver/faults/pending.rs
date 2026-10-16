//! Retain an in-flight Store control call while cancellation closes resources.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pending_store_poll_survives_cancellation_while_resource_closure_progresses() {
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
    h.gate.driver_poll_hold.store(true, Ordering::SeqCst);
    let handle = start_initial(&h, false).await;
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), observer.entered.notified())
        .await
        .unwrap();
    let resource = handle
        .worker_resources_for_test()
        .tool_resources(crabber_core::ToolCallId::new(), None);
    let closing = resource.resources().closing().unwrap();
    let release_child = Arc::new(Notify::new());
    let child_release = release_child.clone();
    let child_closing = Arc::new(Notify::new());
    let started = child_closing.clone();
    let gate = h.gate.clone();
    resource
        .resources()
        .spawn(async move {
            closing.await;
            // This original in-flight Store call can finish only after actual
            // resource closure begins. A replacement poll cannot complete it.
            gate.driver_poll_release.notify_one();
            started.notify_one();
            child_release.notified().await;
            Ok(())
        })
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(7),
        h.gate.driver_poll_entered.notified(),
    )
    .await
    .unwrap();
    handle.interrupt();
    tokio::time::timeout(Duration::from_secs(2), child_closing.notified())
        .await
        .expect("cancellation must close resources during the original pending Store call");
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.gate.driver_poll_completed.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original Store future must finish, not be dropped or replaced");
    let main_pending = h
        .runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    assert_eq!(main_pending.pending, vec![run.clone()]);
    drop(release_main);
    let child_pending = h
        .runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    assert_eq!(child_pending.pending, vec![run.clone()]);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Active
    );
    release_child.notify_one();
    let joined = h
        .runtime
        .drain_execution_tasks(Duration::from_secs(2))
        .await;
    assert_eq!(joined.pending.len(), 0);
    assert_eq!(joined.joined.len(), 1);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Joined
    );
    assert_eq!(h.gate.driver_poll_completed.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 1);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    handle.done().await.unwrap();
    h.provider.cleanup_release.notify_one();
}
