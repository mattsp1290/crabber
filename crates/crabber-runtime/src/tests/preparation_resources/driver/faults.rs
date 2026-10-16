//! Physical ownership survives custom Store faults and mismatched sources.
use super::*;
#[path = "faults/late.rs"]
mod late;
#[path = "faults/pending.rs"]
mod pending;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn store_poll_panic_retains_actual_main_and_cleanup_until_their_joins() {
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
    h.gate.driver_poll_panic.store(true, Ordering::SeqCst);
    let handle = start_initial(&h, false).await;
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), observer.entered.notified())
        .await
        .unwrap();
    let resources = h.runtime.execution_tasks_for_test().tool_resources(
        handle.session_id().clone(),
        run.clone(),
        crabber_core::ToolCallId::new(),
        None,
    );
    let release_child = Arc::new(Notify::new());
    let child_release = release_child.clone();
    let child_returned = Arc::new(AtomicUsize::new(0));
    let returned = child_returned.clone();
    resources
        .resources()
        .spawn(async move {
            child_release.notified().await;
            returned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(7),
        h.gate.driver_poll_entered.notified(),
    )
    .await
    .unwrap();
    let first = h
        .runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    assert_eq!(first.pending, vec![run.clone()]);
    assert_eq!(first.joined.len(), 0);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Active
    );
    drop(release_main);
    let blocked_cleanup = h
        .runtime
        .drain_execution_tasks(Duration::from_millis(20))
        .await;
    assert_eq!(blocked_cleanup.pending, vec![run.clone()]);
    assert_eq!(child_returned.load(Ordering::SeqCst), 0);
    release_child.notify_one();
    let final_drain = h
        .runtime
        .drain_execution_tasks(Duration::from_secs(2))
        .await;
    assert_eq!(final_drain.pending.len(), 0);
    assert_eq!(final_drain.joined.len(), 1);
    assert_eq!(child_returned.load(Ordering::SeqCst), 1);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Joined
    );
    assert_eq!(h.fake.requests().len(), 1);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    handle.done().await.unwrap();
    h.provider.cleanup_release.notify_one();
}

#[tokio::test]
async fn activation_store_panics_join_actual_no_dispatch() {
    for point in [3, 4] {
        let h = Harness::new();
        h.gate
            .driver_activation_fault
            .store(point, Ordering::SeqCst);
        let handle = start_initial(&h, false).await;
        let run = handle.run_id().clone();
        assert!(handle.done().await.is_err());
        assert_eq!(h.fake.requests().len(), 0);
        assert_eq!(
            current_driver_state(&h, &run).await,
            SessionOwnerState::Joined
        );
        assert_eq!(
            h.runtime
                .drain_execution_tasks(Duration::from_secs(2))
                .await
                .pending
                .len(),
            0
        );
        h.provider.cleanup_release.notify_one();
    }
}

#[tokio::test]
async fn join_store_panics_retry_same_actual_observation() {
    for point in [3, 4] {
        let h = Harness::new();
        h.gate.driver_join_fault.store(point, Ordering::SeqCst);
        let handle = start_initial(&h, false).await;
        let run = handle.run_id().clone();
        handle.done().await.unwrap();
        h.runtime
            .drain_execution_tasks(Duration::from_secs(2))
            .await;
        assert_eq!(h.fake.requests().len(), 1);
        assert_eq!(h.gate.driver_join_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            current_driver_state(&h, &run).await,
            SessionOwnerState::Joined
        );
        h.provider.cleanup_release.notify_one();
    }
}

#[tokio::test]
async fn genuine_other_run_driver_grant_cannot_dispatch_this_runs_work() {
    use crabber_session::session_owner::{driver::ExecutionDriverOutcome, owned_scope::*};
    let h = Harness::new();
    // Actual prepared B admission; its deliberately unacknowledged initial
    // Register never reaches storage and invokes no model work.
    h.gate.driver_register_fault.store(1, Ordering::SeqCst);
    let b = start_initial(&h, false).await;
    let b_run = b.run_id().clone();
    assert!(b.done().await.is_err());
    let persisted = h.memory.get_run(&b_run).await.unwrap().unwrap();
    *h.gate.driver_register_request_override.lock().unwrap() = Some(RegisterExecutionDriver {
        context: SessionContext::new(
            persisted.session_id.clone(),
            h.input.workspace_id.clone(),
            h.input.directory.clone(),
        ),
        fence: crabber_core::RunFence {
            run_id: b_run.clone(),
            claim_token: persisted.claim_token,
        },
    });
    let mut input = h.input.clone();
    input.session_id = Some(SessionId::new());
    let crate::Admission::Started { handle: a, .. } =
        h.runtime.start_keyed(input, options()).await.unwrap()
    else {
        panic!("fresh A admission")
    };
    assert!(a.done().await.is_err());
    assert_eq!(h.fake.requests().len(), 0);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);

    // Tear down the genuinely acquired B owner using an actual registered
    // NoDispatch main and the original run's separately joined resources.
    let Some(ExecutionDriverOutcome::Applied { grant, .. }) =
        h.gate.driver_register_captured.lock().unwrap().take()
    else {
        panic!("genuine other-run grant")
    };
    let mut main = OwnedTaskRegistration::register(grant.scope_source().unwrap(), || async {
        panic!("unacknowledged work must not dispatch");
    })
    .unwrap();
    let tasks = h.runtime.execution_tasks_for_test().clone();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        main.activation().clone(),
        tokio::spawn(async move {
            tasks
                .finish_run_resources(&b_run)
                .await
                .map_err(|()| OwnedCleanupFailure::Failed)
        }),
    );
    let proof = main
        .join()
        .await
        .unwrap()
        .observe_resources(&mut cleanup)
        .await
        .unwrap();
    h.memory
        .join_owned_scope(proof, grant.authority().clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.provider.cleanup_completed.load(Ordering::SeqCst) != 2 {
            h.provider.cleanup_release.notify_waiters();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
