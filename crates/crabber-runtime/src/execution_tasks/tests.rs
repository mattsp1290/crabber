use super::*;
use tokio::sync::{Notify, oneshot};

#[tokio::test]
async fn cancelled_drain_retains_join_handle_and_concurrent_joiners_reconcile() {
    let tasks = ExecutionTasks::default();
    let run = RunId::new();
    let release = Arc::new(Notify::new());
    let released = release.clone();
    let task = tasks.spawn(SessionId::new(), run.clone(), async move {
        released.notified().await;
    });
    let report = tasks.drain(Duration::from_millis(1)).await;
    assert_eq!(report.joined, [] as [crate::JoinedExecutionTask; 0]);
    assert_eq!(report.pending, vec![run.clone()]);
    release.notify_one();
    let (left, right) = tokio::join!(tasks.join(&task), tasks.join(&task));
    assert_eq!(left, TaskTermination::Returned);
    assert_eq!(right, left);
    assert_eq!(tasks.pending(None).0, [] as [RunId; 0]);
    assert_eq!(tasks.join(&task).await, left);
}

#[tokio::test]
async fn dropped_waiter_does_not_cancel_work_or_relabel_return_as_join() {
    let tasks = ExecutionTasks::default();
    let (sender, receiver) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let released = release.clone();
    let run = RunId::new();
    let task = tasks.spawn(SessionId::new(), run.clone(), async move {
        let _ = sender.send(());
        released.notified().await;
    });
    receiver.await.unwrap();
    // A result/notification can arrive while the actual task still owns work.
    assert_eq!(tasks.pending(None).0, vec![run]);
    drop(task);
    assert_eq!(tasks.drain(Duration::from_millis(1)).await.pending.len(), 1);
    release.notify_one();
    let report = tasks.drain(Duration::from_secs(1)).await;
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(report.joined.len(), 1);
    assert_eq!(report.joined[0].termination, TaskTermination::Returned);
}

#[tokio::test]
async fn retains_distinct_tasks_for_the_same_run_and_sanitizes_panics() {
    let tasks = ExecutionTasks::default();
    let run = RunId::new();
    tasks.spawn(SessionId::new(), run.clone(), async {
        panic!("private callback panic")
    });
    tasks.spawn(SessionId::new(), run, async {});
    assert_eq!(tasks.pending(None).0.len(), 2);
    let report = tasks.drain(Duration::from_secs(1)).await;
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(report.joined.len(), 2);
    assert!(
        report
            .joined
            .iter()
            .any(|r| r.termination == TaskTermination::Panicked)
    );
    assert!(
        report
            .joined
            .iter()
            .any(|r| r.termination == TaskTermination::Returned)
    );
}

#[tokio::test]
async fn panic_payload_drop_cannot_destroy_retained_join_evidence() {
    struct Payload;
    impl Drop for Payload {
        fn drop(&mut self) {
            panic!("private payload drop")
        }
    }
    let tasks = ExecutionTasks::default();
    let task = tasks.spawn(SessionId::new(), RunId::new(), async {
        std::panic::panic_any(Payload)
    });
    assert_eq!(tasks.join(&task).await, TaskTermination::Panicked);
    assert_eq!(tasks.join(&task).await, TaskTermination::Panicked);
    assert_eq!(tasks.pending(None).0, [] as [RunId; 0]);
}

#[tokio::test]
async fn resource_drain_cannot_hide_a_worker_started_during_its_wait() {
    let tasks = ExecutionTasks::default();
    let scope = tasks.tool_resources(SessionId::new(), RunId::new(), ToolCallId::new(), None);
    let release = Arc::new(Notify::new());
    let wait = release.clone();
    scope
        .resources()
        .spawn(async move {
            wait.notified().await;
            Ok(())
        })
        .unwrap();
    scope.close();
    let mut drain = Box::pin(tasks.drain(Duration::from_millis(10)));
    assert!(futures::poll!(drain.as_mut()).is_pending());
    let new_run = RunId::new();
    let worker = tasks.spawn(SessionId::new(), new_run.clone(), async {
        std::future::pending::<()>().await;
    });
    let report = drain.await;
    assert!(report.pending.contains(&new_run));
    release.notify_one();
    // Explicitly stop this test-only worker; production drains never abort work.
    if let JoinState::Running(handle) = &*worker.join.lock().await {
        handle.abort();
    }
    assert_eq!(tasks.join(&worker).await, TaskTermination::Cancelled);
    tasks.drain(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn session_drain_joins_only_selected_preparation_workers_and_resources() {
    let tasks = ExecutionTasks::default();
    let selected = SessionId::new();
    let other = SessionId::new();
    let selected_run = RunId::new();
    let other_run = RunId::new();
    tasks.spawn(selected.clone(), selected_run.clone(), async {});
    tasks.spawn_preparation(selected.clone(), async {});
    let release = Arc::new(Notify::new());
    let wait = release.clone();
    tasks.spawn(other.clone(), other_run.clone(), async move {
        wait.notified().await;
    });
    let other_preparation = tasks.spawn_preparation(other.clone(), std::future::pending());
    let other_resources =
        tasks.tool_resources(other.clone(), other_run.clone(), ToolCallId::new(), None);
    let resource_release = Arc::new(Notify::new());
    let wait = resource_release.clone();
    other_resources
        .resources()
        .spawn(async move {
            wait.notified().await;
            Ok(())
        })
        .unwrap();
    other_resources.close();
    let report = tokio::time::timeout(
        Duration::from_secs(1),
        tasks.drain_session(&selected, Duration::from_secs(10)),
    )
    .await
    .unwrap();
    assert_eq!(report.joined.len(), 1);
    assert_eq!(report.joined[0].session_id, selected);
    assert_eq!(report.joined[0].run_id, selected_run);
    assert_eq!(report.joined_preparations.len(), 1);
    assert_eq!(report.joined_preparations[0].session_id, selected);
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(report.pending_preparations, [] as [PreparationTaskId; 0]);
    assert_eq!(report.resources, [] as [ToolResourceDrain; 0]);
    let (pending, preparations, resources) = tasks.pending(Some(&other));
    assert_eq!(pending, vec![other_run]);
    assert_eq!(preparations.len(), 1);
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].session_id, other);
    assert_eq!(resources[0].resources.pending, 1);
    // Explicit test-only cleanup; the scoped drain never cancels unrelated work.
    if let JoinState::Running(handle) = &*other_preparation.join.lock().await {
        handle.abort();
    }
    release.notify_one();
    resource_release.notify_one();
    let report = tasks.drain(Duration::from_secs(1)).await;
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(report.pending_preparations, [] as [PreparationTaskId; 0]);
    assert!(
        report
            .resources
            .iter()
            .all(|scope| scope.resources.pending == 0)
    );
}

#[tokio::test]
async fn session_drain_reports_worker_handoff_during_resource_wait() {
    let tasks = ExecutionTasks::default();
    let session = SessionId::new();
    let resources = tasks.tool_resources(session.clone(), RunId::new(), ToolCallId::new(), None);
    let release = Arc::new(Notify::new());
    let wait = release.clone();
    resources
        .resources()
        .spawn(async move {
            wait.notified().await;
            Ok(())
        })
        .unwrap();
    resources.close();
    let mut draining = Box::pin(tasks.drain_session(&session, Duration::from_millis(10)));
    assert!(futures::poll!(draining.as_mut()).is_pending());
    let run = RunId::new();
    let wait = release.clone();
    tasks.spawn(session.clone(), run.clone(), async move {
        wait.notified().await;
    });
    let report = draining.await;
    assert_eq!(report.pending, vec![run]);
    assert_eq!(report.resources.len(), 1);
    assert_eq!(report.resources[0].session_id, session);
    release.notify_waiters();
    assert_eq!(
        tasks.drain(Duration::from_secs(1)).await.pending,
        [] as [RunId; 0]
    );
}

#[tokio::test(start_paused = true)]
async fn driver_cleanup_closes_every_scope_before_awaiting_cross_scope_joins() {
    let tasks = ExecutionTasks::default();
    let session = SessionId::new();
    let run = RunId::new();
    let a = tasks.tool_resources(session.clone(), run.clone(), ToolCallId::new(), None);
    let b = tasks.tool_resources(session, run.clone(), ToolCallId::new(), None);
    let wait_a = a.resources().closing().unwrap();
    let wait_b = b.resources().closing().unwrap();
    a.resources()
        .spawn(async move {
            wait_b.await;
            Ok(())
        })
        .unwrap();
    b.resources()
        .spawn(async move {
            wait_a.await;
            Ok(())
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), tasks.finish_run_resources(&run))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.snapshot().joined, 1);
    assert_eq!(b.snapshot().joined, 1);
}
