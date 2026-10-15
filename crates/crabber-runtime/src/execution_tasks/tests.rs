use super::*;
use tokio::sync::{Notify, oneshot};

#[tokio::test]
async fn cancelled_drain_retains_join_handle_and_concurrent_joiners_reconcile() {
    let tasks = ExecutionTasks::default();
    let run = RunId::new();
    let release = Arc::new(Notify::new());
    let released = release.clone();
    let task = tasks.spawn(run.clone(), async move { released.notified().await });
    let report = tasks.drain(Duration::from_millis(1)).await;
    assert_eq!(report.joined, [] as [crate::JoinedExecutionTask; 0]);
    assert_eq!(report.pending, vec![run.clone()]);
    release.notify_one();
    let (left, right) = tokio::join!(tasks.join(&task), tasks.join(&task));
    assert_eq!(left, TaskTermination::Returned);
    assert_eq!(right, left);
    assert_eq!(tasks.pending().0, [] as [RunId; 0]);
    assert_eq!(tasks.join(&task).await, left);
}

#[tokio::test]
async fn dropped_waiter_does_not_cancel_work_or_relabel_return_as_join() {
    let tasks = ExecutionTasks::default();
    let (sender, receiver) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let released = release.clone();
    let run = RunId::new();
    let task = tasks.spawn(run.clone(), async move {
        let _ = sender.send(());
        released.notified().await;
    });
    receiver.await.unwrap();
    // A result/notification can arrive while the actual task still owns work.
    assert_eq!(tasks.pending().0, vec![run]);
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
    tasks.spawn(run.clone(), async { panic!("private callback panic") });
    tasks.spawn(run, async {});
    assert_eq!(tasks.pending().0.len(), 2);
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
    let task = tasks.spawn(RunId::new(), async { std::panic::panic_any(Payload) });
    assert_eq!(tasks.join(&task).await, TaskTermination::Panicked);
    assert_eq!(tasks.join(&task).await, TaskTermination::Panicked);
    assert_eq!(tasks.pending().0, [] as [RunId; 0]);
}

#[tokio::test]
async fn resource_drain_cannot_hide_a_worker_started_during_its_wait() {
    let tasks = ExecutionTasks::default();
    let scope = tasks.tool_resources(RunId::new(), ToolCallId::new(), None);
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
    let worker = tasks.spawn(new_run.clone(), async {
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
