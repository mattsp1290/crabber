//! A contained Notify error must not erase actual callback disposal failure.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Condvar,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default)]
struct Disposal {
    entered: Notify,
    release: (Mutex<bool>, Condvar),
    dropped: AtomicUsize,
}
struct Release(Arc<Disposal>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
struct AbnormalFuture {
    disposal: Arc<Disposal>,
    value: Option<Value>,
}
impl Future for AbnormalFuture {
    type Output = Result<Value, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self.value.take().unwrap()))
    }
}
impl Drop for AbnormalFuture {
    fn drop(&mut self) {
        self.disposal.entered.notify_one();
        let (lock, changed) = &self.disposal.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        self.disposal.dropped.fetch_add(1, Ordering::SeqCst);
        panic!("actual notification future disposal failed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_notification_keeps_actual_join_and_propagates_disposal_failure() {
    let call = ScriptedCall::text(ECHO, "secret");
    let harness = Harness::builder(one_turn(&[&call])).build().await;
    let disposal = Arc::new(Disposal::default());
    let release = Release(disposal.clone());
    let captured = disposal.clone();
    let notifier = ClosureExtension::new("owned-abnormal-notification", move |r| {
        let captured = captured.clone();
        r.on_notify(
            EventPublished::ID,
            0,
            "abnormal",
            Arc::new(move |value| {
                if value["kind"] == "tool_call_settled" {
                    Box::pin(AbnormalFuture {
                        disposal: captured.clone(),
                        value: Some(value),
                    })
                } else {
                    Box::pin(async move { Ok(value) })
                }
            }),
        );
    });
    harness
        .registry
        .mount_owned(notifier, Scope::Global)
        .await
        .unwrap();
    let gate = harness.block_results();
    let handle = harness.start().await;
    gate.entered().await;
    let (done, mut completion, worker) = interrupt_observation(&harness, handle);
    tokio::pin!(worker);
    wait_outcome(&mut completion).await;
    timeout(Duration::from_secs(2), disposal.entered.notified())
        .await
        .unwrap();
    let blocked = harness
        .runtime()
        .drain_session_execution_tasks(&done.session_id, Duration::from_millis(25))
        .await;
    assert!(blocked.background_tasks.iter().any(|scope| scope.kind
        == crate::WorkerResourceKind::InterruptedSettlementNotification
        && scope.resources.pending > 0));
    assert_eq!(disposal.dropped.load(Ordering::SeqCst), 0);
    drop(release);
    let result = timeout(Duration::from_secs(3), &mut worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_interrupted(&done, &call.id, INTERRUPTED_RESULT_TEXT).await;
    assert_eq!(disposal.dropped.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        let report = harness
            .runtime()
            .drain_session_execution_tasks(&done.session_id, Duration::from_secs(1))
            .await;
        assert!(
            report.background_tasks.iter().any(|scope| scope.kind
                == crate::WorkerResourceKind::InterruptedSettlementNotification
                && scope.resources.pending == 0
                && !scope.resources.failures.is_empty()),
            "contained Notify error erased actual disposal failure: {report:?}"
        );
    }
}
