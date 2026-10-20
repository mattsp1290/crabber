use super::*;
use crabber_extension::ToolExecutor;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Default)]
pub(super) struct Gate {
    pub(super) entered: Notify,
    pub(super) disposing: Notify,
    pub(super) release: (Mutex<bool>, Condvar),
    pub(super) panic: bool,
}
pub(super) struct Release(pub(super) Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
pub(super) struct Pending(pub(super) Arc<Gate>);
impl Future for Pending {
    type Output = Result<crabber_core::ToolOutput, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let mut released = self.0.release.0.lock().unwrap();
        while !*released {
            released = self.0.release.1.wait(released).unwrap();
        }
        assert!(!self.0.panic, "actual tool future disposal failed");
    }
}
struct Blocked(Arc<Gate>);
#[async_trait]
impl ToolExecutor for Blocked {
    async fn execute(&self, _: Value) -> Result<crabber_core::ToolOutput, ExtensionError> {
        Pending(self.0.clone()).await
    }
}
pub(super) async fn contract(store: Arc<dyn Store>) {
    for panic in [false, true] {
        run(store.clone(), panic).await;
    }
}
pub(super) async fn signal(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap();
}
async fn run(store: Arc<dyn Store>, panic: bool) {
    let gate = Arc::new(Gate {
        panic,
        ..Default::default()
    });
    let release = Release(gate.clone());
    let call = ToolCallId::new();
    let (runtime, registry) = runtime(
        store.clone(),
        vec![tool(Arc::new(Blocked(gate.clone())))],
        vec![call_script(call.clone(), r#"{"text":"blocked"}"#)],
    )
    .await;
    let handle = runtime.start(request()).await.unwrap();
    signal(&gate.entered).await;
    handle.interrupt();
    signal(&gate.disposing).await;
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(30))
        .await;
    let attempt = store
        .latest_execution_attempt(handle.run_id())
        .await
        .unwrap()
        .unwrap()
        .attempt;
    let settled = store
        .list_events(handle.session_id(), None, 1000)
        .await
        .unwrap()
        .iter()
        .any(|event| event.kind == EventKind::ToolCallSettled);
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    let closed = registry.close_all().await;
    assert!(
        pending.resources.iter().any(|scope| scope.call_id == call
            && scope.phase.is_some()
            && scope.resources.sealed
            && scope.resources.pending > 0),
        "{pending:?}"
    );
    assert_eq!(
        attempt.state,
        crabber_session::AttemptState::Started,
        "{attempt:?}"
    );
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    assert!(!settled);
    if panic {
        assert!(result.is_err());
        assert!(closed.is_err());
    } else {
        assert!(
            matches!(
                result,
                Err(RuntimeError::PhaseOutcomeUnknown {
                    reason: crabber_session::PhaseError::Uncertain,
                    ..
                })
            ),
            "{result:?}"
        );
        closed.unwrap();
    }
}
