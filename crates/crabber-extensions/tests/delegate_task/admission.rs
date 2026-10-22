//! Synthetic hosts isolate the acknowledgement/retention transaction. Actual
//! runtime ownership remains covered by detached and attribution journeys.
use super::*;
use crabber::extension::{
    CleanupOwner, SessionInvocationAdmission as Admission, SessionInvocationContext,
    SessionInvocationError as Error, SessionInvocationRequest, SessionInvocationSpawner,
    SessionInvocationTask, SessionInvocations, ToolContext,
};
use crabber_extensions::delegate_task::{LIST_TOOL, START_TOOL, STATUS_TOOL, SharedCapacity};
use std::sync::Mutex;

fn invocation() -> SessionInvocationContext {
    let cleanup = CleanupOwner::new();
    SessionInvocationContext {
        cancel: CancellationToken::new(),
        cleanup: cleanup.tracker().clone(),
        public_source: Default::default(),
        invocations: Default::default(),
    }
}
fn next(mut context: ToolContext) -> ToolContext {
    context.call_id = ToolCallId::new();
    context
}
struct Live {
    registrations: AtomicUsize,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for Live {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        self.registrations.fetch_add(1, Ordering::SeqCst);
        self.workers.lock().unwrap().push(tokio::spawn(async move {
            task(invocation()).await.unwrap();
        }));
        Ok(Admission::Started)
    }
}

#[path = "admission/replay.rs"]
mod replay;
#[path = "admission/retention.rs"]
mod retention;
#[path = "admission/uncertainty.rs"]
mod uncertainty;

#[path = "admission/publication.rs"]
mod publication;
