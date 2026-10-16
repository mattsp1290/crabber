//! Renewal failures are observations only, never resource termination evidence.
use crabber_session::{
    Store,
    session_owner::{MutateSessionOwner, SessionOwnerAuthority, SessionOwnerCommand},
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
#[path = "heartbeat/cancellation.rs"]
mod cancellation;

pub(in crate::orchestrator::preparation) fn start(
    store: Arc<dyn Store>,
    authorities: Vec<SessionOwnerAuthority>,
    stop: CancellationToken,
    resources: Option<crabber_extension::ToolResourceOwner>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut observed = vec![None; authorities.len()];
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                _ = interval.tick() => {
                    for (authority, observation) in authorities.iter().zip(&mut observed) {
                        if stop.is_cancelled() { break; }
                        if let Some(resources) = &resources {
                            cancellation::poll(store.as_ref(), authority, resources, observation, &stop).await;
                        }
                        if stop.is_cancelled() { break; }
                        let renewal = MutateSessionOwner {
                            id: crabber_core::SessionOwnerOperationId::new(),
                            session_id: authority.session_id().clone(),
                            target: authority.target().clone(),
                            command: SessionOwnerCommand::Renew { lease_ms: 30_000 },
                        };
                        // A committed root Join legitimately refuses renewals;
                        // independent retained child resources still need theirs.
                        let _ = store.mutate_session_owner(renewal, Some(authority.clone())).await;
                    }
                }
            }
        }
    })
}

/// Actual task observation stays in its owner across cancellation of a waiter.
#[derive(Default)]
pub(in crate::orchestrator::preparation) enum RetainedTask {
    #[default]
    Absent,
    Running(tokio::task::JoinHandle<()>),
    Joined,
    Failed,
}
impl RetainedTask {
    pub(in crate::orchestrator::preparation) fn running(task: tokio::task::JoinHandle<()>) -> Self {
        Self::Running(task)
    }
    pub(in crate::orchestrator::preparation) fn is_running(&self) -> bool {
        matches!(self, Self::Running(task) if !task.is_finished())
    }
    pub(in crate::orchestrator::preparation) fn is_finished(&self) -> bool {
        match self {
            Self::Running(task) => task.is_finished(),
            Self::Joined | Self::Failed => true,
            Self::Absent => false,
        }
    }
    pub(in crate::orchestrator::preparation) fn failed(&self) -> bool {
        matches!(self, Self::Failed)
    }
    pub(in crate::orchestrator::preparation) async fn join(&mut self) -> Result<(), ()> {
        if self.failed() {
            return Err(());
        }
        if let Self::Running(handle) = self {
            // Await by reference: dropping this waiter cannot detach the task.
            let result = handle.await;
            *self = if result.is_ok() {
                Self::Joined
            } else {
                Self::Failed
            };
            if let Err(error) = result {
                // Persist failure before disposing a potentially hostile payload.
                if let Err(nested) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(error)))
                {
                    std::mem::forget(nested);
                }
                return Err(());
            }
        }
        Ok(())
    }
}
pub(in crate::orchestrator::preparation) async fn stop(
    current: &mut super::Entry,
) -> Result<(), ()> {
    current.heartbeat_stop.cancel();
    current.heartbeat.join().await
}
