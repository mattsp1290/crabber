//! Signal children while main runs; final cleanup follows the actual main join.
use crate::execution_tasks::WorkerResources;
use crabber_session::session_owner::owned_scope::OwnedCleanupFailure;
use std::time::Duration;
use tokio::sync::oneshot;

pub(in crate::orchestrator) async fn finish(
    resources: WorkerResources,
    mut main: oneshot::Receiver<()>,
) -> Result<(), OwnedCleanupFailure> {
    let mut failed = false;
    loop {
        tokio::select! {
            joined = &mut main => {
                joined.map_err(|_| OwnedCleanupFailure::Failed)?;
                break;
            }
            closed = async {
                let result = resources.finish().await;
                // A still-running main can register another scope after an
                // empty drain. Keep this actual cleanup task alive, and close
                // later scopes without spinning or waiting for main first.
                tokio::time::sleep(Duration::from_millis(10)).await;
                result
            } => failed |= closed.is_err(),
        }
    }
    // No main result, early empty drain, cancellation, or barrier substitutes
    // for this final actual drain after the independently observed main join.
    let final_drain = resources.finish().await;
    if failed || final_drain.is_err() {
        Err(OwnedCleanupFailure::Failed)
    } else {
        Ok(())
    }
}
