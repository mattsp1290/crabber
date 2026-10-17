//! Physical observations survive retries, including failed cleanup results.
use super::super::OwnedEntry;
use crabber_session::session_owner::owned_scope::{
    JoinSource, JoinedOwnedScope, OwnedCleanupFailure, OwnedCleanupTask,
};
use std::time::Duration;

pub(in crate::orchestrator::preparation) async fn observe(
    owned: &mut OwnedEntry,
) -> Result<JoinedOwnedScope, Option<OwnedCleanupFailure>> {
    if let Some(proof) = &owned.joined {
        return Ok(proof.clone());
    }
    if let Some(failure) = owned.cleanup_failure {
        return Err(Some(failure));
    }
    if owned.cleanup.is_none() {
        let resources = owned.resources.clone();
        owned.cleanup = Some(OwnedCleanupTask::from_host_task(
            owned.task.activation().clone(),
            tokio::spawn(async move {
                loop {
                    let report = resources.drain(Duration::from_secs(5)).await;
                    if !report.failures.is_empty() {
                        return Err(OwnedCleanupFailure::Failed);
                    }
                    if report.pending == 0 {
                        return Ok(());
                    }
                }
            }),
        ));
    }
    let observed = owned.task.join().await.map_err(|_| None)?;
    owned.cleanup_source = Some(JoinSource {
        activation: observed.activation().clone(),
        disposition: observed.disposition(),
    });
    let cleanup = owned.cleanup.as_mut().ok_or(None)?;
    if let Ok(proof) = observed.observe_resources(cleanup).await {
        owned.joined = Some(proof.clone());
        Ok(proof)
    } else {
        // Only an actual observed task failure may publish uncertainty.
        // A source mismatch or an unobserved task remains incomplete.
        owned.cleanup_failure = cleanup.observed_failure();
        Err(owned.cleanup_failure)
    }
}
