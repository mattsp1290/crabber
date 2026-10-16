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
                        if let Some(resources) = &resources {
                            cancellation::poll(store.as_ref(), authority, resources, observation).await;
                        }
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
