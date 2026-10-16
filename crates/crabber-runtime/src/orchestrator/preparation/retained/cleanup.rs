//! Scope disposal only signals this supervisor; actual joins precede publication.
use super::{RetainedPreparations, SharedEntry};
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
        owned_scope::{OwnedCleanupFailure, OwnedCleanupTask},
    },
};
use std::{sync::Arc, time::Duration};

pub(in crate::orchestrator::preparation) async fn start(
    registry: RetainedPreparations,
    entry: SharedEntry,
    store: Arc<dyn Store>,
) {
    let mut held = entry.lock().await;
    if let Some(supervisor) = &held.cleanup_supervisor {
        if !supervisor.is_finished() {
            return;
        }
        if let Some(supervisor) = held.cleanup_supervisor.take() {
            let _ = supervisor.await;
        }
    }
    let Some(owned) = &held.owned else { return };
    let disposed = owned.disposed.clone();
    let retained = entry.clone();
    held.cleanup_supervisor = Some(tokio::spawn(async move {
        disposed.cancelled().await;
        if joined_resources(&retained, store.as_ref()).await.is_err() {
            return;
        }
        let mut current = retained.lock().await;
        if !current.committed
            && let Some(original) = &current.joined
            && let Ok(reconciled) = store.complete_preparation(original.clone()).await
            && reconciled.is_replayed()
        {
            // The declared child has already Joined. Its fresh native guard
            // cannot pass; only the original native metadata replay can succeed.
            // Preserve that historical source rather than relabeling it cleanup.
            current.committed = true;
        }
        if !current.committed {
            let cleanup = MutateSessionOwner {
                id: current.descriptor.join_id.clone(),
                session_id: current.descriptor.session_id.clone(),
                target: current.grant.owner_authority().target().clone(),
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Cancelled,
                },
            };
            // A native commit with an unknown ACK has another source-bound
            // Join digest. Its conflict remains explicit until native replay.
            let Ok(digest) = cleanup.fingerprint() else {
                return;
            };
            let outcome = store
                .mutate_session_owner(
                    cleanup.clone(),
                    Some(current.grant.owner_authority().clone()),
                )
                .await;
            if !outcome.is_ok_and(|outcome| {
                valid_join(
                    outcome,
                    &cleanup,
                    &digest,
                    &current.grant.original_reservation().owner.binding,
                )
            }) {
                return;
            }
        }
        current.heartbeat_stop.cancel();
        if let Some(heartbeat) = current.heartbeat.take() {
            let _ = heartbeat.await;
        }
        registry.remove(&current.descriptor, &retained);
    }));
}

async fn joined_resources(entry: &SharedEntry, store: &dyn Store) -> Result<(), ()> {
    let mut current = entry.lock().await;
    let authority = current
        .grant
        .retained_resources_authority()
        .cloned()
        .ok_or(())?;
    let owned = current.owned.as_mut().ok_or(())?;
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
    let observed = owned.task.join().await.map_err(|_| ())?;
    let proof = observed
        .observe_resources(owned.cleanup.as_mut().ok_or(())?)
        .await
        .map_err(|_| ())?;
    // The renewal/cancellation observer is retained service work too. Stop and
    // observe its actual join before publishing the resource owner's Join.
    current.heartbeat_stop.cancel();
    if let Some(heartbeat) = current.heartbeat.take() {
        heartbeat.await.map_err(|_| ())?;
    }
    // Unknown Join ACK leaves the opaque proof and actual joins retained; an
    // explicit later cleanup call retries this exact source, never work.
    let request = proof.request().map_err(|_| ())?;
    let digest = proof.fingerprint().map_err(|_| ())?;
    let binding =
        crabber_session::session_owner::preparation::PreparationResourceLink::reservation(
            &current.grant,
        )
        .map_err(|_| ())?
        .binding;
    let outcome = store
        .join_owned_scope(proof, authority)
        .await
        .map_err(|_| ())?;
    if !valid_join(outcome, &request, &digest, &binding) {
        return Err(());
    }
    Ok(())
}

fn valid_join(
    outcome: SessionOwnerOutcome,
    request: &MutateSessionOwner,
    digest: &crabber_core::InputFingerprint,
    binding: &crabber_session::session_owner::SessionOwnerBinding,
) -> bool {
    let (SessionOwnerOutcome::Applied {
        receipt,
        authority: None,
    }
    | SessionOwnerOutcome::Replayed(receipt)) = outcome
    else {
        return false;
    };
    let SessionOwnerCommand::Join { termination } = request.command else {
        return false;
    };
    receipt.session_id == request.session_id
        && receipt.operation_id == request.id
        && receipt.operation == crabber_session::session_owner::SessionOwnerOperation::Join
        && receipt.semantic_digest == *digest
        && receipt.owner.target == request.target
        && receipt.owner.binding == *binding
        && receipt.owner.state == crabber_session::session_owner::SessionOwnerState::Joined
        && receipt.owner.termination == Some(termination)
}
