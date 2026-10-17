//! Scope disposal only signals this supervisor; actual joins precede publication.
use super::{RetainedPreparations, SharedEntry};
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
    },
};
use std::sync::Arc;
#[path = "cleanup/failure.rs"]
pub(in crate::orchestrator::preparation) mod failure;
#[path = "cleanup/observation.rs"]
pub(in crate::orchestrator::preparation) mod observation;

pub(in crate::orchestrator::preparation) async fn start(
    registry: RetainedPreparations,
    entry: SharedEntry,
    store: Arc<dyn Store>,
) {
    let mut held = entry.lock().await;
    if held.cleanup_supervisor.is_running() {
        return;
    }
    if held.cleanup_supervisor.join().await.is_err() {
        return;
    }
    let Some(owned) = &held.owned else { return };
    let disposed = owned.disposed.clone();
    let retained = entry.clone();
    held.cleanup_supervisor = super::heartbeat::RetainedTask::running(tokio::spawn(async move {
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
        if super::heartbeat::stop(&mut current).await.is_err() {
            return;
        }
        registry.remove(&current.descriptor, &retained);
    }));
}

async fn joined_resources(entry: &SharedEntry, store: &dyn Store) -> Result<(), ()> {
    let mut current = entry.lock().await;
    let result = observation::observe(current.owned.as_mut().ok_or(())?).await;
    let proof = match result {
        Ok(proof) => proof,
        Err(Some(failure)) => {
            let _ = failure::publish(&current, store, failure).await;
            return Err(());
        }
        Err(None) => return Err(()),
    };
    // The renewal/cancellation observer is retained service work too. Stop and
    // observe its actual join before publishing the resource owner's Join.
    super::heartbeat::stop(&mut current).await?;
    publish_join(&current.grant, store, proof).await
}

pub(in crate::orchestrator::preparation) async fn publish_join(
    grant: &crabber_session::session_owner::preparation::PreparationGrant,
    store: &dyn Store,
    proof: crabber_session::session_owner::owned_scope::JoinedOwnedScope,
) -> Result<(), ()> {
    let request = proof.request().map_err(|_| ())?;
    let digest = proof.fingerprint().map_err(|_| ())?;
    let binding =
        crabber_session::session_owner::preparation::PreparationResourceLink::reservation(grant)
            .map_err(|_| ())?
            .binding;
    // Reconcile the original receipt before any current-state check. A lost or
    // malformed ACK cannot turn successful physical cleanup into uncertainty.
    if let Some(receipt) = store
        .lookup_session_owner(&request.session_id, &request.id)
        .await
        .map_err(|_| ())?
    {
        return valid_join(
            SessionOwnerOutcome::Replayed(receipt),
            &request,
            &digest,
            &binding,
        )
        .then_some(())
        .ok_or(());
    }
    let authority = grant.retained_resources_authority().cloned().ok_or(())?;
    let outcome = store
        .join_owned_scope(proof, authority)
        .await
        .map_err(|_| ())?;
    valid_join(outcome, &request, &digest, &binding)
        .then_some(())
        .ok_or(())
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
