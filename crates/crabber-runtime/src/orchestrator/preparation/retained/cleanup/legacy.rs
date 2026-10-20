//! A genuinely joined legacy business error can cancel only its original root.
use super::{RetainedPreparations, SharedEntry, valid_join};
use crate::RuntimeError;
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
    },
};
use std::sync::Arc;

pub(in crate::orchestrator::preparation) async fn cancel_failed_legacy(
    registry: RetainedPreparations,
    entry: SharedEntry,
    store: Arc<dyn Store>,
) -> Result<(), RuntimeError> {
    let mut held = entry.lock().await;
    if held.legacy_failure.is_none()
        || held
            .grant
            .original_reservation()
            .resource_contract
            .is_some()
        || held.grant.retained_resources_authority().is_some()
        || held.joined.is_some()
        || held.value.is_some()
        || held.owned.is_some()
        || held.pending.is_some()
        || held.committed
    {
        return Err(RuntimeError::PreparationRecoveryRequired);
    }
    held.disposing = true;
    // The marker proves the actual factory task already joined with a business
    // error. Its SAME renewal observer must join before the native root retires.
    if let Err(error) = super::super::heartbeat::stop(&mut held).await {
        drop(held);
        super::super::heartbeat::discard_join_error(error);
        return Err(RuntimeError::TaskStopped);
    }
    let request = MutateSessionOwner {
        id: held.descriptor.join_id.clone(),
        session_id: held.descriptor.session_id.clone(),
        target: held.grant.owner_authority().target().clone(),
        command: SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Cancelled,
        },
    };
    let digest = request
        .fingerprint()
        .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
    // A lost ACK retains this original marker, authority and operation. Retry
    // observes its original receipt before any current-state authority check.
    let outcome = if let Some(receipt) = store
        .lookup_session_owner(&request.session_id, &request.id)
        .await
        .map_err(|_| RuntimeError::PreparationRecoveryRequired)?
    {
        SessionOwnerOutcome::Replayed(receipt)
    } else {
        store
            .mutate_session_owner(request.clone(), Some(held.grant.owner_authority().clone()))
            .await
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?
    };
    if !valid_join(
        outcome,
        &request,
        &digest,
        &held.grant.original_reservation().owner.binding,
    ) {
        return Err(RuntimeError::PreparationRecoveryRequired);
    }
    if let Some(root) = &held.root_scope {
        root.resolve_preparation(&held.descriptor);
    }
    registry.remove(&held.descriptor, &entry);
    Ok(())
}
