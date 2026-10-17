//! Cancel only a root whose original native-completion main never existed.
use super::super::PendingPreparation;
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerOperation, SessionOwnerOutcome,
        SessionOwnerState, SessionOwnerTermination,
    },
};

pub(in crate::orchestrator::preparation::pending) async fn cancel_unstarted_root(
    pending: &PendingPreparation,
    store: &dyn Store,
) -> Result<(), ()> {
    // Native_done here acknowledges the closed local pre-main stage, not an
    // inferred native outcome. Existing/unknown Complete calls never use this.
    if pending.main.get().is_some()
        || !pending.native_done.is_cancelled()
        || pending.committed.load(std::sync::atomic::Ordering::Acquire)
    {
        return Err(());
    }
    let grant = &pending.grant;
    let descriptor = grant.reconciliation().map_err(|_| ())?;
    let request = MutateSessionOwner {
        id: descriptor.join_id,
        session_id: descriptor.session_id,
        target: grant.owner_authority().target().clone(),
        command: SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Cancelled,
        },
    };
    let digest = request.fingerprint().map_err(|_| ())?;
    let outcome = if let Some(receipt) = store
        .lookup_session_owner(&request.session_id, &request.id)
        .await
        .map_err(|_| ())?
    {
        SessionOwnerOutcome::Replayed(receipt)
    } else {
        store
            .mutate_session_owner(request.clone(), Some(grant.owner_authority().clone()))
            .await
            .map_err(|_| ())?
    };
    let (SessionOwnerOutcome::Applied {
        receipt,
        authority: None,
    }
    | SessionOwnerOutcome::Replayed(receipt)) = outcome
    else {
        return Err(());
    };
    (receipt.session_id == request.session_id
        && receipt.operation_id == request.id
        && receipt.operation == SessionOwnerOperation::Join
        && receipt.semantic_digest == digest
        && receipt.owner.target == request.target
        && receipt.owner.binding == grant.original_reservation().owner.binding
        && receipt.owner.state == SessionOwnerState::Joined
        && receipt.owner.termination == Some(SessionOwnerTermination::Cancelled))
    .then_some(())
    .ok_or(())
}
