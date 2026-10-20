//! Actual cleanup failure is durable uncertainty, never inferred physical Join.
use super::super::Entry;
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerOperation, SessionOwnerOutcome,
        SessionOwnerReceipt, SessionOwnerResolver, SessionOwnerState,
        owned_scope::OwnedCleanupFailure,
    },
};

pub(super) async fn publish(
    current: &Entry,
    store: &dyn Store,
    failure: OwnedCleanupFailure,
) -> Result<(), ()> {
    let owned = current.owned.as_ref().ok_or(())?;
    publish_owned(&current.grant, owned, store, failure).await
}

pub(in crate::orchestrator::preparation) async fn publish_owned(
    grant: &crabber_session::session_owner::preparation::PreparationGrant,
    owned: &super::super::OwnedEntry,
    store: &dyn Store,
    failure: OwnedCleanupFailure,
) -> Result<(), ()> {
    if owned.cleanup_failure != Some(failure) {
        return Err(());
    }
    let source = owned.cleanup_source.as_ref().ok_or(())?;
    let join = source.request().map_err(|_| ())?;
    // Even when local evidence failed, an acknowledged or unknown original
    // Join must be reconciled first. Never overwrite any committed Join.
    if store
        .lookup_session_owner(&join.session_id, &join.id)
        .await
        .map_err(|_| ())?
        .is_some()
    {
        return Err(());
    }
    let request = request(&source.activation, failure)?;
    let digest = request.fingerprint().map_err(|_| ())?;
    let binding =
        crabber_session::session_owner::preparation::PreparationResourceLink::reservation(grant)
            .map_err(|_| ())?
            .binding;
    if let Some(receipt) = store
        .lookup_session_owner(&request.session_id, &request.id)
        .await
        .map_err(|_| ())?
    {
        return valid(&receipt, &request, &digest, &binding)
            .then_some(())
            .ok_or(());
    }
    let snapshot = store
        .get_session_owner(&request.session_id, &request.target.id)
        .await
        .map_err(|_| ())?;
    if snapshot.binding != binding
        || snapshot.target != request.target
        || !matches!(
            snapshot.state,
            SessionOwnerState::Active | SessionOwnerState::Uncertain
        )
    {
        return Err(());
    }
    let authority = grant.retained_resources_authority().cloned().ok_or(())?;
    let outcome = store
        .mutate_session_owner(request.clone(), Some(authority))
        .await
        .map_err(|_| ())?;
    let (SessionOwnerOutcome::Applied {
        receipt,
        authority: None,
    }
    | SessionOwnerOutcome::Replayed(receipt)) = outcome
    else {
        return Err(());
    };
    valid(&receipt, &request, &digest, &binding)
        .then_some(())
        .ok_or(())
}

fn request(
    activation: &crabber_session::session_owner::owned_scope::OwnedScopeActivation,
    failure: OwnedCleanupFailure,
) -> Result<MutateSessionOwner, ()> {
    let identity = activation.fingerprint().map_err(|_| ())?;
    Ok(MutateSessionOwner {
        id: crabber_core::SessionOwnerOperationId(format!(
            "cleanup-uncertain-v1-{}",
            identity.as_str()
        )),
        session_id: activation.session_id().clone(),
        target: activation.target().clone(),
        command: SessionOwnerCommand::MarkUncertain {
            claim: SessionOwnerResolver {
                resolver_id: "crabber.runtime.preparation.cleanup".into(),
                authority_claim: format!("observed retained resource cleanup failure: {failure:?}"),
            },
        },
    })
}

fn valid(
    receipt: &SessionOwnerReceipt,
    request: &MutateSessionOwner,
    digest: &crabber_core::InputFingerprint,
    binding: &crabber_session::session_owner::SessionOwnerBinding,
) -> bool {
    receipt.session_id == request.session_id
        && receipt.operation_id == request.id
        && receipt.operation == SessionOwnerOperation::MarkUncertain
        && receipt.semantic_digest == *digest
        && receipt.owner.binding == *binding
        && receipt.owner.target == request.target
        && receipt.owner.state == SessionOwnerState::Uncertain
        && receipt.owner.termination.is_none()
}
