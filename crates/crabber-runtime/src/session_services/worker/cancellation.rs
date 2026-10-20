//! Local signalling precedes the original durable cancellation observation.
use super::{Entry, SessionOwnerError};
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, ObserveSessionOwnerCancel, SessionOwnerAuthority, SessionOwnerCommand,
        SessionOwnerOperation, SessionOwnerOutcome,
    },
};

pub(super) async fn renew(store: &dyn Store, entry: &Entry, authority: &SessionOwnerAuthority) {
    poll(store, entry, authority).await;
    let request = MutateSessionOwner {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: authority.session_id().clone(),
        target: authority.target().clone(),
        command: SessionOwnerCommand::Renew { lease_ms: 30_000 },
    };
    if store
        .mutate_session_owner(request, Some(authority.clone()))
        .await
        .is_err()
    {
        entry.cancel.cancel();
        entry.resources.close();
    }
}
async fn poll(store: &dyn Store, entry: &Entry, authority: &SessionOwnerAuthority) {
    let intent = match store.pending_session_owner_cancel(authority).await {
        Ok(Some(intent)) => intent,
        Ok(None) => return,
        Err(_) => {
            entry.cancel.cancel();
            entry.resources.close();
            return;
        }
    };
    entry.cancel.cancel();
    entry.resources.close();
    if intent.observed.is_some() {
        return;
    }
    let request = {
        let mut original = entry.cancel_observation.lock().unwrap();
        if original
            .as_ref()
            .is_some_and(|request| request.request_id != intent.request_id)
        {
            return;
        }
        original
            .get_or_insert_with(|| ObserveSessionOwnerCancel {
                id: crabber_core::SessionOwnerOperationId::new(),
                session_id: authority.session_id().clone(),
                target: authority.target().clone(),
                request_id: intent.request_id,
            })
            .clone()
    };
    let Ok(digest) = request.fingerprint() else {
        return;
    };
    for _ in 0..2 {
        let receipt = match store
            .observe_session_owner_cancel(request.clone(), authority.clone())
            .await
        {
            Ok(
                SessionOwnerOutcome::Applied {
                    receipt,
                    authority: None,
                }
                | SessionOwnerOutcome::Replayed(receipt),
            ) => receipt,
            Err(SessionOwnerError::UnknownStoreOutcome) => continue,
            _ => return,
        };
        if receipt.session_id == request.session_id
            && receipt.operation_id == request.id
            && receipt.semantic_digest == digest
            && receipt.owner.target == request.target
            && receipt.operation == SessionOwnerOperation::ObserveCancel
            && receipt.owner.cancellation.as_ref().is_some_and(|intent| {
                intent.request_id == request.request_id
                    && intent
                        .observed
                        .as_ref()
                        .is_some_and(|observed| observed.operation_id == request.id)
            })
        {
            return;
        }
    }
}
