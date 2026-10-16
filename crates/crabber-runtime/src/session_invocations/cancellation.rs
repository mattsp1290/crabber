//! Observe only after the actual retained worker has signalled its local token.
use super::Invocation;
use crabber_core::SessionOwnerOperationId;
use crabber_session::{
    Store,
    session_owner::{
        ObserveSessionOwnerCancel, SessionOwnerAuthority, SessionOwnerError, SessionOwnerOperation,
        SessionOwnerOutcome, SessionOwnerReceipt,
    },
};

pub(super) async fn poll(
    store: &dyn Store,
    invocation: &Invocation,
    authority: &SessionOwnerAuthority,
) {
    if !store.supports_session_owner_cancellation() {
        return;
    }
    let intent = match store.pending_session_owner_cancel(authority).await {
        Ok(Some(intent)) => intent,
        Ok(None) => return,
        Err(_) => {
            invocation.cancel.cancel();
            return;
        }
    };
    invocation.cancel.cancel();
    if intent.observed.is_some() {
        return;
    }
    let request = {
        let mut retained = invocation.cancel_observation.lock().unwrap();
        if retained
            .as_ref()
            .is_some_and(|request| request.request_id != intent.request_id)
        {
            return;
        }
        retained
            .get_or_insert_with(|| ObserveSessionOwnerCancel {
                id: SessionOwnerOperationId::new(),
                session_id: authority.session_id().clone(),
                target: authority.target().clone(),
                request_id: intent.request_id,
            })
            .clone()
    };
    let Ok(digest) = request.fingerprint() else {
        return;
    };
    // Unknown ACK retries the same signal acknowledgement, never the task factory.
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
            ) => Some(receipt),
            Err(SessionOwnerError::UnknownStoreOutcome) => None,
            _ => return,
        };
        if receipt
            .as_ref()
            .is_some_and(|receipt| valid(receipt, &request, &digest))
        {
            return;
        }
    }
}
fn valid(
    receipt: &SessionOwnerReceipt,
    request: &ObserveSessionOwnerCancel,
    digest: &crabber_core::InputFingerprint,
) -> bool {
    receipt.session_id == request.session_id
        && receipt.operation_id == request.id
        && receipt.semantic_digest == *digest
        && receipt.operation == SessionOwnerOperation::ObserveCancel
        && receipt.owner.binding.session_id == request.session_id
        && receipt.owner.target == request.target
        && receipt.owner.cancellation.as_ref().is_some_and(|intent| {
            intent.request_id == request.request_id
                && intent
                    .observed
                    .as_ref()
                    .is_some_and(|observed| observed.operation_id == request.id)
        })
}
