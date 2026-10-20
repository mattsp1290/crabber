//! Acknowledgement follows the actual cooperative signal, never task completion.
use crabber_extension::ToolResourceOwner;
use crabber_session::{
    Store,
    session_owner::{
        ObserveSessionOwnerCancel, SessionOwnerAuthority, SessionOwnerError, SessionOwnerOperation,
        SessionOwnerOutcome,
    },
};

pub(super) async fn poll(
    store: &dyn Store,
    authority: &SessionOwnerAuthority,
    resources: &ToolResourceOwner,
    retained: &mut Option<ObserveSessionOwnerCancel>,
    stop: &tokio_util::sync::CancellationToken,
) {
    if stop.is_cancelled() || !store.supports_session_owner_cancellation() {
        return;
    }
    let Ok(Some(intent)) = store.pending_session_owner_cancel(authority).await else {
        return;
    };
    // This seals registration and applies the real cooperative signal. It does
    // not abort a non-cooperative factory, release its output or claim Join.
    resources.close();
    let request = retained.get_or_insert_with(|| ObserveSessionOwnerCancel {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: authority.session_id().clone(),
        target: authority.target().clone(),
        request_id: intent.request_id.clone(),
    });
    if request.request_id != intent.request_id {
        return;
    }
    let Ok(digest) = request.fingerprint() else {
        return;
    };
    for _ in 0..2 {
        if stop.is_cancelled() {
            return;
        }
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
        if receipt.operation == SessionOwnerOperation::ObserveCancel
            && receipt.operation_id == request.id
            && receipt.session_id == request.session_id
            && receipt.owner.target == request.target
            && receipt.semantic_digest == digest
            && receipt.owner.cancellation.as_ref().is_some_and(|cancel| {
                cancel.request_id == request.request_id
                    && cancel
                        .observed
                        .as_ref()
                        .is_some_and(|observed| observed.operation_id == request.id)
            })
        {
            return;
        }
    }
}
