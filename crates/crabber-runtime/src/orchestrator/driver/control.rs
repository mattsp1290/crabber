//! The cooperative signal precedes its exact native cancellation observation.
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, ObserveSessionOwnerCancel, SessionOwnerAuthority, SessionOwnerCommand,
        SessionOwnerOperation,
    },
};
use tokio_util::sync::CancellationToken;
pub(super) async fn poll(
    store: &dyn Store,
    authority: &SessionOwnerAuthority,
    cancellation: &CancellationToken,
    retained: &mut Option<ObserveSessionOwnerCancel>,
) {
    if let Ok(Some(intent)) = store.pending_session_owner_cancel(authority).await {
        cancellation.cancel();
        let request = retained.get_or_insert_with(|| ObserveSessionOwnerCancel {
            id: crabber_core::SessionOwnerOperationId::new(),
            session_id: authority.session_id().clone(),
            target: authority.target().clone(),
            request_id: intent.request_id.clone(),
        });
        if request.request_id == intent.request_id
            && let Ok(outcome) = store
                .observe_session_owner_cancel(request.clone(), authority.clone())
                .await
        {
            let (crabber_session::session_owner::SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            }
            | crabber_session::session_owner::SessionOwnerOutcome::Replayed(receipt)) = outcome
            else {
                return;
            };
            let Ok(digest) = request.fingerprint() else {
                return;
            };
            // Invalid acknowledgements never establish an observation. The
            // real signal stays applied and the original request is retained.
            if receipt.operation != SessionOwnerOperation::ObserveCancel
                || receipt.operation_id != request.id
                || receipt.semantic_digest != digest
                || receipt.session_id != request.session_id
                || receipt.owner.target != request.target
                || !receipt.owner.cancellation.as_ref().is_some_and(|cancel| {
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
    let renewal = MutateSessionOwner {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: authority.session_id().clone(),
        target: authority.target().clone(),
        command: SessionOwnerCommand::Renew { lease_ms: 30000 },
    };
    let _ = store
        .mutate_session_owner(renewal, Some(authority.clone()))
        .await;
}
