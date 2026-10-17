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

/// Stop before starting another poll; an already started Store call must finish.
pub(in crate::orchestrator) async fn supervise(
    store: &dyn Store,
    authority: &SessionOwnerAuthority,
    cancellation: &CancellationToken,
    stop: &CancellationToken,
) -> Result<(), crabber_session::session_owner::SessionOwnerError> {
    use std::time::Duration;
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut observed = None;
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            _ = ticker.tick() => (),
        }
        // A delayed original poll can leave a tick ready alongside stop.
        // Do not start another Store call after completing that original one.
        if stop.is_cancelled() {
            return Ok(());
        }
        super::store_call(async {
            poll(store, authority, cancellation, &mut observed).await;
            Ok(())
        })
        .await?;
    }
}
