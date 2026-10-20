//! Native Join follows both the physical main and exact independent cleanup join.
use super::{Entry, Status, cancellation};
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerAuthority, SessionOwnerError, SessionOwnerOperation, SessionOwnerOutcome,
        SessionOwnerState,
        owned_scope::{OwnedCleanupFailure, OwnedCleanupTask, OwnedMainDisposition},
    },
};
use std::time::Duration;

pub(super) async fn complete(
    store: &dyn Store,
    entry: &Entry,
    authority: &SessionOwnerAuthority,
    scope: &mut super::super::entry::Scope,
    ticker: &mut tokio::time::Interval,
) {
    let Ok(main) = scope.task.join().await else {
        entry.set_status(Status::Uncertain);
        return;
    };
    let successful = main.disposition() == OwnedMainDisposition::NoDispatch
        || (main.disposition() == OwnedMainDisposition::Returned
            && matches!(main.value(), Some(Ok(()))));
    if scope.cleanup.is_none() {
        let resources = entry.resources.clone();
        scope.cleanup = Some(OwnedCleanupTask::from_host_task(
            main.activation().clone(),
            tokio::spawn(async move {
                let report = super::super::entry::drain(&resources).await;
                if report.sealed && report.pending == 0 && report.failures.is_empty() {
                    Ok(())
                } else {
                    Err(OwnedCleanupFailure::Failed)
                }
            }),
        ));
    }
    let cleanup = scope.cleanup.as_mut().expect("retained cleanup installed");
    let proof = loop {
        tokio::select! {
            proof = main.observe_resources(cleanup) => break proof,
            _ = ticker.tick() => cancellation::renew(store, entry, authority).await,
        }
    };
    let Ok(proof) = proof else {
        super::failure::record(store, entry, authority, main.activation()).await;
        return;
    };
    if !successful {
        super::failure::record(store, entry, authority, main.activation()).await;
        return;
    }
    let (Ok(request), Ok(digest)) = (proof.request(), proof.fingerprint()) else {
        entry.set_status(Status::Uncertain);
        return;
    };
    let crabber_session::session_owner::SessionOwnerCommand::Join { termination } = request.command
    else {
        entry.set_status(Status::Uncertain);
        return;
    };
    let Some(original) = entry
        .credentials
        .lock()
        .unwrap()
        .as_ref()
        .map(|credentials| credentials.original.clone())
    else {
        entry.set_status(Status::Uncertain);
        return;
    };
    loop {
        match store
            .join_owned_scope(proof.clone(), authority.clone())
            .await
        {
            Ok(
                SessionOwnerOutcome::Applied {
                    receipt,
                    authority: None,
                }
                | SessionOwnerOutcome::Replayed(receipt),
            ) if receipt.session_id == request.session_id
                && receipt.operation_id == request.id
                && receipt.semantic_digest == digest
                && receipt.owner.target == request.target
                && receipt.operation == SessionOwnerOperation::Join
                && receipt.owner.state == SessionOwnerState::Joined
                && receipt.owner.binding == original.binding
                && receipt.owner.created_at == original.created_at
                && receipt.owner.termination == Some(termination) =>
            {
                entry.set_status(Status::Joined);
                return;
            }
            Err(SessionOwnerError::Deleted) => {
                entry.set_status(Status::Deleted);
                return;
            }
            _ => entry.set_status(Status::Closing),
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(1)) => {},
            _ = ticker.tick() => cancellation::renew(store, entry, authority).await,
        }
    }
}
