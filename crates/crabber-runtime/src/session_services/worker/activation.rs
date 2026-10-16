//! The local factory guard and start ACK close the gate-delivery race.
use super::super::entry::{Credentials, Entry, Scope, Status};
use crabber_extension::{SessionServiceDependencyContext, SessionServiceDependencyTask};
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerAuthority, SessionOwnerError,
        owned_scope::{
            OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
            OwnedTaskRegistration,
        },
        service::SessionServiceDependencyReceipt,
    },
};

pub(super) async fn start(
    store: &dyn Store,
    entry: &Entry,
    receipt: &SessionServiceDependencyReceipt,
    authority: &SessionOwnerAuthority,
    task: SessionServiceDependencyTask,
) -> Option<()> {
    let Ok(source) = OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began) else {
        entry.set_status(Status::Uncertain);
        return None;
    };
    let context = SessionServiceDependencyContext {
        cancel: entry.cancel.clone(),
        cleanup: entry.resources.cleanup_tracker(),
    };
    let parent_done = entry.parent_done.clone();
    let (started, observed) = tokio::sync::oneshot::channel();
    let Ok(registration) = OwnedTaskRegistration::register(source, move || async move {
        // This guard closes ingress only; it is never a main/resource Join.
        // Once this task has actually been transferred, ending the parent does
        // not cancel the independently retained dependency's lifetime.
        if parent_done.is_cancelled() || context.cancel.is_cancelled() {
            return Ok(());
        }
        let work = task(context);
        let _ = started.send(());
        work.await
    }) else {
        entry.set_status(Status::Uncertain);
        return None;
    };
    *entry.credentials.lock().unwrap() = Some(Credentials {
        source: receipt.source.clone(),
        authority: authority.clone(),
        original: receipt.began.owner.clone(),
    });
    // Install the actual handle before polling any external activation future.
    let mut scope = entry.scope.lock().await;
    *scope = Some(Scope {
        task: registration,
        cleanup: None,
    });
    let registration = &mut scope.as_mut()?.task;
    let activated = if entry.parent_done.is_cancelled() || entry.cancel.is_cancelled() {
        Err(SessionOwnerError::CancellationPending)
    } else {
        store
            .activate_owned_scope(
                registration.activation().clone(),
                authority.clone(),
                Some(OwnedScopeParentAuthority::ServicePreparation(Box::new(
                    entry.original.grant.clone(),
                ))),
            )
            .await
    };
    let dispatched = match activated {
        Ok(OwnedScopeActivationOutcome::Applied { permit, .. })
            if !entry.cancel.is_cancelled() && !entry.parent_done.is_cancelled() =>
        {
            registration.dispatch(*permit).is_ok()
        }
        Err(SessionOwnerError::CancellationPending) => {
            entry.cancel.cancel();
            false
        }
        _ => false,
    };
    // Delivery of a permit is not proof that the actual FnOnce was admitted.
    // Its constructor must have run under the guard before Acquired is exposed.
    if dispatched && observed.await.is_ok() {
        entry.set_status(Status::Active);
    } else {
        entry.set_status(Status::Uncertain);
    }
    Some(())
}
