//! Detached work uses its own acknowledged dispatch and physical cleanup scope.
use super::{Admission, Error, Invocation, Parent, SessionInvocationStatus, registration::Started};
use crabber_extension::SessionInvocationTask;
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerAuthority, SessionOwnerError, SessionOwnerOutcome,
        owned_scope::{
            JoinedOwnedScope, OwnedCleanupFailure, OwnedCleanupTask, OwnedMainDisposition,
            OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedTaskRegistration,
        },
    },
};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

pub(super) async fn run(
    store: Arc<dyn Store>,
    parent: Parent,
    invocation: Arc<Invocation>,
    task: SessionInvocationTask,
    sender: oneshot::Sender<Result<Admission, Error>>,
    started: Started,
) {
    let authority = started.authority;
    let original = started.original;
    let context = crabber_extension::SessionInvocationContext {
        cancel: invocation.cancel.clone(),
        cleanup: invocation.resources.cleanup_tracker(),
    };
    let original_owner = invocation.clone();
    let registration = OwnedTaskRegistration::register(started.source, move || {
        super::close::scope(original_owner, async move {
            if context.cancel.is_cancelled() {
                Ok(())
            } else {
                task(context).await
            }
        })
    });
    let Ok(mut registration) = registration else {
        invocation.resources.close();
        invocation.status(SessionInvocationStatus::Uncertain);
        let _ = sender.send(Err(Error::UnknownOutcome));
        return;
    };
    super::cancellation::poll(&*store, &invocation, &authority).await;
    let activated = super::store_call(async {
        store
            .activate_owned_scope(
                registration.activation().clone(),
                authority.clone(),
                Some(OwnedScopeParentAuthority::Native(
                    crabber_session::session_owner::SessionOwnerParentAuthority::Phase(
                        parent.authority,
                    ),
                )),
            )
            .await
    })
    .await;
    let acknowledged = acknowledge_registration(
        activated,
        &mut registration,
        &*store,
        &invocation,
        &authority,
    )
    .await;
    invocation.status(if acknowledged {
        SessionInvocationStatus::Active
    } else {
        SessionInvocationStatus::Uncertain
    });
    let _ = sender.send(if acknowledged {
        Ok(Admission::Started)
    } else {
        Err(Error::UnknownOutcome)
    });
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let main = loop {
        tokio::select! {
            joined = registration.join() => break joined,
            _ = ticker.tick() => super::worker::renew(&*store, &invocation, &authority).await,
        }
    };
    let Ok(main) = main else {
        invocation.resources.close();
        invocation.status(SessionInvocationStatus::Uncertain);
        return;
    };
    let successful = main.disposition() == OwnedMainDisposition::NoDispatch
        || (main.disposition() == OwnedMainDisposition::Returned
            && matches!(main.value(), Some(Ok(()))));
    if !successful {
        invocation.status(SessionInvocationStatus::Uncertain);
    }
    let mut cleanup = register_cleanup(invocation.resources.clone(), main.activation().clone());
    let proof = loop {
        tokio::select! {
            proof = main.observe_resources(&mut cleanup) => break proof,
            _ = ticker.tick() => super::worker::renew(&*store, &invocation, &authority).await,
        }
    };
    match proof {
        Ok(proof) if successful => {
            publish_join(&*store, &invocation, authority, proof, &original).await;
        }
        _ => super::worker::mark_uncertain(&*store, &invocation, authority).await,
    }
}

fn register_cleanup(
    resources: crabber_extension::ToolResourceOwner,
    activation: crabber_session::session_owner::owned_scope::OwnedScopeActivation,
) -> OwnedCleanupTask {
    // Sealing and actual child joins are observed separately from main.
    OwnedCleanupTask::from_host_task(
        activation,
        tokio::spawn(async move {
            loop {
                let report = resources.drain(Duration::from_secs(1)).await;
                if report.pending == 0 {
                    return if report.sealed && report.failures.is_empty() {
                        Ok(())
                    } else {
                        Err(OwnedCleanupFailure::Failed)
                    };
                }
            }
        }),
    )
}

// A known cancelled registration still acknowledges its fresh Reserve+Begin.
// Unknown activation never becomes a dispatch permit or a known cancellation.
async fn acknowledge_registration<T: Send + 'static>(
    activated: Result<OwnedScopeActivationOutcome, SessionOwnerError>,
    registration: &mut OwnedTaskRegistration<T>,
    store: &dyn Store,
    invocation: &Invocation,
    authority: &SessionOwnerAuthority,
) -> bool {
    match activated {
        Ok(OwnedScopeActivationOutcome::Applied { permit, .. }) => {
            registration.dispatch(*permit).is_ok()
        }
        Err(SessionOwnerError::CancellationPending) => {
            // Reserve and Begin were acknowledged. Cancellation definitively
            // keeps this registration's gate closed; join() still observes the
            // actual waiting task and independently joined cleanup as NoDispatch.
            invocation.cancel.cancel();
            super::cancellation::poll(store, invocation, authority).await;
            true
        }
        _ => false,
    }
}

async fn publish_join(
    store: &dyn Store,
    invocation: &Invocation,
    authority: SessionOwnerAuthority,
    proof: JoinedOwnedScope,
    original: &crabber_session::session_owner::SessionOwnerSnapshot,
) {
    let (Ok(request), Ok(digest)) = (proof.request(), proof.fingerprint()) else {
        invocation.status(SessionInvocationStatus::Uncertain);
        return;
    };
    loop {
        let receipt = match super::store_call(async {
            store
                .join_owned_scope(proof.clone(), authority.clone())
                .await
        })
        .await
        {
            Ok(
                SessionOwnerOutcome::Applied {
                    receipt,
                    authority: None,
                }
                | SessionOwnerOutcome::Replayed(receipt),
            ) => Some(receipt),
            _ => None,
        };
        if receipt.is_some_and(|receipt| {
            super::registration::valid_join(&receipt, &request, &digest, original)
        }) {
            invocation.status(SessionInvocationStatus::Joined);
            return;
        }
        invocation.status(SessionInvocationStatus::Uncertain);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
