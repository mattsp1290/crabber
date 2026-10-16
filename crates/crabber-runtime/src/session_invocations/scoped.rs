//! Detached work uses its own acknowledged dispatch and physical cleanup scope.
use super::{Admission, Error, Invocation, Parent, SessionInvocationStatus, registration::Started};
use crabber_extension::SessionInvocationTask;
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerAuthority, SessionOwnerOperation, SessionOwnerOutcome, SessionOwnerState,
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
    let context = crabber_extension::SessionInvocationContext {
        cancel: invocation.cancel.clone(),
        cleanup: invocation.resources.cleanup_tracker(),
    };
    let registration = OwnedTaskRegistration::register(started.source, move || async move {
        if context.cancel.is_cancelled() {
            Ok(())
        } else {
            task(context).await
        }
    });
    let Ok(mut registration) = registration else {
        invocation.resources.close();
        invocation.status(SessionInvocationStatus::Uncertain);
        let _ = sender.send(Err(Error::UnknownOutcome));
        return;
    };
    super::cancellation::poll(&*store, &invocation, &authority).await;
    let activated = store
        .activate_owned_scope(
            registration.activation().clone(),
            authority.clone(),
            Some(OwnedScopeParentAuthority::Native(
                crabber_session::session_owner::SessionOwnerParentAuthority::Phase(
                    parent.authority,
                ),
            )),
        )
        .await;
    let dispatched = match activated {
        Ok(OwnedScopeActivationOutcome::Applied { permit, .. }) => {
            registration.dispatch(*permit).is_ok()
        }
        _ => false,
    };
    invocation.status(if dispatched {
        SessionInvocationStatus::Active
    } else {
        SessionInvocationStatus::Uncertain
    });
    let _ = sender.send(if dispatched {
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
    let resources = invocation.resources.clone();
    // This task owns the exact invocation scope's sealing and actual child
    // joins. Its result is observed independently from the main task result.
    let mut cleanup = OwnedCleanupTask::from_host_task(
        main.activation().clone(),
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
    );
    let proof = loop {
        tokio::select! {
            proof = main.observe_resources(&mut cleanup) => break proof,
            _ = ticker.tick() => super::worker::renew(&*store, &invocation, &authority).await,
        }
    };
    match proof {
        Ok(proof) if successful => publish_join(&*store, &invocation, authority, proof).await,
        _ => super::worker::mark_uncertain(&*store, &invocation, authority).await,
    }
}

async fn publish_join(
    store: &dyn Store,
    invocation: &Invocation,
    authority: SessionOwnerAuthority,
    proof: JoinedOwnedScope,
) {
    let (Ok(request), Ok(digest)) = (proof.request(), proof.fingerprint()) else {
        invocation.status(SessionInvocationStatus::Uncertain);
        return;
    };
    loop {
        let receipt = match store
            .join_owned_scope(proof.clone(), authority.clone())
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
            receipt.session_id == request.session_id
                && receipt.operation_id == request.id
                && receipt.semantic_digest == digest
                && receipt.operation == SessionOwnerOperation::Join
                && receipt.owner.target == request.target
                && receipt.owner.state == SessionOwnerState::Joined
        }) {
            invocation.status(SessionInvocationStatus::Joined);
            return;
        }
        invocation.status(SessionInvocationStatus::Uncertain);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
