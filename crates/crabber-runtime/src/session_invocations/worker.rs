//! Independent dispatch, private heartbeat, and actual cleanup/retirement joins.
use super::{Admission, Error, Invocation, Parent, SessionInvocationStatus};
use crabber_extension::SessionInvocationTask;
use crabber_session::Store;
use crabber_session::session_owner::{
    MutateSessionOwner, SessionOwnerAuthority, SessionOwnerCommand as Command,
    SessionOwnerOperation, SessionOwnerOutcome as Outcome, SessionOwnerState,
    SessionOwnerTermination,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
pub(super) async fn run(
    store: Arc<dyn Store>,
    parent: Parent,
    invocation: Arc<Invocation>,
    task: SessionInvocationTask,
    sender: oneshot::Sender<Result<Admission, Error>>,
) {
    let authority = match super::registration::start(&*store, &parent, &invocation).await {
        Ok(Some(authority)) => authority,
        Ok(None) => {
            invocation.status(SessionInvocationStatus::Replayed);
            invocation.resources.close();
            let _ = sender.send(Ok(Admission::Replayed));
            return;
        }
        Err(error) => {
            invocation.status(if error == Error::UnknownOutcome {
                SessionInvocationStatus::Uncertain
            } else {
                SessionInvocationStatus::Rejected
            });
            invocation.resources.close();
            let _ = sender.send(Err(error));
            return;
        }
    };
    invocation.status(SessionInvocationStatus::Active);
    let resources = invocation.resources.clone();
    let context = crabber_extension::SessionInvocationContext {
        cancel: invocation.cancel.clone(),
        cleanup: resources.cleanup_tracker(),
    };
    // Calling the factory may itself panic. It runs inside the actual joined
    // task, only after both acknowledged fresh commits above.
    let mut work = tokio::spawn(async move {
        if context.cancel.is_cancelled() {
            Ok(())
        } else {
            task(context).await
        }
    });
    let _ = sender.send(Ok(Admission::Started));
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            result = &mut work => break result,
            _ = ticker.tick() => {
                renew(&*store, &invocation, &authority).await;
            }
        }
    };
    let incomplete = join_resources(
        &*store,
        &resources,
        &invocation,
        &authority,
        &mut ticker,
        result,
    )
    .await;
    if incomplete {
        invocation.status(SessionInvocationStatus::Uncertain);
        let uncertain = MutateSessionOwner {
            id: crabber_core::SessionOwnerOperationId::new(),
            session_id: parent.session.clone(),
            target: authority.target().clone(),
            command: Command::MarkUncertain {
                claim: crabber_session::session_owner::SessionOwnerResolver {
                    resolver_id: "crabber.runtime".into(),
                    authority_claim: "observed detached task or resource cleanup failure".into(),
                },
            },
        };
        let _ = store.mutate_session_owner(uncertain, Some(authority)).await;
        return;
    }
    let termination = SessionOwnerTermination::Returned;
    publish_join(&*store, parent, &invocation, authority, termination).await;
}

async fn publish_join(
    store: &dyn Store,
    parent: Parent,
    invocation: &Invocation,
    authority: SessionOwnerAuthority,
    termination: SessionOwnerTermination,
) {
    let join = MutateSessionOwner {
        id: invocation.request.join_id.clone(),
        session_id: parent.session,
        target: authority.target().clone(),
        command: Command::Join { termination },
    };
    let Ok(digest) = join.fingerprint() else {
        invocation.status(SessionInvocationStatus::Uncertain);
        return;
    };
    loop {
        let receipt = match store
            .mutate_session_owner(join.clone(), Some(authority.clone()))
            .await
        {
            Ok(
                Outcome::Applied {
                    receipt,
                    authority: None,
                }
                | Outcome::Replayed(receipt),
            ) => Some(receipt),
            _ => None,
        };
        if receipt.is_some_and(|receipt| {
            receipt.session_id == join.session_id
                && receipt.operation_id == join.id
                && receipt.semantic_digest == digest
                && receipt.operation == SessionOwnerOperation::Join
                && receipt.owner.target == join.target
                && receipt.owner.state == SessionOwnerState::Joined
                && receipt.owner.termination == Some(termination)
        }) {
            invocation.status(SessionInvocationStatus::Joined);
            return;
        }
        invocation.status(SessionInvocationStatus::Uncertain);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub(super) fn discard_panic(error: tokio::task::JoinError) {
    if error.is_panic() {
        let payload = error.into_panic();
        if let Err(nested) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
        {
            std::mem::forget(nested);
        }
    }
}
async fn renew(store: &dyn Store, invocation: &Invocation, authority: &SessionOwnerAuthority) {
    let renew = MutateSessionOwner {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: authority.session_id().clone(),
        target: authority.target().clone(),
        command: Command::Renew { lease_ms: 30_000 },
    };
    if store
        .mutate_session_owner(renew, Some(authority.clone()))
        .await
        .is_err()
    {
        invocation.cancel.cancel();
    }
}
async fn join_resources(
    store: &dyn Store,
    resources: &crabber_extension::ToolResourceOwner,
    invocation: &Invocation,
    authority: &SessionOwnerAuthority,
    ticker: &mut tokio::time::Interval,
    result: Result<Result<(), crabber_extension::ResourceFailure>, tokio::task::JoinError>,
) -> bool {
    let mut incomplete = match result {
        Ok(Ok(())) => false,
        Ok(Err(_)) => true,
        Err(error) => {
            discard_panic(error);
            true
        }
    };
    loop {
        let report = tokio::select! {
            report = resources.drain(Duration::from_secs(1)) => report,
            _ = ticker.tick() => { renew(store, invocation, authority).await; continue; }
        };
        incomplete |= !report.failures.is_empty();
        if incomplete {
            invocation.status(SessionInvocationStatus::Uncertain);
        }
        if report.pending == 0 {
            return incomplete;
        }
    }
}
