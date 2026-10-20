//! The retained supervisor owns native I/O, the closed dispatch gate and joins.
mod activation;
mod cancellation;
mod cleanup;
mod failure;
use super::{
    Reply,
    entry::{Access, Entry, Status},
};
use crabber_extension::SessionServiceDependencyTask;
use crabber_session::{
    Store,
    session_owner::{SessionOwnerError, service::SessionServiceDependencyOutcome},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

pub(super) async fn run(
    store: Arc<dyn Store>,
    entry: Arc<Entry>,
    task: SessionServiceDependencyTask,
    mut commands: mpsc::Receiver<Access>,
) {
    let first = tokio::select! {
        biased;
        () = entry.cancel.cancelled() => None,
        access = commands.recv() => access,
    };
    let Some(first) = first else {
        drop(task);
        super::entry::drain(&entry.resources).await;
        entry.set_status(Status::Cancelled);
        return;
    };
    let Some((authority, receipt, initial)) = register(&*store, &entry, first, &mut commands).await
    else {
        drop(task);
        super::entry::drain(&entry.resources).await;
        return;
    };
    let Some(()) = activation::start(&*store, &entry, &receipt, &authority, task).await else {
        let _ = initial.reply.send(Reply::Uncertain);
        super::entry::drain(&entry.resources).await;
        return;
    };
    access_ready(&*store, &entry, initial).await;
    // The first caller, like each later caller, must check its own exact live
    // root before exposing this generation. No metadata ACK is an access grant.
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut scope = entry.scope.lock().await;
    let Some(scope) = scope.as_mut() else { return };
    let successful_join = loop {
        tokio::select! {
            joined = scope.task.join() => break joined.is_ok(),
            Some(access) = commands.recv() => access_ready(&*store, &entry, access).await,
            _ = ticker.tick() => cancellation::renew(&*store, &entry, &authority).await,
        }
    };
    entry.set_status(Status::Closing);
    if successful_join {
        cleanup::complete(&*store, &entry, &authority, scope, &mut ticker).await;
    } else {
        entry.set_status(Status::Uncertain);
    }
}

async fn register(
    store: &dyn Store,
    entry: &Entry,
    first: Access,
    commands: &mut mpsc::Receiver<Access>,
) -> Option<(
    crabber_session::session_owner::SessionOwnerAuthority,
    Box<crabber_session::session_owner::service::SessionServiceDependencyReceipt>,
    Access,
)> {
    let mut access = first;
    loop {
        let same = crabber_session::session_owner::service::SessionServiceLink::for_grant(
            &access.grant,
            entry.source.generation.clone(),
        )
        .ok()
        .as_ref()
            == Some(&entry.source);
        if same
            && !entry.parent_done.is_cancelled()
            && !access.parent_done.is_cancelled()
            && !entry.cancel.is_cancelled()
        {
            let uncertain = entry.status() == Status::Uncertain;
            match store
                .register_session_service_dependency(entry.original.clone())
                .await
            {
                Ok(SessionServiceDependencyOutcome::Applied { receipt, authority })
                    if receipt.verify().is_ok()
                        && receipt.source == entry.source
                        && authority.session_id() == &receipt.source.root.session_id
                        && authority.target() == &receipt.began.owner.target =>
                {
                    // Keep this response sender until activation/access is known.
                    // It is queued ahead of later callers, without dispatching a
                    // second supplied task or replacing the original root.
                    return Some((authority, receipt, access));
                }
                Ok(SessionServiceDependencyOutcome::Replayed(_)) | Err(_) if uncertain => {
                    let _ = access.reply.send(Reply::Uncertain);
                }
                Ok(SessionServiceDependencyOutcome::Replayed(_)) => {
                    entry.set_status(Status::Replayed);
                    let _ = access.reply.send(Reply::Replayed);
                    return None;
                }
                Err(SessionOwnerError::UnknownStoreOutcome) => {
                    entry.set_status(Status::Uncertain);
                    let _ = access.reply.send(Reply::Uncertain);
                }
                Err(error) => {
                    entry.set_status(if error == SessionOwnerError::Deleted {
                        Status::Deleted
                    } else {
                        Status::Rejected
                    });
                    let _ = access.reply.send(Reply::Error(super::error(error)));
                    return None;
                }
                _ => {
                    entry.set_status(Status::Uncertain);
                    let _ = access.reply.send(Reply::Uncertain);
                }
            }
        } else {
            entry.set_status(Status::Uncertain);
            let _ = access.reply.send(Reply::Uncertain);
        }
        access = tokio::select! {
            () = entry.cancel.cancelled() => return None,
            access = commands.recv() => access?,
        };
    }
}

async fn access_ready(store: &dyn Store, entry: &Entry, access: Access) {
    if entry.status() != Status::Active
        || entry.cancel.is_cancelled()
        || access.parent_done.is_cancelled()
    {
        let _ = access.reply.send(Reply::Uncertain);
        return;
    }
    let credentials = entry.credentials.lock().unwrap().clone();
    let reply = if let Some(credentials) = credentials {
        match store
            .check_session_service_dependency_access(
                crabber_session::session_owner::service::SessionServiceDependencyAccess {
                    grant: access.grant,
                    source: credentials.source,
                    authority: credentials.authority,
                },
            )
            .await
        {
            Ok(())
                if entry.status() == Status::Active
                    && !entry.cancel.is_cancelled()
                    && !access.parent_done.is_cancelled() =>
            {
                Reply::Acquired
            }
            _ => Reply::Uncertain,
        }
    } else {
        Reply::Uncertain
    };
    let _ = access.reply.send(reply);
}

// Called only after observing the original supervisor. The retained scope is
// never dispatched here, including when activation acknowledgement was lost.
pub(super) async fn recover(entry: &Entry) {
    let credentials = entry.credentials.lock().unwrap().clone();
    let Some(credentials) = credentials else {
        return;
    };
    let mut scope = entry.scope.lock().await;
    let Some(scope) = scope.as_mut() else { return };
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    cleanup::complete(
        &*entry.store,
        entry,
        &credentials.authority,
        scope,
        &mut ticker,
    )
    .await;
}
