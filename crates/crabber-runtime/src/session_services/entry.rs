//! The opaque control retains the original supervisor and its actual join.
use super::{Error, Reply};
use crabber_extension::{ResourceDrain, SessionServiceDependencyControl, ToolResourceOwner};
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerAuthority,
        owned_scope::{OwnedCleanupTask, OwnedTaskRegistration},
        preparation::PreparationGrant,
        service::{RegisterSessionServiceDependency, SessionServiceLink},
    },
};
use futures::FutureExt;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub(super) use super::close::SessionServiceStatus as Status;
pub(super) struct Access {
    pub grant: PreparationGrant,
    pub parent_done: CancellationToken,
    pub reply: oneshot::Sender<Reply>,
}
#[derive(Clone)]
pub(super) struct Credentials {
    pub source: SessionServiceLink,
    pub authority: SessionOwnerAuthority,
    pub original: crabber_session::session_owner::SessionOwnerSnapshot,
}
pub(super) struct Scope {
    pub task: OwnedTaskRegistration<Result<(), crabber_extension::ResourceFailure>>,
    pub cleanup: Option<OwnedCleanupTask>,
}
pub(super) struct Entry {
    pub store: Arc<dyn Store>,
    pub scope: tokio::sync::Mutex<Option<Scope>>,
    pub original: RegisterSessionServiceDependency,
    pub source: SessionServiceLink,
    pub parent_done: CancellationToken,
    pub cancel: CancellationToken,
    pub resources: ToolResourceOwner,
    pub status: Mutex<Status>,
    pub credentials: Mutex<Option<Credentials>>,
    pub commands: mpsc::Sender<Access>,
    pub cancel_observation:
        Mutex<Option<crabber_session::session_owner::ObserveSessionOwnerCancel>>,
    pub supervisor: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub observed: Mutex<bool>,
    pub join_failed: AtomicBool,
}
impl Entry {
    pub fn new(
        store: Arc<dyn Store>,
        original: RegisterSessionServiceDependency,
        commands: mpsc::Sender<Access>,
        parent_done: CancellationToken,
    ) -> Result<Self, Error> {
        let source = original.source().map_err(super::error)?;
        Ok(Self {
            store,
            scope: tokio::sync::Mutex::new(None),
            original,
            source,
            parent_done,
            cancel: CancellationToken::new(),
            resources: ToolResourceOwner::new(),
            status: Mutex::new(Status::Registering),
            credentials: Mutex::new(None),
            commands,
            cancel_observation: Mutex::new(None),
            supervisor: tokio::sync::Mutex::new(None),
            observed: Mutex::new(false),
            join_failed: AtomicBool::new(false),
        })
    }
    pub fn status(&self) -> Status {
        *self.status.lock().unwrap()
    }
    pub fn set_status(&self, status: Status) {
        *self.status.lock().unwrap() = status;
    }
    pub async fn join(&self) -> Result<(), Error> {
        let mut supervisor = self.supervisor.lock().await;
        let error = if let Some(task) = supervisor.as_mut() {
            let result = task.await;
            *supervisor = None;
            let error = result.err();
            if error.is_some() {
                self.join_failed.store(true, Ordering::Release);
                self.set_status(Status::Uncertain);
            }
            *self.observed.lock().unwrap() = true;
            error
        } else {
            None
        };
        drop(supervisor);
        if let Some(error) = error {
            if error.is_panic() {
                discard_panic(error.into_panic());
            }
            return Err(Error::Incomplete);
        }
        if self.join_failed.load(Ordering::Acquire) || !*self.observed.lock().unwrap() {
            return Err(Error::Incomplete);
        }
        Ok(())
    }
    pub async fn access(&self, grant: PreparationGrant, parent_done: CancellationToken) -> Reply {
        if self.status() == Status::Closing {
            return Reply::Uncertain;
        }
        let (reply, result) = oneshot::channel();
        if self
            .commands
            .send(Access {
                grant,
                parent_done,
                reply,
            })
            .await
            .is_err()
        {
            return match self.status() {
                Status::Joined | Status::Cancelled => Reply::Cancelled,
                Status::Replayed => Reply::Replayed,
                Status::Deleted => Reply::Error(Error::Deleted),
                Status::Rejected => Reply::Error(Error::Rejected),
                _ => Reply::Uncertain,
            };
        }
        result.await.unwrap_or(Reply::Uncertain)
    }
}
#[async_trait::async_trait]
impl SessionServiceDependencyControl for Entry {
    async fn close_and_join(&self, bound: Duration) -> Result<ResourceDrain, Error> {
        self.cancel.cancel();
        self.resources.close();
        let recovered = std::panic::AssertUnwindSafe(async {
            // A failed supervisor cannot own away the physical task. Its scope
            // stays in this control and cleanup may resume without dispatch.
            let _ = self.join().await;
            if self.status() == Status::Uncertain {
                super::worker::recover(self).await;
            }
        })
        .catch_unwind();
        if let Err(payload) = tokio::time::timeout(bound, recovered)
            .await
            .map_err(|_| Error::Incomplete)?
        {
            self.set_status(Status::Uncertain);
            discard_panic(payload);
            return Err(Error::Incomplete);
        }
        let report = self.resources.snapshot();
        if !report.sealed || report.pending != 0 || !report.failures.is_empty() {
            return Err(Error::Incomplete);
        }
        match self.status() {
            Status::Joined | Status::Cancelled | Status::Replayed | Status::Rejected => Ok(report),
            Status::Deleted => Err(Error::Deleted),
            _ => Err(Error::Incomplete),
        }
    }
}

pub(super) type Entries = std::collections::BTreeMap<(crabber_core::SessionId, String), Arc<Entry>>;

// drain() observes the retained task handles; pending is read only after that
// real join attempt. Failed child results remain sticky in the owner.
pub(super) async fn drain(resources: &ToolResourceOwner) -> ResourceDrain {
    loop {
        let report = resources.drain(Duration::from_secs(1)).await;
        if report.pending == 0 {
            return report;
        }
    }
}

// Dropping a custom panic payload may itself panic. Neither failure may unwind
// the retained owner before its physical join state has been saved.
fn discard_panic(payload: Box<dyn std::any::Any + Send>) {
    if let Err(nested) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(nested);
    }
}
