//! Native session dependencies retain their original task and source across waiters.
mod close;
mod entry;
pub(crate) use close::ServiceCloseSelection;
pub use close::{SessionServiceReport, SessionServiceStatus};
mod generation;
pub(crate) mod provider;
mod worker;
use crabber_extension::{
    ServiceGenerationDescriptor, SessionServiceDependencies,
    SessionServiceDependencyAdmission as Admission, SessionServiceDependencyError as Error,
    SessionServiceDependencyHandle, SessionServiceDependencyTask, SessionServiceRegistrar,
};
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerError, preparation::PreparationGrant, service::RegisterSessionServiceDependency,
    },
};
use entry::{Entries, Entry, Status};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, oneshot};

const CAPACITY: usize = 1024;
#[derive(Default, Clone)]
pub(crate) struct SessionServices {
    entries: Arc<Mutex<Entries>>,
    closed: Arc<AtomicBool>,
}
struct Registrar {
    manager: SessionServices,
    store: Arc<dyn Store>,
    grant: PreparationGrant,
    parent_done: tokio_util::sync::CancellationToken,
}
pub(super) enum Reply {
    Acquired,
    Uncertain,
    Replayed,
    Cancelled,
    Error(Error),
}
impl SessionServices {
    pub(crate) fn bind(
        &self,
        store: Arc<dyn Store>,
        grant: PreparationGrant,
        parent_done: tokio_util::sync::CancellationToken,
    ) -> SessionServiceDependencies {
        SessionServiceDependencies::from_host(Arc::new(Registrar {
            manager: self.clone(),
            store,
            grant,
            parent_done,
        }))
    }
    async fn reap(&self) {
        let candidates = self
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in candidates {
            if !matches!(
                entry.status(),
                Status::Joined | Status::Replayed | Status::Cancelled | Status::Rejected
            ) {
                continue;
            }
            let finished = {
                let Ok(supervisor) = entry.supervisor.try_lock() else {
                    continue;
                };
                supervisor
                    .as_ref()
                    .is_some_and(tokio::task::JoinHandle::is_finished)
                    || *entry.observed.lock().unwrap()
            };
            if !finished || entry.join().await.is_err() {
                continue;
            }
            let mut entries = self.entries.lock().unwrap();
            let key = (
                entry.source.root.session_id.clone(),
                entry.source.generation.id.clone(),
            );
            if entries
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
            {
                entries.remove(&key);
            }
        }
    }
}
#[async_trait::async_trait]
impl SessionServiceRegistrar for Registrar {
    async fn register(
        &self,
        descriptor: ServiceGenerationDescriptor,
        task: SessionServiceDependencyTask,
    ) -> Result<Admission, Error> {
        if !self.store.supports_session_service_dependencies()
            || !self.store.supports_owned_scopes()
        {
            return Err(Error::Unsupported);
        }
        let generation = generation::capture(&descriptor, &self.grant).map_err(error)?;
        let original = RegisterSessionServiceDependency {
            grant: self.grant.clone(),
            generation,
        };
        let source = original.source().map_err(error)?;
        self.manager.reap().await;
        let key = (source.root.session_id.clone(), source.generation.id.clone());
        let mut task = Some(task);
        let entry = {
            let mut entries = self.manager.entries.lock().unwrap();
            if self.manager.closed.load(Ordering::Acquire) {
                drop(entries);
                return Err(Error::Rejected);
            }
            if let Some(entry) = entries.get(&key) {
                if entry.source.generation != source.generation {
                    return Err(Error::Rejected);
                }
                entry.clone()
            } else {
                if self.parent_done.is_cancelled() {
                    return Err(Error::Rejected);
                }
                if entries.len() >= CAPACITY {
                    return Err(Error::Capacity);
                }
                let (commands, receiver) = mpsc::channel(8);
                let entry = Arc::new(Entry::new(
                    self.store.clone(),
                    original,
                    commands,
                    self.parent_done.clone(),
                )?);
                let (release, retained) = oneshot::channel();
                let worker_entry = entry.clone();
                let task = task.take().expect("fresh dependency task");
                let store = self.store.clone();
                let original_entry = entry.clone();
                let supervisor = tokio::spawn(close::scope(original_entry, async move {
                    if retained.await.is_ok() {
                        worker::run(store, worker_entry, task, receiver).await;
                    }
                }));
                // The worker cannot begin native I/O until its actual handle is
                // retained and the entry belongs to the manager's exact roster.
                *entry.supervisor.try_lock().expect("unexposed supervisor") = Some(supervisor);
                entries.insert(key, entry.clone());
                let _ = release.send(());
                entry
            }
        };
        // The original task is already retained. This duplicate task must never
        // replace it or run, even when the new root's access check succeeds.
        drop(task);
        self.finish_registration(entry).await
    }
}
impl Registrar {
    async fn finish_registration(&self, entry: Arc<Entry>) -> Result<Admission, Error> {
        let reply = entry
            .access(self.grant.clone(), self.parent_done.clone())
            .await;
        let handle = || SessionServiceDependencyHandle::from_host(entry.clone());
        match reply {
            Reply::Acquired => Ok(Admission::Acquired(handle())),
            Reply::Uncertain => Ok(Admission::Uncertain(handle())),
            Reply::Replayed | Reply::Cancelled | Reply::Error(_) => {
                // Finite no-dispatch replies include actual supervisor disposal,
                // rather than an ACK emitted before its captures were dropped.
                if matches!(
                    entry.status(),
                    Status::Replayed | Status::Cancelled | Status::Rejected | Status::Deleted
                ) && entry.join().await.is_err()
                {
                    return Ok(Admission::Uncertain(handle()));
                }
                match reply {
                    Reply::Replayed => Ok(Admission::Replayed),
                    Reply::Cancelled => Ok(Admission::Cancelled),
                    Reply::Error(error) => Err(error),
                    _ => unreachable!(),
                }
            }
        }
    }
}
fn error(error: SessionOwnerError) -> Error {
    match error {
        SessionOwnerError::Unsupported => Error::Unsupported,
        SessionOwnerError::Capacity => Error::Capacity,
        SessionOwnerError::Deleted => Error::Deleted,
        SessionOwnerError::UnknownStoreOutcome => Error::UnknownOutcome,
        _ => Error::Rejected,
    }
}
