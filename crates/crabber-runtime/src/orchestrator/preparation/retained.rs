//! Exact local completion retention. A registry entry never remints dispatch.
use super::{JoinedPreparation, PreparationGrant, RuntimeError};
use crate::execution_tasks::RootScope;
use crabber_session::session_owner::{
    owned_scope::{
        JoinSource, JoinedOwnedScope, OwnedCleanupFailure, OwnedCleanupTask, OwnedTaskRegistration,
    },
    preparation::{PreparationReconciliation, PreparedOperation},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
#[path = "retained/close.rs"]
mod close;
pub(in crate::orchestrator) use close::PreparationCloseSelection;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
#[path = "retained/value.rs"]
mod value;
pub(in crate::orchestrator) use value::{PreparationValue, RetainedValue};
#[path = "retained/cleanup.rs"]
pub(super) mod cleanup;
#[path = "retained/heartbeat.rs"]
pub(super) mod heartbeat;

const MAX_RETAINED_PREPARATIONS: usize = 64;
type Key = (String, String);
type TaskResult = Result<(RetainedValue, PreparedOperation), RuntimeError>;
pub(super) type SharedEntry = Arc<Mutex<Entry>>;
struct IndexedEntry {
    descriptor: PreparationReconciliation,
    entry: SharedEntry,
    root_scope: Option<RootScope>,
    resources: Option<crabber_extension::ToolResourceOwner>,
}

#[derive(Clone)]
pub(in crate::orchestrator) struct RetainedPreparations {
    entries: Arc<std::sync::Mutex<BTreeMap<Key, IndexedEntry>>>,
    capacity: Arc<tokio::sync::Semaphore>,
    closed: Arc<AtomicBool>,
    changed: Arc<tokio::sync::Notify>,
}
pub(super) struct Entry {
    pub descriptor: PreparationReconciliation,
    pub root_scope: Option<RootScope>,
    pub grant: PreparationGrant,
    pub joined: Option<JoinedPreparation>,
    pub value: Option<RetainedValue>,
    pub owned: Option<OwnedEntry>,
    pub committed: bool,
    pub pending: Option<Arc<super::pending::PendingPreparation>>,
    pub legacy_failure: Option<super::finish::LegacyFailure>,
    pub disposing: bool,
    pub heartbeat_stop: CancellationToken,
    pub heartbeat: heartbeat::RetainedTask,
    pub cleanup_supervisor: Arc<cleanup::supervisor::CleanupSupervisor>,
    _capacity: tokio::sync::OwnedSemaphorePermit,
}
pub(super) struct OwnedEntry {
    pub task: OwnedTaskRegistration<TaskResult>,
    pub resources: crabber_extension::ToolResourceOwner,
    pub disposed: CancellationToken,
    pub cleanup: Option<OwnedCleanupTask>,
    pub cleanup_source: Option<JoinSource>,
    pub joined: Option<JoinedOwnedScope>,
    pub cleanup_failure: Option<OwnedCleanupFailure>,
}
impl RetainedPreparations {
    pub(super) fn reserve_capacity(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, RuntimeError> {
        self.capacity.clone().try_acquire_owned().map_err(|_| {
            RuntimeError::InvalidConfiguration("retained preparation capacity reached")
        })
    }
    pub(super) fn reserve(
        &self,
        grant: PreparationGrant,
        capacity: tokio::sync::OwnedSemaphorePermit,
        root_scope: Option<RootScope>,
    ) -> Result<SharedEntry, RuntimeError> {
        let descriptor = grant.reconciliation()?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.closed.load(Ordering::Acquire) && root_scope.is_none() {
            return Err(RuntimeError::HostClosed);
        }
        if entries.contains_key(&key(&descriptor)) {
            return Err(super::incomplete(
                descriptor,
                RuntimeError::PreparationRecoveryRequired,
            ));
        }
        if entries.len() >= MAX_RETAINED_PREPARATIONS {
            return Err(RuntimeError::InvalidConfiguration(
                "retained preparation capacity reached",
            ));
        }
        let entry = Arc::new(Mutex::new(Entry {
            descriptor: descriptor.clone(),
            root_scope: root_scope.clone(),
            grant,
            joined: None,
            value: None,
            owned: None,
            committed: false,
            pending: None,
            legacy_failure: None,
            disposing: false,
            heartbeat_stop: CancellationToken::new(),
            heartbeat: heartbeat::RetainedTask::default(),
            cleanup_supervisor: Arc::default(),
            _capacity: capacity,
        }));
        entries.insert(
            key(&descriptor),
            IndexedEntry {
                descriptor,
                entry: entry.clone(),
                root_scope,
                resources: None,
            },
        );
        self.changed.notify_waiters();
        Ok(entry)
    }
    pub(super) fn lookup(&self, descriptor: &PreparationReconciliation) -> Option<SharedEntry> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key(descriptor))
            .map(|indexed| indexed.entry.clone())
    }
    pub(super) fn remove(&self, descriptor: &PreparationReconciliation, entry: &SharedEntry) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = key(descriptor);
        if entries
            .get(&key)
            .is_some_and(|found| Arc::ptr_eq(&found.entry, entry))
        {
            let removed = entries.remove(&key);
            drop(entries);
            if let Some(root) = removed.and_then(|indexed| indexed.root_scope) {
                root.complete();
            }
            self.changed.notify_waiters();
        }
    }
    pub(super) fn status(
        &self,
        session: &crabber_core::SessionId,
    ) -> Vec<super::progress::RetainedPreparationProgress> {
        use super::progress::{RetainedPreparationProgress, RetainedPreparationStatus as Status};
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries
            .values()
            .filter(|indexed| indexed.descriptor.session_id == *session)
            .map(|indexed| {
                let descriptor = &indexed.descriptor;
                let entry = &indexed.entry;
                let status = entry
                    .try_lock()
                    .map_or(Status::ObservationInProgress, |current| {
                        if current
                            .pending
                            .as_ref()
                            .is_some_and(|pending| pending.failed())
                            || current.heartbeat.failed()
                            || current.cleanup_supervisor.is_finished()
                        {
                            Status::CleanupUncertain
                        } else if current.disposing {
                            Status::CleanupRequested
                        } else if current.committed
                            || current
                                .pending
                                .as_ref()
                                .is_some_and(|pending| pending.committed())
                        {
                            Status::ResourcesRetained
                        } else if current.joined.is_some() {
                            Status::NativeCommitPending
                        } else {
                            Status::ObservationInProgress
                        }
                    });
                RetainedPreparationProgress {
                    preparation: descriptor.clone(),
                    status,
                }
            })
            .collect()
    }
}
impl Default for RetainedPreparations {
    fn default() -> Self {
        Self {
            entries: Arc::default(),
            capacity: Arc::new(tokio::sync::Semaphore::new(MAX_RETAINED_PREPARATIONS)),
            closed: Arc::new(AtomicBool::new(false)),
            changed: Arc::default(),
        }
    }
}
fn key(descriptor: &PreparationReconciliation) -> Key {
    (
        descriptor.session_id.0.clone(),
        serde_json::to_string(&descriptor.owner).expect("bounded target serializes"),
    )
}
#[cfg(test)]
#[path = "retained/capacity_tests.rs"]
mod capacity_tests;
