//! Exact local completion retention. A registry entry never remints dispatch.
use super::{JoinedPreparation, PreparationGrant, RuntimeError};
use crabber_session::session_owner::{
    owned_scope::{
        JoinSource, JoinedOwnedScope, OwnedCleanupFailure, OwnedCleanupTask, OwnedTaskRegistration,
    },
    preparation::{PreparationReconciliation, PreparedOperation},
};
use std::{collections::BTreeMap, sync::Arc};
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
type IndexedEntry = (PreparationReconciliation, SharedEntry);

#[derive(Clone)]
pub(in crate::orchestrator) struct RetainedPreparations {
    entries: Arc<std::sync::Mutex<BTreeMap<Key, IndexedEntry>>>,
    capacity: Arc<tokio::sync::Semaphore>,
}
pub(super) struct Entry {
    pub descriptor: PreparationReconciliation,
    pub grant: PreparationGrant,
    pub joined: Option<JoinedPreparation>,
    pub value: Option<RetainedValue>,
    pub owned: Option<OwnedEntry>,
    pub committed: bool,
    pub disposing: bool,
    pub heartbeat_stop: CancellationToken,
    pub heartbeat: heartbeat::RetainedTask,
    pub cleanup_supervisor: heartbeat::RetainedTask,
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
    ) -> Result<SharedEntry, RuntimeError> {
        let descriptor = grant.reconciliation()?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            grant,
            joined: None,
            value: None,
            owned: None,
            committed: false,
            disposing: false,
            heartbeat_stop: CancellationToken::new(),
            heartbeat: heartbeat::RetainedTask::default(),
            cleanup_supervisor: heartbeat::RetainedTask::default(),
            _capacity: capacity,
        }));
        entries.insert(key(&descriptor), (descriptor, entry.clone()));
        Ok(entry)
    }
    pub(super) fn lookup(&self, descriptor: &PreparationReconciliation) -> Option<SharedEntry> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key(descriptor))
            .map(|(_, entry)| entry.clone())
    }
    pub(super) fn remove(&self, descriptor: &PreparationReconciliation, entry: &SharedEntry) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = key(descriptor);
        if entries
            .get(&key)
            .is_some_and(|(_, found)| Arc::ptr_eq(found, entry))
        {
            entries.remove(&key);
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
            .filter(|(descriptor, _)| descriptor.session_id == *session)
            .map(|(descriptor, entry)| {
                let status = entry
                    .try_lock()
                    .map_or(Status::ObservationInProgress, |current| {
                        if current.heartbeat.failed() || current.cleanup_supervisor.is_finished() {
                            Status::CleanupUncertain
                        } else if current.disposing {
                            Status::CleanupRequested
                        } else if current.committed {
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
