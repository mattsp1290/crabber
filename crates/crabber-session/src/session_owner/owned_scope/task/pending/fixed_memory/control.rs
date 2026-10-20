//! The actual library control root; no injected Store or clock enters this path.
use super::super::PendingDriverTasks;
use crate::MemoryStore;
use std::sync::Arc;

pub(crate) struct FixedMemoryInitialControl {
    store: Arc<MemoryStore>,
    tasks: PendingDriverTasks,
}
impl FixedMemoryInitialControl {
    pub(super) fn new() -> Self {
        Self {
            store: Arc::new(MemoryStore::new()),
            tasks: PendingDriverTasks::default(),
        }
    }
    pub(crate) fn actual_store(&self) -> &MemoryStore {
        &self.store
    }
    pub(crate) fn store(&self) -> &Arc<MemoryStore> {
        &self.store
    }
    pub(super) fn now() -> time::OffsetDateTime {
        crabber_core::Clock::now(&crabber_core::SystemClock)
    }
    pub(super) fn tasks(&self) -> &PendingDriverTasks {
        &self.tasks
    }
}
