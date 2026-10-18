//! Actual original supervisor joins, with bounded retained failure accounting.
mod body;
mod observation;
use super::super::Error;
use super::PendingSuccessorError;
use super::core::{Control, Observation, TaskOutput};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch},
    task::JoinHandle,
};

const MAX_PENDING: usize = 64;
pub(super) struct CapacityLease {
    _permit: OwnedSemaphorePermit,
}
struct Entry {
    failed: AtomicBool,
    _capacity: Arc<CapacityLease>,
    unobserved: Mutex<Option<JoinHandle<()>>>,
}
struct Registry {
    tasks: Mutex<BTreeMap<uuid::Uuid, Arc<Entry>>>,
    capacity: Arc<Semaphore>,
    changed: watch::Sender<()>,
    #[cfg(test)]
    epilogue: Mutex<Option<Arc<supervision_joins::Gate>>>,
}
/// Actual retained-supervisor observation, not native/resource retirement proof.
/// Concurrent later registrations and caller-owned cached outputs are excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSupervisionReport {
    pending: usize,
    failed: usize,
}
impl PendingSupervisionReport {
    #[must_use]
    pub fn pending_supervisors(&self) -> usize {
        self.pending
    }
    #[must_use]
    pub fn failed_supervisors(&self) -> usize {
        self.failed
    }
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.pending == 0 && self.failed == 0
    }
}
/// At most 64 registrations, including sticky failed supervision and cached
/// legacy output leases. Dropped public tasks retain their actual supervisors.
#[derive(Clone)]
pub struct PendingDriverTasks {
    inner: Arc<Registry>,
}
impl Default for PendingDriverTasks {
    fn default() -> Self {
        Self {
            inner: Arc::new(Registry {
                tasks: Mutex::default(),
                capacity: Arc::new(Semaphore::new(MAX_PENDING)),
                changed: watch::channel(()).0,
                #[cfg(test)]
                epilogue: Mutex::default(),
            }),
        }
    }
}
impl Registry {
    fn report(&self) -> PendingSupervisionReport {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failed = tasks
            .values()
            .filter(|entry| entry.failed.load(Ordering::Acquire))
            .count();
        PendingSupervisionReport {
            pending: tasks.len() - failed,
            failed,
        }
    }
}
impl PendingDriverTasks {
    #[cfg(test)]
    pub(super) fn available_capacity(&self) -> usize {
        self.inner.capacity.available_permits()
    }
    pub(super) fn capacity(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.inner
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)
    }
    /// Wait for successful actual supervisor joins. Sticky failed supervision
    /// keeps this wait pending; use `observe_supervision` for a bounded report.
    /// This does not observe disposal of caller-retained legacy cached outputs.
    pub async fn wait_for_supervision(&self) {
        let mut changed = self.inner.changed.subscribe();
        loop {
            if self.inner.report().is_complete() {
                return;
            }
            if changed.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
    /// Observe the same actual supervisors until completion, terminal failure,
    /// or the deadline. Cancellation drops only this watch waiter. Zero or
    /// overflowing durations return the current conservative snapshot.
    /// No native settlement, arbitrary resource join, or future admission is certified.
    pub async fn observe_supervision(&self, bound: Duration) -> PendingSupervisionReport {
        let mut changed = self.inner.changed.subscribe();
        let Some(deadline) = tokio::time::Instant::now().checked_add(bound) else {
            return self.inner.report();
        };
        loop {
            let report = self.inner.report();
            if report.pending == 0 || tokio::time::Instant::now() >= deadline {
                return report;
            }
            if tokio::time::timeout_at(deadline, changed.changed())
                .await
                .is_err()
            {
                return self.inner.report();
            }
        }
    }
    pub(super) fn retain<T: Send + 'static>(
        &self,
        task: JoinHandle<Result<TaskOutput<T>, PendingSuccessorError>>,
        control: Control,
        send: oneshot::Sender<Observation<T>>,
        capacity: OwnedSemaphorePermit,
        protected: Option<super::core::RetainedOwner>,
    ) {
        let capacity = Arc::new(CapacityLease { _permit: capacity });
        // The original main is still parked until register_body returns.
        let supervisor = tokio::spawn(body::run(
            task,
            control,
            send,
            capacity.clone(),
            protected,
            #[cfg(test)]
            self.inner.epilogue.lock().unwrap().clone(),
        ));
        self.track(supervisor, capacity);
    }
    fn track(&self, supervisor: JoinHandle<()>, capacity: Arc<CapacityLease>) {
        let id = uuid::Uuid::new_v4();
        let entry = Arc::new(Entry {
            failed: AtomicBool::new(false),
            _capacity: capacity,
            unobserved: Mutex::default(),
        });
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, entry.clone());
        let observer = observation::Observer::new(self.inner.clone(), id, entry, supervisor);
        tokio::spawn(observer.run());
    }
}

pub(super) fn discard<T>(value: T) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)))
        && let Err(nested) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
    {
        std::mem::forget(nested);
    }
}

#[cfg(test)]
#[path = "retain/tests.rs"]
mod supervision_joins;
