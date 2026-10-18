use super::super::{Error, JoinedOwnedTask, OwnedMainDisposition};
use super::PendingSuccessorError;
use super::core::{Control, Observation, TaskOutput};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch},
    task::JoinHandle,
};

const MAX_PENDING: usize = 64;
struct Registry {
    tasks: Mutex<BTreeMap<uuid::Uuid, JoinHandle<()>>>,
    capacity: Arc<Semaphore>,
    changed: watch::Sender<()>,
}
/// Explicit Session-owned supervisor registry, limited to 64 concurrent retained
/// registrations. This is not a per-Run history or native ownership limit.
/// Dropping all public handles still retains supervisors through actual joins.
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
            }),
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
    /// Wait for actual main observations/disposal by retained supervisors.
    /// Cached caller-owned results may still retain capacity; no native/resource
    /// retirement or complete ownership policy follows from this observation.
    pub async fn wait_for_supervision(&self) {
        let mut changed = self.inner.changed.subscribe();
        loop {
            if self
                .inner
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
            {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
    pub(super) fn retain<T: Send + 'static>(
        &self,
        task: JoinHandle<Result<TaskOutput<T>, PendingSuccessorError>>,
        control: Control,
        send: oneshot::Sender<Observation<T>>,
        capacity: OwnedSemaphorePermit,
        protected: Option<Arc<super::enrolled_initial::RetainedInitial>>,
    ) {
        let id = uuid::Uuid::new_v4();
        let registry = self.inner.clone();
        // The original task is parked; it cannot finish before insertion below.
        let supervisor = tokio::spawn(async move {
            let (disposition, value, error, replay) = match task.await {
                Ok(Ok(TaskOutput::Business(value))) => {
                    (OwnedMainDisposition::Returned, Some(value), None, None)
                }
                Ok(Ok(TaskOutput::BusinessFailed(error))) => {
                    (OwnedMainDisposition::Returned, None, Some(error), None)
                }
                Ok(Ok(TaskOutput::Stopped)) => (OwnedMainDisposition::NoDispatch, None, None, None),
                Ok(Ok(TaskOutput::Replayed(replay))) => {
                    (OwnedMainDisposition::NoDispatch, None, None, Some(replay))
                }
                Ok(Err(error)) => (OwnedMainDisposition::NoDispatch, None, Some(error), None),
                Err(error) if error.is_panic() => {
                    // Persist failure before safely disposing an untrusted payload.
                    control.finish(OwnedMainDisposition::Panicked);
                    super::super::discard_panic(error);
                    (OwnedMainDisposition::Panicked, None, None, None)
                }
                Err(_) => (OwnedMainDisposition::Aborted, None, None, None),
            };
            let main = control.binding().map(|bound| JoinedOwnedTask {
                activation: bound.activation().clone(),
                disposition,
                value,
            });
            // The same supervisor retains original outputs/resources even if
            // the bounded observation or its caller-owned retry is dropped.
            let physical = protected.clone();
            let protected = match protected {
                Some(retained) => Some(retained.after_join(disposition).await),
                None => None,
            };
            let pending = physical
                .as_ref()
                .is_some_and(|retained| retained.resources_pending());
            let (capacity, physical_capacity) = if protected.is_some() {
                if pending {
                    (None, Some(capacity))
                } else {
                    drop(capacity);
                    (None, None)
                }
            } else {
                (Some(capacity), None)
            };
            let observation = Observation {
                family: control.family(),
                main,
                disposition,
                error,
                replay,
                _capacity: capacity,
                protected,
            };
            control.finish(disposition);
            // Failed send owns/disposes the actual result here, not a detached main.
            if let Err(observation) = send.send(observation) {
                drop(observation);
            }
            if let Some(capacity) = physical_capacity {
                if let Some(retained) = &physical {
                    retained.join_original_resources().await;
                }
                drop(capacity);
            }
            registry
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            registry.changed.send_replace(());
        });
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, supervisor);
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
