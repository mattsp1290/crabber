//! One retained cleanup pass, observed through its actual join handle.
use super::super::{ContextualPlanProvider, Inner};
use crate::ExtensionError;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle, time::Instant};

type Outcome = Result<(), ExtensionError>;
#[derive(Default)]
pub(in crate::contextual) struct CloseCoordinator {
    current: Mutex<Option<Arc<Pass>>>,
    abnormal: Arc<AtomicBool>,
}
struct Pass {
    deadline: Instant,
    task: AsyncMutex<Option<JoinHandle<Outcome>>>,
    result: Mutex<Option<Outcome>>,
}
/// Observation of the original contextual cleanup; no plan or execution authority.
#[derive(Clone)]
pub struct ContextualCloseObservation {
    provider: Weak<Inner>,
    pass: Arc<Pass>,
    abnormal: Arc<AtomicBool>,
}
impl CloseCoordinator {
    pub(super) fn current(
        &self,
        provider: &ContextualPlanProvider,
    ) -> Option<ContextualCloseObservation> {
        let current = self.current.lock().unwrap();
        let pass = current.as_ref()?;
        let result = pass.result.lock().unwrap();
        if result.as_ref().is_some_and(Result::is_err) && !self.abnormal.load(Ordering::Acquire) {
            return None;
        }
        Some(ContextualCloseObservation {
            provider: Arc::downgrade(&provider.inner),
            pass: pass.clone(),
            abnormal: self.abnormal.clone(),
        })
    }
    pub(super) fn retain(
        &self,
        task: JoinHandle<Outcome>,
        provider: &ContextualPlanProvider,
        deadline: Instant,
    ) -> ContextualCloseObservation {
        let pass = Arc::new(Pass {
            deadline,
            task: AsyncMutex::new(Some(task)),
            result: Mutex::new(None),
        });
        *self.current.lock().unwrap() = Some(pass.clone());
        ContextualCloseObservation {
            provider: Arc::downgrade(&provider.inner),
            pass,
            abnormal: self.abnormal.clone(),
        }
    }
}
impl ContextualCloseObservation {
    /// Cached join of this original observation pass. Absence remains pending.
    /// A later successful refresh returned by `observe_until` does not rewrite
    /// this historical result.
    /// # Panics
    /// Panics if a previous internal panic poisoned the observation lock.
    #[must_use]
    pub fn result(&self) -> Option<Result<(), ExtensionError>> {
        if self.abnormal.load(Ordering::Acquire) {
            return Some(Err(ExtensionError::ContextCleanupIncomplete));
        }
        self.pass.result.lock().unwrap().clone()
    }
    /// Observe the retained handle within an absolute caller deadline.
    /// An already joined earlier timeout may refresh observation once, using
    /// the same physical cleanup owners and the remaining caller deadline.
    /// Canceling this waiter never removes its handle or original cleanup work.
    /// # Errors
    /// Returns the actual cleanup error or incomplete observation at the deadline.
    pub async fn observe_until(&self, deadline: Instant) -> Result<(), ExtensionError> {
        let result = tokio::time::timeout_at(deadline, self.observe())
            .await
            .unwrap_or(Err(ExtensionError::ContextCleanupIncomplete));
        // An earlier caller may have left an actual joined timeout result.
        // Refresh only that observation once; physical cleanup remains original.
        if result.is_err()
            && self.result().is_some_and(|result| result.is_err())
            && !self.abnormal.load(Ordering::Acquire)
            && self.pass.deadline < deadline
            && Instant::now() < deadline
            && let Some(inner) = self.provider.upgrade()
        {
            let provider = ContextualPlanProvider { inner };
            let next =
                provider.begin_close_all(deadline.saturating_duration_since(Instant::now()))?;
            return tokio::time::timeout_at(deadline, next.observe())
                .await
                .unwrap_or(Err(ExtensionError::ContextCleanupIncomplete));
        }
        result
    }
    async fn observe(&self) -> Outcome {
        if let Some(result) = self.result() {
            return result;
        }
        let mut task = self.pass.task.lock().await;
        if let Some(result) = self.result() {
            return result;
        }
        let joined = task.as_mut().expect("retained contextual cleanup").await;
        match joined {
            Ok(result) => {
                *self.pass.result.lock().unwrap() = Some(result.clone());
                task.take();
                result
            }
            Err(error) => {
                self.abnormal.store(true, Ordering::Release);
                *self.pass.result.lock().unwrap() =
                    Some(Err(ExtensionError::ContextCleanupIncomplete));
                task.take();
                drop(task);
                if error.is_panic() {
                    crate::dispatch::discard_panic_payload(error.into_panic());
                }
                Err(ExtensionError::ContextCleanupIncomplete)
            }
        }
    }
}
#[cfg(test)]
mod tests;
