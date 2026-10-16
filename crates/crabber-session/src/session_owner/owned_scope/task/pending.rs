//! One retained task spans preparation completion and its gated successor main.
mod execute;
mod retain;
mod state;
use super::{DriverMainContext, Error, JoinedOwnedTask, OwnedMainDisposition};
use crate::{
    Store,
    session_owner::preparation::{
        JoinedPreparation, PreparationCommitError, PreparationCompletion,
    },
};
use crabber_core::SessionContext;
pub use retain::PendingSuccessorTasks;
pub use state::{BoundSuccessorControl, PendingSuccessorControl};
use std::{future::Future, sync::Arc};
use tokio::sync::oneshot;

/// Administrative failure retained separately from actual physical disposition.
#[derive(Debug, thiserror::Error)]
pub enum PendingSuccessorError {
    #[error("preparation: {0}")]
    Completion(#[from] PreparationCommitError),
    #[error("successor: {0}")]
    Owner(#[from] Error),
}

/// Actual original main observation. Unbound tasks cannot supply an owned Join.
/// Physical completion alone certifies neither native retirement nor resources.
pub struct PendingSuccessorObservation<T> {
    main: Option<JoinedOwnedTask<T>>,
    disposition: OwnedMainDisposition,
    error: Option<PendingSuccessorError>,
    _capacity: tokio::sync::OwnedSemaphorePermit,
}
impl<T> PendingSuccessorObservation<T> {
    #[must_use]
    pub fn bound_main(&self) -> Option<&JoinedOwnedTask<T>> {
        self.main.as_ref()
    }
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.disposition
    }
    #[must_use]
    pub fn error(&self) -> Option<&PendingSuccessorError> {
        self.error.as_ref()
    }
    #[must_use]
    pub fn value(&self) -> Option<&T> {
        self.main.as_ref().and_then(JoinedOwnedTask::value)
    }
}
impl<T> Drop for PendingSuccessorObservation<T> {
    fn drop(&mut self) {
        // The original observation is already persisted. Hostile returned values
        // cannot unwind through the supervisor before its capacity is released.
        if let Some(main) = &mut self.main {
            retain::discard(main.value.take());
        }
    }
}

/// A parked registration owned by an explicit bounded Session supervisor.
/// Drop stops NEW administrative calls/dispatch, while the supervisor retains
/// and observes the same original task and disposes its actual result.
pub struct PendingSuccessorTask<T> {
    start: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<PendingSuccessorObservation<T>>,
    observed: Option<PendingSuccessorObservation<T>>,
    control: PendingSuccessorControl,
}
impl<T: Send + 'static> PendingSuccessorTask<T> {
    /// Register before native completion IO, then retain this handle before start.
    /// The fixed administrative sequence and business future share one task.
    /// Resources require separate actual cleanup; no complete Run policy follows.
    /// # Errors
    /// Refuses unsupported original preparation, mismatched context or capacity.
    pub fn register<F, W>(
        owner: &PendingSuccessorTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
        business: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(DriverMainContext, PreparationCompletion) -> W + Send + 'static,
        W: Future<Output = T> + Send + 'static,
    {
        let source = execute::Source::new(&context, &original)?;
        let capacity = owner.capacity()?;
        let control = PendingSuccessorControl::new();
        let (start, ready) = oneshot::channel();
        let (send, result) = oneshot::channel();
        let current = control.clone();
        let ingress = super::driver::MainIngress::new();
        let seal = ingress.guard();
        let task = tokio::spawn(async move {
            let _seal = seal;
            if ready.await.is_err() || current.stopped() {
                return Ok(None);
            }
            execute::run(
                store, context, original, source, &current, ingress, business,
            )
            .await
        });
        owner.retain(task, control.clone(), send, capacity);
        Ok(Self {
            start: Some(start),
            result,
            observed: None,
            control,
        })
    }
    #[must_use]
    pub fn control(&self) -> PendingSuccessorControl {
        self.control.clone()
    }
    /// Start only administrative work; business still needs a fresh permit.
    /// # Errors
    /// Refuses an already started/closed registration.
    pub fn start(&mut self) -> Result<(), Error> {
        self.start
            .take()
            .ok_or(Error::InvalidTransition)?
            .send(())
            .map_err(|()| Error::UnknownStoreOutcome)
    }
    /// Observe the actual original task without dropping it on caller cancellation.
    /// # Errors
    /// Reports lost supervision; never substitutes a synthetic physical Join.
    pub async fn join(&mut self) -> Result<&PendingSuccessorObservation<T>, Error> {
        drop(self.start.take());
        if self.observed.is_none() {
            self.observed = Some(
                (&mut self.result)
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?,
            );
        }
        self.observed.as_ref().ok_or(Error::UnknownStoreOutcome)
    }
}
impl<T> Drop for PendingSuccessorTask<T> {
    fn drop(&mut self) {
        self.control.request_stop();
    }
}

#[cfg(test)]
mod tests;
