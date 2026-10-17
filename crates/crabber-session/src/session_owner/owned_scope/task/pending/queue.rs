//! Typed Queue admission facade; private core retains the actual main.
pub(super) mod source;
use super::initial::BoundInitialControl;
use super::{
    DriverMainContext, Error, JoinedOwnedTask, OwnedMainDisposition, PendingDriverTasks,
    PendingSuccessorError, core,
};
use crate::QueueReceipt;
use crate::{
    Store,
    session_owner::preparation::{JoinedPreparation, PreparationCompletion},
};
use crabber_core::{SessionContext, SessionId};
use std::{future::Future, sync::Arc};
/// Validated fresh metadata, without a mutable duplicate routing identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueAdmissionMetadata {
    pub(super) receipt: QueueReceipt,
}
impl QueueAdmissionMetadata {
    #[must_use]
    pub fn receipt(&self) -> &QueueReceipt {
        &self.receipt
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.receipt.session_id
    }
}
/// Only the same actual task's acknowledged and original-source checked result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingQueueCompletion {
    Fresh(QueueAdmissionMetadata),
    Replayed(QueueReceipt),
}
/// Signals/observes one original Queue main; it cannot create native authority.
#[derive(Clone)]
pub struct PendingQueueControl(pub(super) core::Control);
impl PendingQueueControl {
    /// Stop new administrative calls/business dispatch, retaining pending IO.
    pub fn request_stop(&self) {
        self.0.request_stop();
    }
    /// Original metadata, never a fence or reconstructed grant.
    /// # Errors
    /// No acknowledged original completion or an inconsistent private family.
    pub async fn completion(&self) -> Result<PendingQueueCompletion, Error> {
        match self.0.completion().await? {
            core::Notice::Queue(value) => Ok(value),
            _ => Err(Error::SemanticConflict),
        }
    }
    /// Actual once-bound initial grant.
    /// # Errors
    /// The original task ended without a binding, or has an inconsistent family.
    pub async fn bound(&self) -> Result<BoundInitialControl, Error> {
        match self.0.bound().await? {
            core::Binding::Queue(value) => Ok(*value),
            _ => Err(Error::SemanticConflict),
        }
    }
}
/// Physical observation of the same original Queue task, not full ownership.
pub struct PendingQueueObservation<T> {
    inner: core::Observation<T>,
}
impl<T> PendingQueueObservation<T> {
    #[must_use]
    pub fn bound_main(&self) -> Option<&JoinedOwnedTask<T>> {
        self.inner.main.as_ref()
    }
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.inner.disposition
    }
    #[must_use]
    pub fn replayed(&self) -> Option<&QueueReceipt> {
        match &self.inner.replay {
            Some(core::Replay::Queue(value)) => Some(value.as_ref()),
            _ => None,
        }
    }
    #[must_use]
    pub fn value(&self) -> Option<&T> {
        self.inner.main.as_ref().and_then(JoinedOwnedTask::value)
    }
    #[must_use]
    pub fn error(&self) -> Option<&PendingSuccessorError> {
        self.inner.error.as_ref()
    }
}
/// Park before Queue completion creates a Run; dispatch only with a fresh permit.
pub struct PendingQueueTask<T> {
    pub(super) inner: core::Task<T>,
    observed: Option<PendingQueueObservation<T>>,
}
impl<T: Send + 'static> PendingQueueTask<T> {
    /// Retain this original registration before starting native completion IO.
    /// # Errors
    /// Refuses unsupported original source, identity mismatch or shared capacity.
    pub fn register<F, W>(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
        business: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(DriverMainContext, PreparationCompletion) -> W + Send + 'static,
        W: Future<Output = T> + Send + 'static,
    {
        let source = source::Source::new(store.as_ref(), &context, &original)?;
        let inner = core::Task::register(
            owner,
            store,
            context,
            original,
            core::Source::Queue(source),
            business,
        )?;
        Ok(Self {
            inner,
            observed: None,
        })
    }
    #[must_use]
    pub fn control(&self) -> PendingQueueControl {
        PendingQueueControl(self.inner.control.clone())
    }
    /// Begin administrative work; business still requires a real fresh permit.
    /// # Errors
    /// The registration was already started or closed.
    pub fn start(&mut self) -> Result<(), Error> {
        self.inner.start()
    }
    /// Observe the SAME original task; cancellation keeps its original receiver.
    /// # Errors
    /// Lost supervision or inconsistent private family; no synthetic Join.
    pub async fn join(&mut self) -> Result<&PendingQueueObservation<T>, Error> {
        if self.observed.is_none() {
            self.observed = Some(PendingQueueObservation {
                inner: self.inner.observation(core::Family::Queue).await?,
            });
        }
        self.observed.as_ref().ok_or(Error::UnknownStoreOutcome)
    }
    /// Move the original administrative error once, after physical observation.
    /// # Errors
    /// Same finite supervision failures as join.
    pub async fn take_error(&mut self) -> Result<Option<PendingSuccessorError>, Error> {
        self.join().await?;
        Ok(self
            .observed
            .as_mut()
            .ok_or(Error::UnknownStoreOutcome)?
            .inner
            .error
            .take())
    }
}
