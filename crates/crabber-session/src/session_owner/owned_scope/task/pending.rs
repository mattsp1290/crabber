//! One retained task spans preparation completion and its gated successor main.
//!
//! Initial, Queue and all eight successor tags share the 64-task registry.
//! Continuation locators come only from the acknowledged transaction publisher,
//! then undergo exact original inspection.
//! Cancellation/unknown acknowledgments never replay business work or remint
//! authority. Native source and local task observations do not prove complete
//! native enrollment, resource policy or whole-Run main ownership.
mod core;
mod enrolled_initial;
mod execute;
mod initial;
pub(crate) use enrolled_initial::{
    DrainedInitialResources, InitialResourceAssociation, InitialRetirementView,
    InitialRetirementWitness, NativeConsumerParts,
};
#[cfg(test)]
pub(crate) use enrolled_initial::{InitialOperationUsage, InitialProofCaps, InitialProofUsage};
pub use enrolled_initial::{
    LocallyJoinedInitialSeal, PendingInitialNativeCall, PendingInitialNativeReply,
    PendingInitialProducer, PendingInitialProducerControl, PendingInitialProducerObservation,
    PendingInitialProducerStatus,
};
pub(crate) use enrolled_initial::{NativeInitialOperation, NativeInitialParts};
pub use enrolled_initial::{PendingInitialConsumerNativeCall, PendingInitialConsumerNativeReply};
mod queue;
pub use queue::{
    PendingQueueCompletion, PendingQueueControl, PendingQueueObservation, PendingQueueTask,
    QueueAdmissionMetadata,
};
mod retain;
mod state;
use super::{DriverMainContext, Error, JoinedOwnedTask, OwnedMainDisposition};
use crate::session_owner::driver::SuccessorExecutionDriverOrigin as Origin;
use crate::{
    Store,
    session_owner::preparation::{
        JoinedPreparation, PreparationCommitError, PreparationCompletion,
    },
};
use crabber_core::SessionContext;
pub use retain::PendingDriverTasks;
/// Compatibility name for the shared initial/successor supervisor registry.
pub type PendingSuccessorTasks = PendingDriverTasks;
pub use initial::{
    BoundInitialControl, InitialAdmissionMetadata, InitialAdmissionReplay,
    PendingInitialCompletion, PendingInitialControl, PendingInitialObservation, PendingInitialTask,
};
pub use state::{BoundSuccessorControl, PendingSuccessorControl};
use std::{future::Future, sync::Arc};

/// Exact metadata returned by replay in the same original task. It carries no
/// fence, activation permit or replacement native capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingSuccessorReplay {
    Admission(crabber_core::AdmissionReceipt),
    Expired(crate::session_owner::preparation::ExpiredRunReceipt),
    ApprovalClaim(Box<crate::ApprovalReceipt>),
    ApprovalRecovery(Box<crate::ApprovalReceipt>),
    ToolClaim(Box<crate::ToolContinuationReceipt>),
    ToolRecovery(Box<crate::ToolContinuationReceipt>),
    FrontendClaim(Box<crate::ToolContinuationReceipt>),
    FrontendRecovery(Box<crate::ToolContinuationReceipt>),
}
/// Safe metadata from the original acknowledged completion, before registration.
/// It contains no fence, grant or business dispatch permission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingSuccessorCompletion {
    Fresh(PendingSuccessorReplay),
    Replayed(PendingSuccessorReplay),
}
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
    inner: core::Observation<T>,
}
impl<T> PendingSuccessorObservation<T> {
    #[must_use]
    pub fn bound_main(&self) -> Option<&JoinedOwnedTask<T>> {
        self.inner.main.as_ref()
    }
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.inner.disposition
    }
    #[must_use]
    pub fn replayed(&self) -> Option<&PendingSuccessorReplay> {
        match &self.inner.replay {
            Some(core::Replay::Successor(value)) => Some(value),
            _ => None,
        }
    }
    #[must_use]
    pub fn error(&self) -> Option<&PendingSuccessorError> {
        self.inner.error.as_ref()
    }
    #[must_use]
    pub fn value(&self) -> Option<&T> {
        self.inner.main.as_ref().and_then(JoinedOwnedTask::value)
    }
}

/// A parked registration owned by an explicit bounded Session supervisor.
/// Drop stops NEW administrative calls/dispatch, while the supervisor retains
/// and observes the same original task and disposes its actual result.
pub struct PendingSuccessorTask<T> {
    inner: core::Task<T>,
    observed: Option<PendingSuccessorObservation<T>>,
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
        if !matches!(
            source.origin(),
            Origin::AdmissionRecovery | Origin::ExpiredRun
        ) && !store.supports_pending_execution_driver_origin(source.origin())
        {
            return Err(Error::Unsupported);
        }
        let inner = core::Task::register(
            owner,
            store,
            context,
            original,
            core::Source::Successor(source),
            business,
        )?;
        Ok(Self {
            inner,
            observed: None,
        })
    }
    #[must_use]
    pub fn control(&self) -> PendingSuccessorControl {
        PendingSuccessorControl(self.inner.control.clone())
    }
    /// Start only administrative work; business still needs a fresh permit.
    /// # Errors
    /// Refuses an already started/closed registration.
    pub fn start(&mut self) -> Result<(), Error> {
        self.inner.start()
    }
    /// Move the original administrative error exactly once after actual observation.
    /// This returns no source, grant or dispatch capability.
    /// # Errors
    /// Reports the same supervision errors as join.
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
    /// Observe the actual original task without dropping it on caller cancellation.
    /// # Errors
    /// Reports lost supervision; never substitutes a synthetic physical Join.
    pub async fn join(&mut self) -> Result<&PendingSuccessorObservation<T>, Error> {
        if self.observed.is_none() {
            self.observed = Some(PendingSuccessorObservation {
                inner: self.inner.observation(core::Family::Successor).await?,
            });
        }
        self.observed.as_ref().ok_or(Error::UnknownStoreOutcome)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod approval_tests;

#[cfg(test)]
mod tool_tests;

#[cfg(test)]
pub(crate) use enrolled_initial::initial_consumer_fixture;

#[cfg(test)]
pub(crate) use enrolled_initial::{ConsumerProofCaps, ConsumerProofUsage};
