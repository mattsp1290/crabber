//! Typed initial admission facade; private core retains the actual main.
pub(super) mod source;
use super::{
    DriverMainContext, Error, JoinedOwnedTask, OwnedMainDisposition, PendingDriverTasks,
    PendingSuccessorError, core,
};
use crate::{
    Store,
    session_owner::{
        driver::ExecutionDriverGrant,
        owned_scope::OwnedScopeActivation,
        preparation::{JoinedPreparation, PreparationAdmissionReceipt, PreparationCompletion},
    },
};
use crabber_core::{AdmissionReceipt, RunId, SessionContext, SessionId};
use std::{future::Future, sync::Arc};
/// Original initial-admission metadata; no fence or execution capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitialAdmissionReplay {
    Keyed(AdmissionReceipt),
    Unkeyed(PreparationAdmissionReceipt),
}
impl InitialAdmissionReplay {
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        match self {
            Self::Keyed(value) => &value.session_id,
            Self::Unkeyed(value) => &value.session_id,
        }
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        match self {
            Self::Keyed(value) => &value.run_id,
            Self::Unkeyed(value) => &value.run_id,
        }
    }
}
/// Validated fresh metadata, without a mutable duplicate routing identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialAdmissionMetadata {
    pub(super) receipt: InitialAdmissionReplay,
}
impl InitialAdmissionMetadata {
    #[must_use]
    pub fn receipt(&self) -> &InitialAdmissionReplay {
        &self.receipt
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        self.receipt.session_id()
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        self.receipt.run_id()
    }
}
/// Only the same actual task's acknowledged and original-source checked result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingInitialCompletion {
    Fresh(InitialAdmissionMetadata),
    Replayed(InitialAdmissionReplay),
}
/// Original fresh initial-driver grant and its once-bound activation.
#[derive(Clone)]
pub struct BoundInitialControl {
    pub(super) grant: ExecutionDriverGrant,
    pub(super) activation: OwnedScopeActivation,
}
impl BoundInitialControl {
    #[must_use]
    pub fn grant(&self) -> &ExecutionDriverGrant {
        &self.grant
    }
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.activation
    }
}
/// Signals/observes one original initial main; it cannot create native authority.
#[derive(Clone)]
pub struct PendingInitialControl(pub(super) core::Control);
impl PendingInitialControl {
    /// Stop new administrative calls/business dispatch, retaining pending IO.
    pub fn request_stop(&self) {
        self.0.request_stop();
    }
    /// Original metadata, never a fence or reconstructed grant.
    /// # Errors
    /// No acknowledged original completion or an inconsistent private family.
    pub async fn completion(&self) -> Result<PendingInitialCompletion, Error> {
        match self.0.completion().await? {
            core::Notice::Initial(value) => Ok(value),
            core::Notice::Successor(_) | core::Notice::Queue(_) => Err(Error::SemanticConflict),
        }
    }
    /// Actual once-bound initial grant.
    /// # Errors
    /// The original task ended without a binding, or has an inconsistent family.
    pub async fn bound(&self) -> Result<BoundInitialControl, Error> {
        match self.0.bound().await? {
            core::Binding::Initial(value) => Ok(*value),
            core::Binding::Successor(_) | core::Binding::Queue(_) => Err(Error::SemanticConflict),
        }
    }
}
/// Physical observation of the same original initial task, not full ownership.
pub struct PendingInitialObservation<T> {
    inner: core::Observation<T>,
}
impl<T> PendingInitialObservation<T> {
    #[must_use]
    pub fn bound_main(&self) -> Option<&JoinedOwnedTask<T>> {
        self.inner.main.as_ref()
    }
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.inner.disposition
    }
    #[must_use]
    pub fn replayed(&self) -> Option<&InitialAdmissionReplay> {
        match &self.inner.replay {
            Some(core::Replay::Initial(value)) => Some(value),
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
/// Park before initial completion creates a Run; dispatch only with a fresh permit.
pub struct PendingInitialTask<T> {
    pub(super) inner: core::Task<T>,
    observed: Option<PendingInitialObservation<T>>,
}
impl<T: Send + 'static> PendingInitialTask<T> {
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
            core::Source::Initial(source),
            business,
        )?;
        Ok(Self {
            inner,
            observed: None,
        })
    }
    #[must_use]
    pub fn control(&self) -> PendingInitialControl {
        PendingInitialControl(self.inner.control.clone())
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
    pub async fn join(&mut self) -> Result<&PendingInitialObservation<T>, Error> {
        if self.observed.is_none() {
            self.observed = Some(PendingInitialObservation {
                inner: self.inner.observation(core::Family::Initial).await?,
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
