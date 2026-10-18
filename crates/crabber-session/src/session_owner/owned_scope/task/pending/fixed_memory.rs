//! Closed library Memory/SystemClock intake. Generic controls remain separate.
mod builder;
mod control;
mod controller;
use super::{
    PendingInitialProducerObservation, PendingInitialProducerStatus, PendingSuccessorError, core,
};
use crate::session_owner::preparation::PreparationCommitError;
use crate::session_owner::{SessionOwnerError as Error, owned_scope::OwnedMainDisposition};
pub(crate) use control::FixedMemoryInitialControl;
pub(super) use controller::RetainedFixedController;
use crabber_core::{AdmissionKey, InputEnvelope, SessionContext};
use crabber_extension::LibraryScriptedSource;
use std::sync::Arc;

/// Owned semantic input for an existing managed Session. The typed input's
/// expected revision is the sole admission CAS; this path does not create it.
pub struct FixedMemoryInitialInput {
    pub context: SessionContext,
    pub title: String,
    pub input: InputEnvelope,
    pub key: Option<AdmissionKey>,
}

/// The actual shared library MemoryStore/SystemClock and retained task registry.
/// No injected Store, clock, factory or prepared output can enter this host.
#[derive(Clone)]
pub struct FixedMemoryInitialHost {
    control: Arc<FixedMemoryInitialControl>,
}
impl Default for FixedMemoryInitialHost {
    fn default() -> Self {
        Self::new()
    }
}
impl FixedMemoryInitialHost {
    #[must_use]
    pub fn new() -> Self {
        Self {
            control: Arc::new(FixedMemoryInitialControl::new()),
        }
    }
    /// Management access to this actual root; it cannot replace its control or clock.
    #[must_use]
    pub fn store(&self) -> &crate::MemoryStore {
        self.control.actual_store()
    }
    /// Register the original parked controller before any Session/preparation I/O.
    /// The pristine source is consumed; only its genuine inner producer can obtain
    /// native Run grants. Capacity refusal never waits for another controller slot.
    /// # Errors
    /// Refuses unsupported input, missing runtime or retained registration capacity.
    pub fn register_scripted(
        &self,
        input: FixedMemoryInitialInput,
        source: LibraryScriptedSource,
    ) -> Result<FixedMemoryInitialTask, PreparationCommitError> {
        self.register_inner(
            input,
            source,
            #[cfg(test)]
            None,
        )
    }
    #[cfg(test)]
    pub(crate) fn register_scripted_test(
        &self,
        input: FixedMemoryInitialInput,
        source: LibraryScriptedSource,
        hooks: Arc<super::fixed_preparation::FixedInitialTestHooks>,
    ) -> Result<FixedMemoryInitialTask, PreparationCommitError> {
        self.register_inner(input, source, Some(hooks))
    }
    fn register_inner(
        &self,
        input: FixedMemoryInitialInput,
        source: LibraryScriptedSource,
        #[cfg(test)] hooks: Option<Arc<super::fixed_preparation::FixedInitialTestHooks>>,
    ) -> Result<FixedMemoryInitialTask, PreparationCommitError> {
        tokio::runtime::Handle::try_current().map_err(|_| Error::Unsupported)?;
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        builder::validate(&input, &source, &mut proof)?;
        let retained = Arc::new(RetainedFixedController::new(self.control.clone()));
        #[cfg(test)]
        {
            *retained.hooks.lock().unwrap() = hooks;
        }
        let current = retained.clone();
        let task = core::Task::register_body(
            self.control.tasks(),
            core::Family::FixedMemoryController,
            Some(core::RetainedOwner::FixedMemoryController(retained.clone())),
            move |control, _| controller::run(current, control, input, source),
        )?;
        Ok(FixedMemoryInitialTask {
            task,
            retained,
            observed: None,
        })
    }
    /// Wait only for actual retained original supervision; this is not a report
    /// that all host persistence or unrelated Session resources were disposed.
    pub async fn wait_for_supervision(&self) {
        self.control.tasks().wait_for_supervision().await;
    }
}

/// Signals only the actual original controller and its actual created child.
#[derive(Clone)]
pub struct FixedMemoryInitialTaskControl {
    retained: Arc<RetainedFixedController>,
    control: core::Control,
}
impl FixedMemoryInitialTaskControl {
    pub fn request_stop(&self) {
        self.control.request_stop();
    }
    #[must_use]
    pub fn status(&self) -> Option<PendingInitialProducerStatus> {
        self.retained.status()
    }
}

pub struct FixedMemoryInitialTask {
    task: core::Task<()>,
    retained: Arc<RetainedFixedController>,
    observed: Option<FixedMemoryInitialObservation>,
}
impl FixedMemoryInitialTask {
    #[must_use]
    pub fn control(&self) -> FixedMemoryInitialTaskControl {
        FixedMemoryInitialTaskControl {
            retained: self.retained.clone(),
            control: self.task.control.clone(),
        }
    }
    /// Start the SAME original parked task.
    /// # Errors
    /// The original start gate has already closed or been consumed.
    pub fn start(&mut self) -> Result<(), Error> {
        self.task.start()
    }
    /// Observe the real controller and inner original. No public fence or permit
    /// is returned, including when a paired-close acknowledgement was lost.
    /// # Errors
    /// Refuses missing original supervision or a mismatched private task family.
    pub async fn join(&mut self) -> Result<&mut FixedMemoryInitialObservation, Error> {
        if self.observed.is_none() {
            let outer = self
                .task
                .observation(core::Family::FixedMemoryController)
                .await?;
            let (inner, cleanup_error) = self.retained.take_output();
            self.observed = Some(FixedMemoryInitialObservation {
                outer,
                inner,
                cleanup_error,
            });
        }
        self.observed.as_mut().ok_or(Error::UnknownStoreOutcome)
    }
}
impl Drop for FixedMemoryInitialTask {
    fn drop(&mut self) {
        self.task.control.request_stop();
    }
}

pub struct FixedMemoryInitialObservation {
    outer: core::Observation<()>,
    inner: Option<PendingInitialProducerObservation>,
    cleanup_error: Option<PendingSuccessorError>,
}
impl FixedMemoryInitialObservation {
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.outer.disposition
    }
    #[must_use]
    pub fn status(&self) -> Option<&PendingInitialProducerStatus> {
        self.inner
            .as_ref()
            .map(PendingInitialProducerObservation::status)
    }
    #[must_use]
    pub fn observation_failed(&self) -> bool {
        self.cleanup_error.is_some()
            || matches!(
                self.outer.disposition,
                OwnedMainDisposition::Panicked | OwnedMainDisposition::Aborted
            )
            || self
                .inner
                .as_ref()
                .is_some_and(PendingInitialProducerObservation::observation_failed)
    }
    #[must_use]
    pub fn error(&self) -> Option<&PendingSuccessorError> {
        self.cleanup_error
            .as_ref()
            .or(self.outer.error.as_ref())
            .or_else(|| {
                self.inner
                    .as_ref()
                    .and_then(PendingInitialProducerObservation::error)
            })
    }
    pub fn take_retry(&mut self) -> Option<super::LocallyJoinedInitialSeal> {
        self.inner
            .as_mut()
            .and_then(PendingInitialProducerObservation::take_retry)
    }
}

#[cfg(test)]
mod tests;
