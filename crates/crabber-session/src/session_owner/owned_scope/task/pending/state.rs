use super::super::{Error, OwnedMainDisposition, OwnedScopeActivation};
use super::PendingSuccessorCompletion;
use crate::session_owner::driver::SuccessorExecutionDriverGrant;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// The original acknowledged grant and once-bound activation, never a source
/// certificate reconstructed from extrinsic Grant metadata. No context minting.
#[derive(Clone)]
pub struct BoundSuccessorControl {
    pub(super) grant: SuccessorExecutionDriverGrant,
    pub(super) activation: OwnedScopeActivation,
}
impl BoundSuccessorControl {
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.activation
    }
    #[must_use]
    pub fn grant(&self) -> &SuccessorExecutionDriverGrant {
        &self.grant
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ready,
    Calling,
    Dispatched,
    Finished(OwnedMainDisposition),
}
struct State {
    stopped: bool,
    phase: Phase,
    bound: Option<BoundSuccessorControl>,
    completion: Option<PendingSuccessorCompletion>,
}
/// Signals/observes this original task. It cannot create a task, context or permit.
#[derive(Clone)]
pub struct PendingSuccessorControl {
    state: Arc<Mutex<State>>,
    changed: watch::Sender<()>,
}
impl PendingSuccessorControl {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                stopped: false,
                phase: Phase::Ready,
                bound: None,
                completion: None,
            })),
            changed: watch::channel(()).0,
        }
    }
    /// Stop new administrative calls and business dispatch. An admitted Store
    /// future stays owned; already-dispatched business work is not aborted.
    /// Its Runtime owner must signal its independent cancellation/cleanup scope.
    pub fn request_stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
        self.changed.send_replace(());
    }
    /// Metadata from the same original task's validated completion ACK.
    /// # Errors
    /// The task ended without an acknowledged completion. No replacement IO runs.
    pub async fn completion(&self) -> Result<PendingSuccessorCompletion, Error> {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(completion) = &state.completion {
                    return Ok(completion.clone());
                }
                if matches!(state.phase, Phase::Finished(_)) {
                    return Err(Error::UnknownStoreOutcome);
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    }
    pub(super) fn completed(&self, completion: PendingSuccessorCompletion) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.completion.is_some() {
            return Err(Error::InvalidTransition);
        }
        state.completion = Some(completion);
        drop(state);
        self.changed.send_replace(());
        Ok(())
    }
    /// Observe a genuine once-binding; unbound termination returns an error.
    /// # Errors
    /// No acknowledged binding was retained before the actual task ended.
    pub async fn bound(&self) -> Result<BoundSuccessorControl, Error> {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(bound) = &state.bound {
                    return Ok(bound.clone());
                }
                if matches!(state.phase, Phase::Finished(_)) {
                    return Err(Error::UnknownStoreOutcome);
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    }
    #[cfg(test)]
    pub(super) fn finished_disposition(&self) -> Option<OwnedMainDisposition> {
        match self.state.lock().unwrap().phase {
            Phase::Finished(value) => Some(value),
            _ => None,
        }
    }
    pub(super) fn enter_call(&self) -> Result<Option<CallAdmission>, Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped {
            return Ok(None);
        }
        if state.phase != Phase::Ready {
            return Err(Error::InvalidTransition);
        }
        state.phase = Phase::Calling;
        Ok(Some(CallAdmission(self.clone())))
    }
    pub(super) fn stopped(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped
    }
    pub(super) fn bind(&self, bound: BoundSuccessorControl) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.bound.is_some() {
            return Err(Error::InvalidTransition);
        }
        state.bound = Some(bound);
        drop(state);
        self.changed.send_replace(());
        Ok(())
    }
    pub(super) fn binding(&self) -> Option<BoundSuccessorControl> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bound
            .clone()
    }
    pub(super) fn dispatch(&self) -> Result<bool, Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Ready {
            return Err(Error::InvalidTransition);
        }
        if state.stopped {
            return Ok(false);
        }
        state.phase = Phase::Dispatched;
        Ok(true)
    }
    pub(super) fn finish(&self, disposition: OwnedMainDisposition) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Finished(disposition);
        self.changed.send_replace(());
    }
}

// Local admission only: it retains no native authority or physical join proof.
pub(super) struct CallAdmission(PendingSuccessorControl);
impl Drop for CallAdmission {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Ready;
    }
}
