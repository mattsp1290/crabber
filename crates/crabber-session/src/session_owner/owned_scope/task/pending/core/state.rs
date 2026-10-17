use super::super::{Error, OwnedMainDisposition};
use super::{Binding, Family, Notice};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ready,
    Calling,
    Dispatched,
    Finished(OwnedMainDisposition),
}
struct State {
    family: Family,
    stopped: bool,
    phase: Phase,
    bound: Option<Binding>,
    completion: Option<Notice>,
}
/// Signals/observes this original task. It cannot create a task, context or permit.
#[derive(Clone)]
pub(in crate::session_owner::owned_scope::task::pending) struct Control {
    state: Arc<Mutex<State>>,
    changed: watch::Sender<()>,
}
impl Control {
    pub(in crate::session_owner::owned_scope::task::pending) fn family(&self) -> Family {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .family
    }

    pub(in crate::session_owner::owned_scope::task::pending) fn new(family: Family) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                family,
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
    pub(in crate::session_owner::owned_scope::task::pending) fn request_stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
        self.changed.send_replace(());
    }
    /// Metadata from the same original task's validated completion ACK.
    /// # Errors
    /// The task ended without an acknowledged completion. No replacement IO runs.
    pub(in crate::session_owner::owned_scope::task::pending) async fn completion(
        &self,
    ) -> Result<Notice, Error> {
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
    pub(in crate::session_owner::owned_scope::task::pending) fn completed(
        &self,
        completion: Notice,
    ) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if completion.family() != state.family {
            return Err(Error::SemanticConflict);
        }
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
    pub(in crate::session_owner::owned_scope::task::pending) async fn bound(
        &self,
    ) -> Result<Binding, Error> {
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
    pub(in crate::session_owner::owned_scope::task::pending) fn finished_disposition(
        &self,
    ) -> Option<OwnedMainDisposition> {
        match self.state.lock().unwrap().phase {
            Phase::Finished(value) => Some(value),
            _ => None,
        }
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn enter_call(
        &self,
    ) -> Result<Option<CallAdmission>, Error> {
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
    pub(in crate::session_owner::owned_scope::task::pending) fn stopped(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn bind(
        &self,
        bound: Binding,
    ) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bound.family() != state.family {
            return Err(Error::SemanticConflict);
        }
        if state.bound.is_some() {
            return Err(Error::InvalidTransition);
        }
        state.bound = Some(bound);
        drop(state);
        self.changed.send_replace(());
        Ok(())
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn binding(&self) -> Option<Binding> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bound
            .clone()
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn dispatch(
        &self,
    ) -> Result<bool, Error> {
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
    pub(in crate::session_owner::owned_scope::task::pending) fn finish(
        &self,
        disposition: OwnedMainDisposition,
    ) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Finished(disposition);
        self.changed.send_replace(());
    }
}

// Local admission only: it retains no native authority or physical join proof.
pub(in crate::session_owner::owned_scope::task::pending) struct CallAdmission(Control);
impl Drop for CallAdmission {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Ready;
    }
}
