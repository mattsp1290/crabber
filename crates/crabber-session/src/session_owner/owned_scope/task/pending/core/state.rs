use super::super::{Error, OwnedMainDisposition};
use super::{Binding, Family, Notice, OwnedScopeActivation};
use std::sync::{Arc, Mutex, Weak};
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
    resources: Option<crabber_extension::ToolResourceOwner>,
    fixed: Option<Weak<super::super::fixed_memory::RetainedFixedController>>,
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
                resources: None,
                fixed: None,
            })),
            changed: watch::channel(()).0,
        }
    }
    /// Stop new administrative calls and business dispatch. An admitted Store
    /// future stays owned; already-dispatched business work is not aborted.
    /// Its Runtime owner must signal its independent cancellation/cleanup scope.
    pub(in crate::session_owner::owned_scope::task::pending) fn request_stop(&self) {
        let (resources, fixed) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stopped = true;
            (state.resources.clone(), state.fixed.clone())
        };
        // Closing can notify actual resource observers; never do that under the
        // administrative mutex or substitute a signal for their actual joins.
        if let Some(resources) = resources {
            resources.close();
        }
        if let Some(fixed) = fixed.and_then(|owner| owner.upgrade()) {
            fixed.request_stop();
        }
        self.changed.send_replace(());
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn install_fixed_controller(
        &self,
        owner: &Arc<super::super::fixed_memory::RetainedFixedController>,
    ) -> Result<(), Error> {
        let stopped = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.family != Family::FixedMemoryController
                || state.phase != Phase::Ready
                || state.fixed.is_some()
            {
                return Err(Error::InvalidTransition);
            }
            state.fixed = Some(Arc::downgrade(owner));
            state.stopped
        };
        if stopped {
            owner.request_stop();
        }
        Ok(())
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
        Ok(Some(CallAdmission {
            control: self.clone(),
            resume: Phase::Ready,
        }))
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn install_resources(
        &self,
        resources: &crabber_extension::ToolResourceOwner,
    ) -> Result<(), Error> {
        let stopped = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.family != Family::EnrolledInitial
                || state.phase != Phase::Ready
                || state.resources.is_some()
            {
                return Err(Error::InvalidTransition);
            }
            state.resources = Some(resources.clone());
            state.stopped
        };
        if stopped {
            resources.close();
        }
        Ok(())
    }
    // Private enrolled calls preserve the once-dispatched phase. A call loan
    // cannot reopen dispatch or be duplicated/reentered while its literal lives.
    pub(in crate::session_owner::owned_scope::task::pending) fn enter_consumer_call(
        &self,
    ) -> Result<Option<CallAdmission>, Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped {
            return Ok(None);
        }
        if !matches!(state.phase, Phase::Ready | Phase::Dispatched) {
            return Err(Error::InvalidTransition);
        }
        let resume = state.phase;
        state.phase = Phase::Calling;
        Ok(Some(CallAdmission {
            control: self.clone(),
            resume,
        }))
    }
    // A freshly observed Begin is required even for this narrowly stopped
    // terminal loan. Its operation/authority/drained witness is checked again
    // by the private consumer ingress and the actual native publisher.
    pub(in crate::session_owner::owned_scope::task::pending) fn enter_enrolled_cleanup(
        &self,
        _begun: &super::super::enrolled_initial::FreshBegun,
    ) -> Result<CallAdmission, Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Dispatched {
            return Err(Error::InvalidTransition);
        }
        state.phase = Phase::Calling;
        Ok(CallAdmission {
            control: self.clone(),
            resume: Phase::Dispatched,
        })
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
    pub(in crate::session_owner::owned_scope::task::pending) fn dispatch_consumer(
        &self,
        permit: crate::session_owner::owned_scope::OwnedDispatchPermit,
        expected: &OwnedScopeActivation,
    ) -> Result<bool, Error> {
        let crate::session_owner::owned_scope::OwnedDispatchPermit { activation } = permit;
        if activation != *expected || self.family() != Family::EnrolledInitial {
            return Err(Error::IdentityMismatch);
        }
        self.dispatch()
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
pub(in crate::session_owner::owned_scope::task::pending) struct CallAdmission {
    control: Control,
    resume: Phase,
}
impl Drop for CallAdmission {
    fn drop(&mut self) {
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Actual completion is absorbing even if a local guard is disposed late.
        if state.phase == Phase::Calling {
            state.phase = self.resume;
        }
    }
}
