//! Registration retains the actual acknowledgement task independently of callers.
mod attempt;
use super::dependencies::Dependency;
use crate::{
    ExtensionError, SessionServiceDependencies, SessionServiceDependencyAdmission as Admission,
    SessionServiceDependencyError as Error, SessionServiceDependencyHandle,
};
use attempt::Attempt;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
pub(super) struct Binding {
    pub context: crabber_core::SessionContext,
    pub services: SessionServiceDependencies,
}

#[derive(Default, Clone)]
enum State {
    #[default]
    Legacy,
    Active(SessionServiceDependencyHandle),
    Uncertain(Option<SessionServiceDependencyHandle>),
    NoGrant,
    Closed,
}
#[derive(Default)]
pub(super) struct Registration {
    pending: Mutex<Option<Arc<Attempt>>>,
    close_gate: AsyncMutex<()>,
    state: Mutex<State>,
    abnormal_exit: std::sync::atomic::AtomicBool,
}
impl Registration {
    pub(super) fn is_registered(&self) -> bool {
        matches!(
            *self.state.lock().unwrap(),
            State::Active(_) | State::Uncertain(_) | State::Closed
        )
    }
    pub(super) fn is_uncertain(&self) -> bool {
        matches!(*self.state.lock().unwrap(), State::Uncertain(_))
    }

    pub(super) async fn acquire(
        &self,
        services: &SessionServiceDependencies,
        dependency: Arc<Dependency>,
    ) -> Result<(), ExtensionError> {
        loop {
            if self.abnormal_exit.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(ExtensionError::ContextInitializationIncomplete);
            }
            let (attempt, created) = {
                let mut pending = self.pending.lock().unwrap();
                if let Some(attempt) = pending.as_ref() {
                    (attempt.clone(), false)
                } else {
                    if dependency.is_sealed() {
                        return Err(ExtensionError::ContextOwnerClosed);
                    }
                    let previous = self.state.lock().unwrap().clone();
                    *self.state.lock().unwrap() = State::Uncertain(retained(&previous));
                    let attempt = Arc::new(Attempt::default());
                    let services = services.clone();
                    let owned = dependency.clone();
                    attempt.retain(tokio::spawn(async move {
                        owned
                            .registration
                            .perform(&services, owned.clone(), previous)
                            .await
                    }));
                    *pending = Some(attempt.clone());
                    (attempt, true)
                }
            };
            let result = attempt.join().await;
            self.record_abnormal_exit(&result);
            let mut pending = self.pending.lock().unwrap();
            if pending
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt))
            {
                *pending = None;
            }
            drop(pending);
            if created {
                return result;
            }
            // Each caller's host binding must perform its own current access check.
            result?;
        }
    }

    async fn perform(
        &self,
        services: &SessionServiceDependencies,
        dependency: Arc<Dependency>,
        previous: State,
    ) -> Result<(), ExtensionError> {
        let task = dependency.clone();
        let outcome = services
            .register(dependency.generation().clone(), move |context| async move {
                tokio::select! {
                    () = context.cancel.cancelled() => {},
                    () = task.closing() => {},
                }
                task.seal();
                task.wait_references().await;
                Ok(())
            })
            .await;
        let mut state = self.state.lock().unwrap();
        match outcome {
            Ok(Admission::Acquired(handle)) => {
                *state = State::Active(retained(&previous).unwrap_or(handle));
                if dependency.is_sealed() {
                    Err(ExtensionError::ContextOwnerClosed)
                } else {
                    Ok(())
                }
            }
            Ok(Admission::Uncertain(handle)) => {
                *state = State::Uncertain(Some(retained(&previous).unwrap_or(handle)));
                Err(Error::UnknownOutcome.into())
            }
            Ok(Admission::Replayed) => {
                let handle = retained(&previous);
                *state = State::Uncertain(handle);
                Err(Error::UnknownOutcome.into())
            }
            Ok(Admission::Cancelled) => {
                *state = match retained(&previous) {
                    Some(handle) => State::Uncertain(Some(handle)),
                    None => State::NoGrant,
                };
                Err(ExtensionError::ContextOwnerClosed)
            }
            Err(error) => {
                *state = match (&previous, error) {
                    (
                        State::Legacy | State::NoGrant,
                        Error::Unsupported | Error::Rejected | Error::Capacity | Error::Deleted,
                    ) => State::NoGrant,
                    _ => State::Uncertain(retained(&previous)),
                };
                Err(error.into())
            }
        }
    }

    fn record_abnormal_exit(&self, result: &Result<(), ExtensionError>) {
        if matches!(result, Err(ExtensionError::ContextInitializationIncomplete)) {
            self.abnormal_exit
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    pub(super) async fn close_and_join(&self, bound: Duration) -> Result<(), ExtensionError> {
        let _gate = self.close_gate.lock().await;
        let pending = self.pending.lock().unwrap().clone();
        if let Some(attempt) = pending {
            // Even an unknown RPC outcome must finish its actual local task before
            // an original retained control can certify its own close.
            let outcome = attempt.join().await;
            self.record_abnormal_exit(&outcome);
        }
        if self.abnormal_exit.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ExtensionError::ContextInitializationIncomplete);
        }
        let state = self.state.lock().unwrap().clone();
        let handle = match state {
            State::Legacy | State::NoGrant | State::Closed => return Ok(()),
            State::Active(handle) | State::Uncertain(Some(handle)) => handle,
            State::Uncertain(None) => return Err(Error::UnknownOutcome.into()),
        };
        let report = handle.close_and_join(bound).await?;
        if !report.sealed || report.pending != 0 || !report.failures.is_empty() {
            return Err(ExtensionError::ContextCleanupIncomplete);
        }
        *self.state.lock().unwrap() = State::Closed;
        Ok(())
    }
}
fn retained(state: &State) -> Option<SessionServiceDependencyHandle> {
    match state {
        State::Active(handle) | State::Uncertain(Some(handle)) => Some(handle.clone()),
        _ => None,
    }
}
