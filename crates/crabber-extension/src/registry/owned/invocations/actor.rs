//! Private original provider actors: both actual parents retain the same task.
use super::{Source, Task, current};
use crate::{ExtensionError, ResourceRegistrationError, dispatch};
use crabber_providers::ProviderReleaseHolds;
use futures::future::BoxFuture;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Mutex as AsyncMutex, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Implementations are private original cells, never an arbitrary host callback.
pub(crate) trait ActorClose: Send + Sync {
    fn close(&self);
    fn observed(&self, success: bool);
    fn released(&self, success: bool) -> BoxFuture<'static, bool>;
}
struct Main {
    handle: AsyncMutex<Option<JoinHandle<bool>>>,
    result: Mutex<Option<bool>>,
    holds: Mutex<Option<ProviderReleaseHolds>>,
    member: Mutex<Option<super::MountMemberLease>>,
    closer: Arc<dyn ActorClose>,
}
impl Main {
    async fn join(&self) -> bool {
        let mut handle = self.handle.lock().await;
        if let Some(original) = handle.as_mut() {
            let success = match original.await {
                Ok(success) => success,
                Err(error) => {
                    if error.is_panic() {
                        dispatch::discard_panic_payload(error.into_panic());
                    }
                    false
                }
            };
            handle.take();
            self.closer.observed(success);
            // A failed original never drops its uncertain holds/member as an ACK.
            let holds = self.holds.lock().unwrap().take();
            let success = if success {
                match std::panic::catch_unwind(AssertUnwindSafe(|| drop(holds))) {
                    Ok(()) => true,
                    Err(payload) => {
                        dispatch::discard_panic_payload(payload);
                        false
                    }
                }
            } else {
                std::mem::forget(holds);
                false
            };
            let success = self.closer.released(success).await && success;
            if success {
                self.member.lock().unwrap().take();
            }
            *self.result.lock().unwrap() = Some(success);
        }
        self.result.lock().unwrap().unwrap_or(false)
    }
}
pub(super) struct Observation {
    main: Arc<Main>,
    observer: AsyncMutex<Option<JoinHandle<bool>>>,
    closer: Arc<dyn ActorClose>,
}
impl Observation {
    pub(super) fn close(&self) {
        self.closer.close();
    }
    pub(super) fn observer_finished(&self) -> bool {
        self.observer
            .try_lock()
            .is_ok_and(|handle| handle.as_ref().is_some_and(JoinHandle::is_finished))
    }
    pub(super) async fn join(&self, result: &Mutex<Option<bool>>) -> bool {
        let mut observer = self.observer.lock().await;
        if let Some(handle) = observer.as_mut() {
            let success = match handle.await {
                Ok(success) => success,
                Err(error) => {
                    if error.is_panic() {
                        dispatch::discard_panic_payload(error.into_panic());
                    }
                    false
                }
            };
            observer.take();
            *result.lock().unwrap() = Some(success);
        }
        // The observer must have joined that same original, not just returned.
        result.lock().unwrap().unwrap_or(false) && *self.main.result.lock().unwrap() == Some(true)
    }
}
/// Private retained actor identity; this is not a native permission or policy.
pub(crate) struct Actor {
    task: Arc<Task>,
}
impl Actor {
    pub(crate) fn close(&self) {
        self.task.cancel.cancel();
        self.task.actor.as_ref().unwrap().close();
    }
    pub(crate) async fn join(&self) -> Result<(), ExtensionError> {
        if self.task.join().await {
            Ok(())
        } else {
            Err(ExtensionError::ContextCleanupIncomplete)
        }
    }
}
impl Source {
    /// Retain both original handles and actual parents before opening work.
    /// The new family never falls back to unowned execution.
    pub(crate) async fn register_actor(
        &self,
        closer: Arc<dyn ActorClose>,
        construct: impl FnOnce(CancellationToken) -> BoxFuture<'static, bool> + Send + 'static,
    ) -> Result<Actor, ExtensionError> {
        let owner = current().ok_or(ResourceRegistrationError::Unsupported)?;
        let holds = ProviderReleaseHolds::capture_current();
        holds
            .check_admission_limit(64)
            .map_err(|_| ResourceRegistrationError::Capacity)?;
        let mount = self.0.upgrade().ok_or(ExtensionError::ContextOwnerClosed)?;
        let state = &mount
            .owned
            .as_ref()
            .ok_or(ExtensionError::ContextOwnerClosed)?
            .invocations;
        state.reap().await;
        owner.reap().await;
        let context = dispatch::CallbackTaskContext::capture(mount.id);
        let observer_context = dispatch::CallbackTaskContext::capture(mount.id);
        let cancel = CancellationToken::new();
        let task;
        {
            let active = mount.active.lock().unwrap();
            if !*active {
                return Err(ExtensionError::ContextOwnerClosed);
            }
            let mut entries = state.tasks.lock().unwrap();
            if entries.full() {
                return Err(ResourceRegistrationError::Capacity.into());
            }
            entries.admit()?;
            let (release, ready) = oneshot::channel();
            let actual = mount.clone();
            let body_owner = owner.clone();
            let body_cancel = cancel.clone();
            let body_holds = holds.clone();
            let handle = tokio::spawn(context.scope(body_holds.scope(move || async move {
                if ready.await.is_err() {
                    return true;
                }
                if !*actual.active.lock().unwrap()
                    || body_cancel.is_cancelled()
                    || body_owner.is_closed()
                {
                    return true;
                }
                super::observe(|| construct(body_cancel), CancellationToken::new())
                    .await
                    .is_ok_and(|output| output == Some(true))
            })));
            mount
                .leases
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let main = Arc::new(Main {
                handle: AsyncMutex::new(Some(handle)),
                result: Mutex::new(None),
                holds: Mutex::new(Some(holds)),
                member: Mutex::new(Some(super::MountMemberLease(Some(mount.clone())))),
                closer: closer.clone(),
            });
            // This is an observer of the actual original handle, not a new main.
            // Its handle is retained before either original work gate opens.
            let original = main.clone();
            let (observe_release, observe_ready) = oneshot::channel();
            let observer = tokio::spawn(observer_context.scope(async move {
                if observe_ready.await.is_err() {
                    return false;
                }
                original.join().await
            }));
            task = Arc::new(Task {
                join: AsyncMutex::new(None),
                result: Mutex::new(None),
                actor: Some(Observation {
                    main,
                    observer: AsyncMutex::new(Some(observer)),
                    closer,
                }),
                member: Mutex::new(None),
                cancel: cancel.clone(),
            });
            entries.tasks.push(task.clone());
            // On refusal the unopened original sees gate disposal. No FnOnce.
            let retained = owner.retain(task.clone());
            let _ = observe_release.send(());
            if retained.is_ok() {
                let _ = release.send(());
            } else {
                // The source still owns the original unopened task/observer.
                // Return its identity so the caller observes NoDispatch instead
                // of guessing that an already-published task never existed.
                drop(release);
            }
        }
        Ok(Actor { task })
    }
}
pub(crate) struct ActorWaiter {
    cancel: CancellationToken,
    closer: Arc<dyn ActorClose>,
    armed: bool,
}
impl ActorWaiter {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for ActorWaiter {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
            self.closer.close();
        }
    }
}
impl Actor {
    pub(crate) fn waiter(&self) -> ActorWaiter {
        ActorWaiter {
            cancel: self.task.cancel.clone(),
            closer: self.task.actor.as_ref().unwrap().closer.clone(),
            armed: true,
        }
    }
}

#[cfg(test)]
mod tests;
