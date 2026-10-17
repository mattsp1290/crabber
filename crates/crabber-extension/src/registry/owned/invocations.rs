//! Actual callback mains retained by their original concrete owned mount.
use super::super::{Mount, MountMemberLease};
use crate::{ExtensionError, dispatch};
use futures::{FutureExt, future::BoxFuture};
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{Mutex as AsyncMutex, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

mod caller;
mod inline;
pub(crate) use caller::current;
pub use caller::{
    CallbackInvocationCapability, CallbackInvocationOwner, with_callback_invocations,
};

const MAX_ACTIVE: usize = 64;

#[derive(Default)]
pub(super) struct State {
    tasks: Mutex<Entries>,
    changed: tokio::sync::Notify,
}
#[derive(Default)]
struct Entries {
    tasks: Vec<Arc<Task>>,
    inline: Vec<Arc<inline::Call>>,
    sealed: bool,
}
impl Entries {
    fn full(&self) -> bool {
        self.tasks.len() + self.inline.len() >= MAX_ACTIVE
    }
    fn reap_observed(&mut self) {
        self.tasks
            .retain(|task| *task.result.lock().unwrap() != Some(true));
        self.inline.retain(|call| call.result() != Some(true));
    }
}
struct Task {
    join: AsyncMutex<Option<JoinHandle<bool>>>,
    result: Mutex<Option<bool>>,
    // Release only AFTER observing the actual task join, never on output send.
    member: Mutex<Option<MountMemberLease>>,
    cancel: CancellationToken,
}
impl Task {
    async fn join(&self) -> bool {
        let mut join = self.join.lock().await;
        if let Some(handle) = join.as_mut() {
            let success = match handle.await {
                Ok(success) => success,
                Err(error) => {
                    if error.is_panic() {
                        dispatch::discard_panic_payload(error.into_panic());
                    }
                    false
                }
            };
            *self.result.lock().unwrap() = Some(success);
            join.take();
            self.member.lock().unwrap().take();
        }
        self.result.lock().unwrap().unwrap_or(false)
    }
}
/// Exact original private roster, retained independently of mount lifetime.
#[derive(Clone)]
pub(crate) struct OriginalRoster {
    state: Arc<State>,
    mount: Weak<Mount>,
}
impl OriginalRoster {
    pub(crate) fn seal(&self) {
        if let Some(mount) = self.mount.upgrade() {
            let mut active = mount.active.lock().unwrap();
            *active = false;
            self.state.close();
        } else {
            self.state.close();
        }
    }

    pub(crate) async fn sealed_join(&self) -> Result<(), ExtensionError> {
        loop {
            let changed = self.state.changed.notified();
            if self.state.tasks.lock().unwrap().sealed {
                break;
            }
            changed.await;
        }
        if self.state.join().await {
            Ok(())
        } else {
            Err(ExtensionError::ContextCleanupIncomplete)
        }
    }
}
impl State {
    pub(super) fn close(&self) {
        let mut entries = self.tasks.lock().unwrap();
        entries.sealed = true;
        for task in &entries.tasks {
            task.cancel.cancel();
        }
        self.changed.notify_waiters();
    }
    pub(super) async fn join(&self) -> bool {
        let (tasks, inline) = {
            let entries = self.tasks.lock().unwrap();
            (entries.tasks.clone(), entries.inline.clone())
        };
        let mut success = true;
        for task in tasks {
            success &= task.join().await;
        }
        for call in inline {
            success &= call.returned().await;
        }
        success
    }
    async fn reap(&self) {
        let tasks = self.tasks.lock().unwrap().tasks.clone();
        for task in tasks {
            // Only observe completed actual handles; never wait behind live work.
            let finished = task
                .join
                .try_lock()
                .is_ok_and(|join| join.as_ref().is_some_and(JoinHandle::is_finished));
            if finished {
                task.join().await;
            }
        }
        // Failed observations remain sticky and consume capacity. Successful
        // actual joins can be removed, permitting sequential use indefinitely.
        self.tasks.lock().unwrap().reap_observed();
    }
}

#[derive(Clone)]
pub(crate) struct Source(Weak<Mount>);
impl Source {
    pub(in crate::registry) fn new(mount: &Arc<Mount>) -> Self {
        Self(Arc::downgrade(mount))
    }

    pub(in crate::registry) fn original_roster(&self) -> Result<OriginalRoster, ExtensionError> {
        let mount = self.0.upgrade().ok_or(ExtensionError::ContextOwnerClosed)?;
        Ok(OriginalRoster {
            state: mount
                .owned
                .as_ref()
                .ok_or(ExtensionError::ContextOwnerClosed)?
                .invocations
                .clone(),
            mount: self.0.clone(),
        })
    }

    pub(crate) async fn invoke<T: Send + 'static>(
        &self,
        construct: impl FnOnce() -> BoxFuture<'static, T> + Send + 'static,
    ) -> Result<T, ExtensionError> {
        let mount = self.0.upgrade().ok_or(ExtensionError::ContextOwnerClosed)?;
        let state = &mount
            .owned
            .as_ref()
            .ok_or(ExtensionError::ContextOwnerClosed)?
            .invocations;
        state.reap().await;
        let owner = current();
        if let Some(owner) = &owner {
            owner.reap().await;
        }
        let context = dispatch::CallbackTaskContext::capture(mount.id);
        let (send, receive) = oneshot::channel();
        let cancel = CancellationToken::new();
        let waiter = CancelOnDrop(cancel.clone());
        let task;
        {
            // Same active lock as concrete close/member admission. Neither
            // callback construction nor executor polling happens under it.
            let active = mount.active.lock().unwrap();
            if !*active {
                return Err(ExtensionError::ContextOwnerClosed);
            }
            let mut tasks = state.tasks.lock().unwrap();
            if tasks.full() {
                return Err(ExtensionError::PreparationRegistration(
                    crate::ResourceRegistrationError::Capacity,
                ));
            }
            mount
                .leases
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (release, ready) = oneshot::channel();
            let actual = mount.clone();
            let task_cancel = cancel.clone();
            let body_owner = owner.clone();
            let handle = tokio::spawn(context.scope(async move {
                if ready.await.is_err() {
                    return true;
                }
                // Actual-body admission: seal before this check forbids FnOnce.
                // No await intervenes between admission and construction.
                if !*actual.active.lock().unwrap() || cancel.is_cancelled() {
                    return true;
                }
                if body_owner
                    .as_ref()
                    .is_some_and(CallbackInvocationCapability::is_closed)
                {
                    return true;
                }
                let output = observe(construct, cancel).await;
                let success = output.is_ok();
                if let Ok(Some(output)) = output {
                    let _ = send.send(output);
                }
                success
            }));
            task = Arc::new(Task {
                join: AsyncMutex::new(Some(handle)),
                result: Mutex::new(None),
                member: Mutex::new(Some(MountMemberLease(Some(mount.clone())))),
                cancel: task_cancel,
            });
            tasks.tasks.push(task.clone());
            if let Some(owner) = &owner {
                owner.retain(task.clone())?;
            }
            let _ = release.send(());
        }
        let output = receive.await;
        let joined = task.join().await;
        drop(waiter);
        if !joined {
            return Err(ExtensionError::MountCleanupIncomplete {
                extension: mount.extension.id().into(),
            });
        }
        output.map_err(|_| ExtensionError::ContextOwnerClosed)
    }
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Poll and explicitly dispose the original future before handing off output.
/// Both operations stay within the original callback's task-local frame.
async fn observe<T>(
    construct: impl FnOnce() -> BoxFuture<'static, T>,
    cancel: CancellationToken,
) -> Result<Option<T>, ()> {
    let mut future = match std::panic::catch_unwind(AssertUnwindSafe(construct)) {
        Ok(future) => future,
        Err(payload) => {
            dispatch::discard_panic_payload(payload);
            return Err(());
        }
    };
    let output = tokio::select! {
        biased;
        () = cancel.cancelled() => Ok(None),
        output = AssertUnwindSafe(&mut future).catch_unwind() => output.map(Some),
    };
    let disposed = std::panic::catch_unwind(AssertUnwindSafe(|| drop(future)));
    let output = match output {
        Ok(output) => Ok(output),
        Err(payload) => {
            dispatch::discard_panic_payload(payload);
            Err(())
        }
    };
    if let Err(payload) = disposed {
        dispatch::discard_panic_payload(payload);
        // Output may retain resources whose ownership became uncertain.
        std::mem::forget(output);
        return Err(());
    }
    output
}
