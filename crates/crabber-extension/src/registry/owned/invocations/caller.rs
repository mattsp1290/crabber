//! One actual parent-resource observer of original callback joins.
use super::{State, Task};
use crate::{ExtensionError, ResourceFailure, ResourceRegistrationError, ToolResources};
use futures::FutureExt;
use std::{future::Future, sync::Arc};

tokio::task_local! { static CURRENT: CallbackInvocationCapability; }

/// Caller-owned callback boundary attached to actual retained cleanup resources.
/// Drop seals immediately; cloned capabilities never delay that seal. Its one
/// registered observer awaits original async callback joins and synchronous
/// call return/unwind, rather than waiter results.
pub struct CallbackInvocationOwner {
    capability: CallbackInvocationCapability,
}
impl CallbackInvocationOwner {
    /// Attach before invoking callbacks. One parent resource slot is consumed;
    /// successful sequential callback joins are reaped from the active roster.
    /// # Errors
    /// Refuses unsupported, sealed or full actual parent resource scopes.
    pub fn attach(resources: ToolResources) -> Result<Self, ResourceRegistrationError> {
        let state = Arc::new(State::default());
        let closing = resources
            .closing()
            .ok_or(ResourceRegistrationError::Unsupported)?;
        let original = state.clone();
        resources.spawn(async move {
            tokio::select! {
                biased;
                () = closing => {},
                () = original.closed() => {},
            }
            original.close();
            if original.join().await {
                Ok(())
            } else {
                Err(ResourceFailure::CleanupFailed)
            }
        })?;
        Ok(Self {
            capability: CallbackInvocationCapability {
                state,
                parent: resources,
            },
        })
    }
    #[must_use]
    pub fn capability(&self) -> CallbackInvocationCapability {
        self.capability.clone()
    }
}
impl Drop for CallbackInvocationOwner {
    fn drop(&mut self) {
        self.capability.state.close();
    }
}

/// Opaque registration capability; it cannot construct ownership from metadata
/// or outlive the original owner's seal. No native permission is represented.
#[derive(Clone)]
pub struct CallbackInvocationCapability {
    state: Arc<State>,
    parent: ToolResources,
}
impl CallbackInvocationCapability {
    /// Check actual async joins and synchronous return/unwind observations.
    /// Success is neither a seal nor evidence of complete resource membership:
    /// this open capability may still admit later callbacks. No task is polled,
    /// aborted or joined by this diagnostic.
    /// # Errors
    /// A recorded original call has not completed successfully, or its observed
    /// failure remains sticky. Pending/unobserved calls are refused too.
    pub fn check_observed_joins(&self) -> Result<(), ResourceFailure> {
        let entries = self.state.tasks.lock().unwrap();
        if entries
            .tasks
            .iter()
            .all(|task| *task.result.lock().unwrap() == Some(true))
            && entries
                .inline
                .iter()
                .all(|call| call.result() == Some(true))
        {
            Ok(())
        } else {
            Err(ResourceFailure::CleanupFailed)
        }
    }
    pub(crate) fn retain_scripted_source(
        &self,
        source: Arc<crate::contextual::product::scripted::State>,
    ) -> Result<(), ResourceRegistrationError> {
        let closing = self
            .parent
            .closing()
            .ok_or(ResourceRegistrationError::Unsupported)?;
        self.parent.spawn(async move {
            tokio::select! { () = closing => {}, () = source.closing() => {} }
            source.signal_close();
            source
                .join()
                .await
                .map_err(|_| ResourceFailure::CleanupFailed)
        })
    }
    pub(crate) fn matches_current(&self) -> bool {
        !self.is_closed()
            && current().is_some_and(|current| Arc::ptr_eq(&self.state, &current.state))
    }
    pub(super) async fn reap(&self) {
        self.state.reap().await;
    }
    pub(super) fn is_closed(&self) -> bool {
        self.state.tasks.lock().unwrap().sealed
            || self
                .parent
                .closing()
                .is_none_or(|closing| closing.now_or_never().is_some())
    }
    pub(super) fn retain(&self, task: Arc<Task>) -> Result<(), ExtensionError> {
        let mut entries = self.state.tasks.lock().unwrap();
        if entries.sealed
            || self
                .parent
                .closing()
                .is_none_or(|closing| closing.now_or_never().is_some())
        {
            return Err(ResourceRegistrationError::Sealed.into());
        }
        if entries.full() {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        entries.tasks.push(task);
        Ok(())
    }
    pub(super) fn retain_inline(
        &self,
        call: Arc<super::inline::Call>,
    ) -> Result<(), ExtensionError> {
        let mut entries = self.state.tasks.lock().unwrap();
        entries.reap_observed();
        if entries.sealed
            || self
                .parent
                .closing()
                .is_none_or(|closing| closing.now_or_never().is_some())
        {
            return Err(ResourceRegistrationError::Sealed.into());
        }
        if entries.full() {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        entries.inline.push(call);
        Ok(())
    }
}
impl State {
    async fn closed(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.tasks.lock().unwrap().sealed {
                return;
            }
            changed.await;
        }
    }
}
pub(crate) fn current() -> Option<CallbackInvocationCapability> {
    CURRENT.try_with(Clone::clone).ok()
}

/// Scope actual callback registration to an already attached caller owner.
pub async fn with_callback_invocations<T>(
    capability: CallbackInvocationCapability,
    future: impl Future<Output = T>,
) -> T {
    CURRENT.scope(capability, future).await
}
