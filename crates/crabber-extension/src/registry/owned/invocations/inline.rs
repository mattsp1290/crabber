//! Original synchronous calls: observe return/unwind, never a substitute task join.
use super::{Source, current};
use crate::{ExtensionError, registry::MountMemberLease};
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

pub(super) struct Call {
    result: Mutex<Option<bool>>,
    member: Mutex<Option<MountMemberLease>>,
    changed: tokio::sync::Notify,
}
impl Call {
    pub(super) fn result(&self) -> Option<bool> {
        *self.result.lock().unwrap()
    }
    pub(super) async fn returned(&self) -> bool {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = self.result() {
                return result;
            }
            changed.await;
        }
    }
}
struct Observation {
    original: Arc<Call>,
    succeeded: bool,
}
impl Drop for Observation {
    fn drop(&mut self) {
        // The actual call and panic-payload disposal have left their original
        // stack before completion is visible or its concrete mount lease drops.
        self.original.member.lock().unwrap().take();
        *self.original.result.lock().unwrap() = Some(self.succeeded);
        self.original.changed.notify_waiters();
    }
}
impl Source {
    pub(crate) fn invoke_inline<T>(&self, call: impl FnOnce() -> T) -> Result<T, ExtensionError> {
        let mount = self.0.upgrade().ok_or(ExtensionError::ContextOwnerClosed)?;
        let state = &mount
            .owned
            .as_ref()
            .ok_or(ExtensionError::ContextOwnerClosed)?
            .invocations;
        let owner = current();
        let original = {
            let active = mount.active.lock().unwrap();
            if !*active {
                return Err(ExtensionError::ContextOwnerClosed);
            }
            let mut entries = state.tasks.lock().unwrap();
            entries.reap_observed();
            if entries.full() {
                return Err(crate::ResourceRegistrationError::Capacity.into());
            }
            entries.admit()?;
            mount
                .leases
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let original = Arc::new(Call {
                result: Mutex::new(None),
                member: Mutex::new(Some(MountMemberLease(Some(mount.clone())))),
                changed: tokio::sync::Notify::new(),
            });
            entries.inline.push(original.clone());
            original
        };
        let mut observation = Observation {
            original,
            succeeded: true,
        };
        if let Some(owner) = &owner {
            owner.retain_inline(observation.original.clone())?;
        }
        if !*mount.active.lock().unwrap()
            || owner
                .as_ref()
                .is_some_and(super::CallbackInvocationCapability::is_closed)
        {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        observation.succeeded = false;
        crate::dispatch::with_mount_inline(mount.frame, || {
            match std::panic::catch_unwind(AssertUnwindSafe(call)) {
                Ok(value) => {
                    observation.succeeded = true;
                    Ok(value)
                }
                Err(payload) => {
                    crate::dispatch::discard_panic_payload(payload);
                    Err(ExtensionError::ContextCleanupIncomplete)
                }
            }
        })
    }
}
