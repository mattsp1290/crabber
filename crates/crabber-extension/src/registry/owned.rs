//! Explicit sealed mount cleanup; completion is an observed actual task join.
use super::{ExtensionError, Mount, MountCloseTimeout, MountHandle, dispatch};
use crate::{ComponentIdentity, ResourceDrain, ResourceFailure, ToolResourceOwner};
use futures::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(super) struct State {
    pub(super) cleanup: ToolResourceOwner,
    close_task: ToolResourceOwner,
    started: Mutex<bool>,
}
impl State {
    pub(super) fn new() -> Self {
        Self {
            cleanup: ToolResourceOwner::new(),
            close_task: ToolResourceOwner::new(),
            started: Mutex::new(false),
        }
    }
}
/// Explicit owned mount handle. Retain it after a timeout; work and its actual
/// joins remain owned. This contract covers this mount's registered cleanup,
/// bounded by [`crate::MAX_TOOL_RESOURCE_TASKS`] retained tasks over its lifetime.
/// Escaped dispatcher clones and caller-driven callback main futures need their
/// own lifecycle ownership; a plan lease or this handle does not certify them.
#[derive(Clone)]
pub struct OwnedMountHandle {
    inner: MountHandle,
}
impl OwnedMountHandle {
    pub(super) fn new(inner: MountHandle) -> Self {
        Self { inner }
    }
    pub(crate) fn identity(&self) -> u64 {
        self.inner.identity()
    }
    pub fn deactivate(&self) {
        self.inner.deactivate();
    }
    pub(crate) fn check_close_allowed(&self) -> Result<(), ExtensionError> {
        self.inner.check_close_allowed()
    }
    /// Seal registration, request close, and await the actual close-task join.
    /// The bound includes plan leases, cleanup, deferred work and shutdown.
    /// A timeout never aborts work and a later call resumes the same joins.
    pub async fn close(&self, bound: Duration) -> Result<OwnedMountJoin, ExtensionError> {
        self.check_close_allowed()?;
        close(&self.inner.mount, bound).await
    }
    pub(crate) fn signal_close(&self) {
        self.inner.deactivate();
        signal(&self.inner.mount);
    }
    pub(crate) async fn close_registered(&self) -> Result<OwnedMountJoin, ExtensionError> {
        self.close(self.inner.mount.close_timeout).await
    }
    /// Current cleanup observations, without asserting an actual close join.
    #[must_use]
    pub fn cleanup_snapshot(&self) -> ResourceDrain {
        self.inner
            .mount
            .owned
            .as_ref()
            .expect("owned mount")
            .cleanup
            .snapshot()
    }
    /// Current close-task observations, without granting a joined capability.
    #[must_use]
    pub fn close_snapshot(&self) -> ResourceDrain {
        self.inner
            .mount
            .owned
            .as_ref()
            .expect("owned mount")
            .close_task
            .snapshot()
    }
}
/// Minted only after the retained close task and every registered cleanup task
/// have actually joined successfully. It carries no native/session authority.
#[derive(Debug, Clone)]
pub struct OwnedMountJoin {
    cleanup: ResourceDrain,
    close_task: ResourceDrain,
}
impl OwnedMountJoin {
    #[must_use]
    pub fn cleanup(&self) -> &ResourceDrain {
        &self.cleanup
    }
    #[must_use]
    pub fn close_task(&self) -> &ResourceDrain {
        &self.close_task
    }
}
pub(super) fn append_contract(mount: &Mount, components: &mut Vec<ComponentIdentity>) {
    if mount.owned.is_some() {
        components.push(ComponentIdentity {
            id: "registry.mount.cleanup".into(),
            version: "crabber.registry.mount.sealed.v1".into(),
        });
    }
}
pub(super) fn signal(mount: &Mount) {
    mount.cleanup.close();
    if let Some(owned) = &mount.owned {
        owned.cleanup.close();
    }
}
pub(super) async fn close(
    mount: &Arc<Mount>,
    bound: Duration,
) -> Result<OwnedMountJoin, ExtensionError> {
    if dispatch::is_active_mount(mount.id) {
        return Err(ExtensionError::SelfClose);
    }
    let owned = mount.owned.as_ref().expect("owned close path");
    start(mount, owned)?;
    let close_task = owned.close_task.drain(bound).await;
    if close_task.pending != 0 {
        observe_timeout(mount, bound);
        return Err(ExtensionError::MountCloseTimeout {
            extension: mount.extension.id().into(),
        });
    }
    let cleanup = owned.cleanup.snapshot();
    if !close_task.sealed
        || close_task.joined != 1
        || !close_task.failures.is_empty()
        || !cleanup.sealed
        || cleanup.pending != 0
        || !cleanup.failures.is_empty()
    {
        return Err(ExtensionError::MountCleanupIncomplete {
            extension: mount.extension.id().into(),
        });
    }
    Ok(OwnedMountJoin {
        cleanup,
        close_task,
    })
}
fn start(mount: &Arc<Mount>, owned: &State) -> Result<(), ExtensionError> {
    // Registration and publication of the leader happen before any follower
    // can seal the separate close-task owner while awaiting its actual join.
    let mut started = owned
        .started
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *started {
        return Ok(());
    }
    *mount.active.lock().unwrap() = false;
    *mount.closing.lock().unwrap() = true;
    signal(mount);
    let retained = mount.clone();
    owned
        .close_task
        .resources()
        .spawn(async move { finish(retained).await })
        .map_err(|_| ExtensionError::MountCleanupIncomplete {
            extension: mount.extension.id().into(),
        })?;
    *started = true;
    Ok(())
}
async fn finish(mount: Arc<Mount>) -> Result<(), ResourceFailure> {
    mount.leases_released().await;
    let cleanup = mount
        .owned
        .as_ref()
        .expect("owned close task")
        .cleanup
        .join()
        .await;
    let rollback = mount.clone();
    // Deferred callbacks may block; retain and await their actual blocking join
    // without preventing the caller's bounded Tokio timer from progressing.
    let deferred = tokio::task::spawn_blocking(move || rollback_all(&rollback)).await;
    let mut failed = !cleanup.failures.is_empty() || !matches!(deferred, Ok(true));
    if let Err(payload) = AssertUnwindSafe(mount.extension.shutdown())
        .catch_unwind()
        .await
    {
        dispatch::discard_panic_payload(payload);
        failed = true;
    }
    *mount.drained.lock().unwrap() = true;
    mount.drained_notify.notify_waiters();
    *mount.closed.lock().unwrap() = true;
    mount.closed_notify.notify_waiters();
    if failed {
        Err(ResourceFailure::CleanupFailed)
    } else {
        Ok(())
    }
}
fn rollback_all(mount: &Mount) -> bool {
    let mut complete = true;
    loop {
        let next = mount
            .registrar
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cleanups
            .pop();
        let Some(cleanup) = next else { return complete };
        if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(cleanup)) {
            dispatch::discard_panic_payload(payload);
            complete = false;
        }
    }
}
fn observe_timeout(mount: &Mount, bound: Duration) {
    if let Some(observer) = &mount.close_observer {
        let timeout = MountCloseTimeout {
            extension: mount.extension.id().into(),
            bound,
            leases: mount.leases.load(std::sync::atomic::Ordering::SeqCst),
            pending_tasks: mount
                .owned
                .as_ref()
                .expect("owned timeout")
                .cleanup
                .snapshot()
                .pending,
        };
        if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| observer(&timeout))) {
            dispatch::discard_panic_payload(payload);
        }
    }
}

#[cfg(test)]
mod tests;

/// Owned failed installation retains the actual blocking rollback join in its
/// caller (the contextual initializer is itself independently join-owned).
pub(super) async fn rollback_install(mut registrar: super::Registrar) -> bool {
    match tokio::task::spawn_blocking(move || {
        let mut complete = true;
        while let Some(cleanup) = registrar.cleanups.pop() {
            if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(cleanup)) {
                dispatch::discard_panic_payload(payload);
                complete = false;
            }
        }
        complete
    })
    .await
    {
        Ok(complete) => complete,
        Err(error) => {
            if error.is_panic() {
                dispatch::discard_panic_payload(error.into_panic());
            }
            false
        }
    }
}
