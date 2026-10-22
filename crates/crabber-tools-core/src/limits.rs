//! Capacity and process-global writer serialization with a single acquisition deadline.
use crate::WorkspaceRoot;
use crabber::ExtensionError;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

/// Required finite bounds shared by a mount.
#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Concurrent work slots, 1..=1024.
    pub max_in_flight: usize,
    /// Combined root-lock and capacity wait, strictly positive.
    pub max_blocking_wait: Duration,
}
impl Limits {
    /// Reject zero or out-of-range bounds with a plan error.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        if !(1..=1024).contains(&self.max_in_flight)
            || self.max_blocking_wait.is_zero()
            || Instant::now().checked_add(self.max_blocking_wait).is_none()
        {
            Err(ExtensionError::Plan("limits".into()))
        } else {
            Ok(())
        }
    }
}
/// Shared per-mount concurrency slots. Owned permits travel with blocking work.
#[derive(Clone)]
pub struct Capacity(Arc<Semaphore>);
impl Capacity {
    /// Validate limits and allocate one semaphore.
    pub fn new(limits: &Limits) -> Result<Self, ExtensionError> {
        limits.validate()?;
        Ok(Self(Arc::new(Semaphore::new(limits.max_in_flight))))
    }
}
/// Writer lock shared by all mounts of the same canonical root.
#[derive(Clone)]
pub struct RootLock(Arc<AsyncMutex<()>>);
type LockRegistry = Mutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>;
static ROOTS: OnceLock<LockRegistry> = OnceLock::new();
impl RootLock {
    /// Obtain a shared writer lock, pruning unused registry entries.
    pub fn for_root(root: &WorkspaceRoot) -> Self {
        let mut roots = ROOTS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        roots.retain(|_, weak| weak.strong_count() > 0);
        let lock = roots
            .get(root.path())
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let lock = Arc::new(AsyncMutex::new(()));
                roots.insert(root.path().to_owned(), Arc::downgrade(&lock));
                lock
            });
        Self(lock)
    }
}
/// Acquisition fails either by cancellation or bounded wait expiry.
#[derive(Debug, PartialEq, Eq)]
pub enum AcquireError {
    /// No tool result may be produced on cancellation.
    Cancelled,
    /// Busy root or exhausted capacity.
    Unavailable,
}
async fn acquire<T>(
    cancel: &CancellationToken,
    deadline: Instant,
    future: impl std::future::Future<Output = T>,
) -> Result<T, AcquireError> {
    tokio::select! { biased;
        _ = cancel.cancelled() => Err(AcquireError::Cancelled),
        value = timeout_at(deadline, future) => value.map_err(|_| AcquireError::Unavailable),
    }
}
/// Acquire only capacity; readers never wait on writer locks.
pub async fn acquire_read(
    capacity: &Capacity,
    limits: &Limits,
    cancel: &CancellationToken,
) -> Result<OwnedSemaphorePermit, AcquireError> {
    let deadline = Instant::now()
        .checked_add(limits.max_blocking_wait)
        .ok_or(AcquireError::Unavailable)?;
    acquire(cancel, deadline, capacity.0.clone().acquire_owned())
        .await?
        .map_err(|_| AcquireError::Unavailable)
}
/// Acquire root lock before capacity, sharing one deadline.
pub async fn acquire_mutating(
    lock: &RootLock,
    capacity: &Capacity,
    limits: &Limits,
    cancel: &CancellationToken,
) -> Result<(OwnedMutexGuard<()>, OwnedSemaphorePermit), AcquireError> {
    let deadline = Instant::now()
        .checked_add(limits.max_blocking_wait)
        .ok_or(AcquireError::Unavailable)?;
    let guard = acquire(cancel, deadline, lock.0.clone().lock_owned()).await?;
    let permit = acquire(cancel, deadline, capacity.0.clone().acquire_owned())
        .await?
        .map_err(|_| AcquireError::Unavailable)?;
    Ok((guard, permit))
}
