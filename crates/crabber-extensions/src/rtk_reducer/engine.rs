//! Transform leases and tracked child ownership. No child belongs to the callback.
use super::{Filter, config::Configuration};
use crabber::{ExtensionError, extension::ToolResultContext};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Pull-only diagnostics, read independently. Counters wrap at `u64::MAX`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Transform calls with at least one accepted replacement.
    pub reduced: u64,
    /// Fields that returned identical or insufficiently smaller output.
    /// Empty selected strings skip processing and do not increment a counter.
    pub no_gain: u64,
    /// Calls skipped by class, binding, input or shape checks.
    pub passed_through: u64,
    /// Calls denied without queueing because all leases were held.
    pub capacity_denied: u64,
    /// Invocations whose shared transform deadline expired.
    pub timed_out: u64,
    /// Failed spawn, I/O, output or private-directory operations.
    pub process_failed: u64,
    /// Invocations or lease attempts cancelled or closed.
    pub cancelled: u64,
    /// Cleanup could not prove reaping or directory removal; reuse is blocked.
    pub quarantined: bool,
    /// Successfully closed, permanently unavailable for further work.
    pub closed: bool,
}
#[derive(Default)]
pub(super) struct Counters {
    pub(super) reduced: AtomicU64,
    pub(super) no_gain: AtomicU64,
    pub(super) passed_through: AtomicU64,
    capacity_denied: AtomicU64,
    timed_out: AtomicU64,
    process_failed: AtomicU64,
    cancelled: AtomicU64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Reason {
    Closed,
    Quarantined,
    Cancelled,
    Capacity,
    Spawn,
    Timeout,
    ExitFailure,
    WriteFailed,
    Overflow,
    EmptyOutput,
    InvalidUtf8,
    PrivateDirs,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Reduced {
    Output(String),
    Unchanged(Reason),
}
#[derive(Default)]
pub(super) struct State {
    pub(super) root: Option<PathBuf>,
    pub(super) seq: u64,
    pub(super) pending_dirs: BTreeSet<PathBuf>,
    failed_dirs: BTreeSet<PathBuf>,
    // live includes owners, unreaped survivors, and failed directory removals.
    pub(super) live: usize,
    // tasks lets close retry failed removals without waiting on itself.
    tasks: usize,
    pub(super) quarantined: bool,
    pub(super) closing: bool,
    closed: bool,
}
pub(super) struct Engine {
    pub(super) configuration: Configuration,
    capacity: Arc<Semaphore>,
    pub(super) closing: CancellationToken,
    idle: Notify,
    pub(super) state: Mutex<State>,
    pub(super) counters: Counters,
    #[cfg(all(test, unix))]
    pub(super) hooks: Mutex<Hooks>,
}
#[cfg(all(test, unix))]
#[derive(Default, Clone)]
pub(super) struct Hooks {
    pub(super) spawned_groups: Vec<i32>,
    pub(super) panic_after_spawn: bool,
    pub(super) reap_timeout_once: bool,
    pub(super) fail_remove: bool,
    pub(super) before_reap: Option<Arc<Semaphore>>,
    pub(super) at_reap: Option<Arc<Semaphore>>,
    pub(super) before_remove: Option<Arc<Semaphore>>,
    pub(super) after_live_increment: Option<Arc<Semaphore>>,
}
pub(super) struct Lease {
    permit: Arc<OwnedSemaphorePermit>,
    deadline: Instant,
    engine: Arc<Engine>,
    context: ToolResultContext,
}
impl Engine {
    pub(super) fn new(configuration: Configuration) -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(configuration.limits.max_concurrent)),
            configuration,
            closing: CancellationToken::new(),
            idle: Notify::new(),
            state: Mutex::new(State::default()),
            counters: Counters::default(),
            #[cfg(all(test, unix))]
            hooks: Mutex::new(Hooks::default()),
        }
    }
    fn unavailable(&self, state: &State, context: &ToolResultContext) -> Option<Reason> {
        if state.closing || state.closed || context.cleanup().is_closing() {
            Some(Reason::Closed)
        } else if state.quarantined {
            Some(Reason::Quarantined)
        } else if context.cancellation().is_cancelled() {
            Some(Reason::Cancelled)
        } else {
            None
        }
    }
    pub(super) fn acquire(self: &Arc<Self>, context: &ToolResultContext) -> Result<Lease, Reason> {
        let state = self.state.lock().unwrap();
        if let Some(reason) = self.unavailable(&state, context) {
            self.count(reason);
            return Err(reason);
        }
        let permit = self.capacity.clone().try_acquire_owned().map_err(|_| {
            self.count(Reason::Capacity);
            Reason::Capacity
        })?;
        Ok(Lease {
            permit: Arc::new(permit),
            deadline: Instant::now() + self.configuration.limits.timeout,
            engine: self.clone(),
            context: context.clone(),
        })
    }
    pub(super) fn count(&self, reason: Reason) {
        let counter = match reason {
            Reason::Capacity => &self.counters.capacity_denied,
            Reason::Timeout => &self.counters.timed_out,
            Reason::Cancelled | Reason::Closed => &self.counters.cancelled,
            _ => &self.counters.process_failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn stats(&self) -> Stats {
        let state = self.state.lock().unwrap();
        let c = &self.counters;
        Stats {
            reduced: c.reduced.load(Ordering::Relaxed),
            no_gain: c.no_gain.load(Ordering::Relaxed),
            passed_through: c.passed_through.load(Ordering::Relaxed),
            capacity_denied: c.capacity_denied.load(Ordering::Relaxed),
            timed_out: c.timed_out.load(Ordering::Relaxed),
            process_failed: c.process_failed.load(Ordering::Relaxed),
            cancelled: c.cancelled.load(Ordering::Relaxed),
            quarantined: state.quarantined,
            closed: state.closed,
        }
    }
    pub(super) fn live_children(&self) -> usize {
        self.state.lock().unwrap().live
    }
    #[cfg(all(test, unix))]
    pub(super) fn available_permits(&self) -> usize {
        self.capacity.available_permits()
    }
    pub(super) async fn close(self: &Arc<Self>, bound: Duration) -> Result<(), ExtensionError> {
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Ok(());
            }
            state.closing = true;
        }
        self.closing.cancel();
        let close = async {
            loop {
                let notified = self.idle.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.state.lock().unwrap().tasks == 0 {
                    break;
                }
                notified.await;
            }
            let dirs = self.state.lock().unwrap().pending_dirs.clone();
            let mut failed = false;
            for dir in dirs {
                if super::private_dirs::remove(dir.clone()).await.is_err() {
                    failed = true;
                    continue;
                }
                let mut state = self.state.lock().unwrap();
                state.pending_dirs.remove(&dir);
                if state.failed_dirs.remove(&dir) {
                    state.live = state.live.saturating_sub(1);
                }
            }
            if failed {
                return Err(());
            }
            let root = self.state.lock().unwrap().root.clone();
            if let Some(root) = root {
                super::private_dirs::remove(root).await?;
            }
            let mut state = self.state.lock().unwrap();
            if state.live != 0 {
                return Err(());
            }
            state.closed = true;
            Ok(())
        };
        match tokio::time::timeout(bound, close).await {
            Ok(Ok(())) => Ok(()),
            _ => {
                self.state.lock().unwrap().quarantined = true;
                Err(ExtensionError::Tool(
                    "rtk reducer operation failed: cleanup-incomplete".into(),
                ))
            }
        }
    }
}
impl Lease {
    pub(super) async fn reduce(&self, filter: Filter, input: &str) -> Reduced {
        {
            let mut state = self.engine.state.lock().unwrap();
            let reason = self
                .engine
                .unavailable(&state, &self.context)
                .or_else(|| (Instant::now() >= self.deadline).then_some(Reason::Timeout));
            if let Some(reason) = reason {
                self.engine.count(reason);
                return Reduced::Unchanged(reason);
            }
            state.live += 1;
            state.tasks += 1;
        }
        let (sender, receiver) = oneshot::channel();
        let guard = Guard {
            engine: self.engine.clone(),
            permit: Some(self.permit.clone()),
            dir: None,
            finished: false,
        };
        let context = self.context.clone();
        let input = input.to_owned();
        let deadline = self.deadline;
        self.context.cleanup().spawn(async move {
            let mut guard = guard;
            #[cfg(all(test, unix))]
            {
                let gate = guard
                    .engine
                    .hooks
                    .lock()
                    .unwrap()
                    .after_live_increment
                    .clone();
                if let Some(gate) = gate {
                    gate.acquire().await.unwrap().forget();
                }
            }
            super::child::run(&mut guard, context, filter, input, deadline, sender).await;
        });
        match receiver.await {
            Ok(result) => result,
            Err(_) => {
                self.engine.count(Reason::Quarantined);
                Reduced::Unchanged(Reason::Quarantined)
            }
        }
    }
}
pub(super) struct Guard {
    pub(super) engine: Arc<Engine>,
    permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(super) dir: Option<PathBuf>,
    finished: bool,
}
impl Guard {
    pub(super) async fn finish(&mut self, reaped: bool) {
        let removed = if reaped {
            if let Some(dir) = &self.dir {
                #[cfg(all(test, unix))]
                let fail = self.engine.hooks.lock().unwrap().fail_remove;
                #[cfg(not(all(test, unix)))]
                let fail = false;
                let removal = async {
                    #[cfg(all(test, unix))]
                    {
                        let gate = self.engine.hooks.lock().unwrap().before_remove.clone();
                        if let Some(gate) = gate {
                            gate.acquire().await.unwrap().forget();
                        }
                    }
                    super::private_dirs::remove(dir.clone()).await
                };
                !fail
                    && matches!(
                        tokio::time::timeout(self.engine.configuration.limits.kill_wait, removal)
                            .await,
                        Ok(Ok(()))
                    )
            } else {
                true
            }
        } else {
            false
        };
        let mut state = self.engine.state.lock().unwrap();
        state.tasks -= 1;
        if removed {
            if let Some(dir) = &self.dir {
                state.pending_dirs.remove(dir);
            }
            state.live -= 1;
        } else {
            state.quarantined = true;
            if reaped && let Some(dir) = &self.dir {
                state.failed_dirs.insert(dir.clone());
            }
        }
        match self.permit.take() {
            Some(permit) if !reaped => std::mem::forget(permit),
            permit => drop(permit),
        }
        self.finished = true;
        self.engine.idle.notify_waiters();
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self.engine.state.lock().unwrap_or_else(|p| p.into_inner());
            state.quarantined = true;
            state.tasks -= 1;
            std::mem::forget(self.permit.take());
            self.engine.idle.notify_waiters();
        }
    }
}
