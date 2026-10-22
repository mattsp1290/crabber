//! Per-owner state and FIFO operation admission.
use super::{failure, runner::Runner};
use crabber::{core::SessionId, extension::ExtensionError};
use std::{
    path::PathBuf,
    sync::{Mutex, atomic::AtomicBool},
};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};
use tokio_util::sync::CancellationToken;
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct OwnerKey {
    pub(super) session: SessionId,
    pub(super) workspace: String,
}
pub(super) struct OwnerSession {
    pub(super) root: PathBuf,
    pub(super) gate: AsyncMutex<Slot>,
    pub(super) queue: Mutex<usize>,
    pub(super) lifecycle: CancellationToken,
    pub(super) live: AtomicBool,
}
#[derive(Default)]
pub(super) struct Slot {
    pub(super) shared_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    pub(super) dirs: Option<PathBuf>,
    pub(super) runner: Option<Runner>,
    pub(super) generation: u64,
    pub(super) pending_reason: Option<ResetReason>,
    pub(super) quarantined: bool,
    pub(super) closed: bool,
}
#[derive(Clone, Copy)]
pub(super) enum ResetReason {
    Canceled,
    TimedOut,
    RunnerFailed,
    Cleared,
}
impl ResetReason {
    pub(super) fn code(self) -> &'static str {
        match self {
            Self::Canceled => "canceled",
            Self::TimedOut => "timed_out",
            Self::RunnerFailed => "runner_failed",
            Self::Cleared => "cleared",
        }
    }
}
struct Queued<'a>(&'a Mutex<usize>);
impl Drop for Queued<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap() -= 1;
    }
}
impl OwnerSession {
    pub(super) async fn acquire(
        &self,
        cancel: &CancellationToken,
        maximum: usize,
    ) -> Result<MutexGuard<'_, Slot>, ExtensionError> {
        if cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        if self.lifecycle.is_cancelled() {
            return Err(failure("manager-closing"));
        }
        if let Ok(slot) = self.gate.try_lock() {
            return Ok(slot);
        }
        {
            let mut queue = self.queue.lock().unwrap();
            if *queue >= maximum {
                return Err(failure("queue-full"));
            }
            *queue += 1;
        }
        let _queued = Queued(&self.queue);
        tokio::select! { biased;
            () = cancel.cancelled() => Err(failure("cancelled")),
            () = self.lifecycle.cancelled() => Err(failure("manager-closing")),
            slot = self.gate.lock() => Ok(slot),
        }
    }
}
