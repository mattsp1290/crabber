//! Safe host-local measurements. These callbacks never assert durable settlement.
use crate::{Observer, RunResult, RuntimeError};
use crabber_core::{RunId, RunStatus, SessionId};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// Finite outcome vocabulary; never contains an error message or user content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalReason {
    Success,
    ProviderError,
    ToolError,
    Cancelled,
    LeaseLost,
    Paused,
    RuntimeError,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelPurpose {
    Turn,
    Compaction,
}
/// Names are identities, not guaranteed bounded metric dimensions. Exporters must
/// apply an allowlist or cap before using names as metric tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationKind {
    Run,
    Model {
        purpose: ModelPurpose,
        provider: String,
        model: String,
    },
    Tool {
        name: String,
    },
}
/// One execution measurement, independent of the durable event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationalObservation {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub kind: OperationKind,
    pub reason: TerminalReason,
    pub elapsed: Duration,
    /// First nonempty text delta, once per model call; absent for calls without text.
    pub first_token: Option<Duration>,
}
/// Injectable monotonic time from an arbitrary origin, unrelated to wall time.
/// Implementations must be fast and nonblocking. Regressions saturate to zero.
pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Duration;
}
pub struct SystemMonotonicClock(Instant);
impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}
impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

pub(crate) fn result_reason(result: &Result<RunResult, RuntimeError>) -> TerminalReason {
    match result {
        Ok(run) => match run.status {
            RunStatus::Completed => TerminalReason::Success,
            RunStatus::Paused => TerminalReason::Paused,
            RunStatus::Interrupted => TerminalReason::Cancelled,
            _ => TerminalReason::RuntimeError,
        },
        Err(RuntimeError::LeaseLost) => TerminalReason::LeaseLost,
        Err(RuntimeError::Interrupted) => TerminalReason::Cancelled,
        Err(RuntimeError::Paused) => TerminalReason::Paused,
        Err(RuntimeError::Provider(_)) => TerminalReason::ProviderError,
        Err(_) => TerminalReason::RuntimeError,
    }
}

/// Dropping an in-flight call on lease loss still produces exactly one local
/// measurement. No store writes are performed by this guard.
pub(crate) struct Measurement<'a> {
    observer: &'a dyn Observer,
    clock: &'a dyn MonotonicClock,
    started: Duration,
    pub observation: OperationalObservation,
    cancellation: &'a CancellationToken,
    lost: &'a AtomicBool,
}
impl<'a> Measurement<'a> {
    pub fn new(
        observer: &'a dyn Observer,
        clock: &'a dyn MonotonicClock,
        session: &SessionId,
        run: &RunId,
        kind: OperationKind,
        cancellation: &'a CancellationToken,
        lost: &'a AtomicBool,
    ) -> Self {
        let reason = match kind {
            OperationKind::Model { .. } => TerminalReason::ProviderError,
            OperationKind::Tool { .. } => TerminalReason::ToolError,
            OperationKind::Run => TerminalReason::RuntimeError,
        };
        Self {
            observer,
            clock,
            started: clock.now(),
            observation: OperationalObservation {
                session_id: session.clone(),
                run_id: run.clone(),
                kind,
                reason,
                elapsed: Duration::ZERO,
                first_token: None,
            },
            cancellation,
            lost,
        }
    }
    pub fn first_text(&mut self, text: &str) {
        if !text.is_empty() && self.observation.first_token.is_none() {
            self.observation.first_token = Some(self.clock.now().saturating_sub(self.started));
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.clock.now().saturating_sub(self.started)
    }
}
impl Drop for Measurement<'_> {
    fn drop(&mut self) {
        self.observation.elapsed = self.elapsed();
        if self.lost.load(Ordering::SeqCst) {
            self.observation.reason = TerminalReason::LeaseLost;
        } else if self.cancellation.is_cancelled()
            && !matches!(self.observation.kind, OperationKind::Run)
            && self.observation.reason != TerminalReason::LeaseLost
        {
            self.observation.reason = TerminalReason::Cancelled;
        }
        self.observer.operational_completed(&self.observation);
    }
}
