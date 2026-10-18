//! Orderly close keeps its observer separate from every owner being joined.
use super::Agent;
use crabber_extension::{ContextualCloseObservation, ExtensionError};
use crabber_runtime::{CloseObservation, RuntimeCloseObservation, RuntimeCloseReport};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle, time::Instant};

/// Unstable until the durable-host consumer contract is qualified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostExtensionClose {
    NotConfigured,
    Pending,
    Closed,
    Failed(ExtensionError),
}
/// Actual observed local owners, independent of durable settlement and deletion.
/// Unstable until the durable-host consumer contract is qualified.
#[derive(Debug, Clone)]
pub struct HostCloseReport {
    pub observation: CloseObservation,
    pub runtime: RuntimeCloseReport,
    pub extensions: HostExtensionClose,
    pub observer_failed: bool,
}
impl HostCloseReport {
    #[must_use]
    pub fn local_drained(&self) -> bool {
        !self.observer_failed
            && self.runtime.local_drained()
            && matches!(
                self.extensions,
                HostExtensionClose::NotConfigured | HostExtensionClose::Closed
            )
    }
}
#[derive(Default)]
pub(super) struct CloseCoordinator {
    current: Mutex<Option<Arc<Pass>>>,
    failed: Arc<AtomicBool>,
}
struct Pass {
    deadline: Instant,
    state: AsyncMutex<PassState>,
    finished: AtomicBool,
    failed: Arc<AtomicBool>,
}
enum PassState {
    Running(JoinHandle<HostCloseReport>),
    Observed(Box<HostCloseReport>),
    Failed,
}
#[derive(Clone)]
struct Observation {
    runtime: RuntimeCloseObservation,
    extensions: Result<Option<ContextualCloseObservation>, ExtensionError>,
}
impl Observation {
    fn snapshot(&self, observation: CloseObservation, failed: bool) -> HostCloseReport {
        let extensions = match &self.extensions {
            Ok(None) => HostExtensionClose::NotConfigured,
            Ok(Some(actual)) => match actual.result() {
                None => HostExtensionClose::Pending,
                Some(Ok(())) => HostExtensionClose::Closed,
                Some(Err(error)) => HostExtensionClose::Failed(error),
            },
            Err(error) => HostExtensionClose::Failed(error.clone()),
        };
        HostCloseReport {
            observation,
            runtime: self.runtime.snapshot(),
            extensions,
            observer_failed: failed,
        }
    }
    async fn run(self, deadline: Instant) -> HostCloseReport {
        let extensions = async {
            match &self.extensions {
                Ok(Some(actual)) => match actual.observe_until(deadline).await {
                    Ok(()) => HostExtensionClose::Closed,
                    Err(_) if actual.result().is_none() => HostExtensionClose::Pending,
                    Err(error) => HostExtensionClose::Failed(error),
                },
                Ok(None) => HostExtensionClose::NotConfigured,
                Err(error) => HostExtensionClose::Failed(error.clone()),
            }
        };
        let (runtime, extensions) = tokio::join!(self.runtime.observe_until(deadline), extensions);
        let mut report = self.snapshot(CloseObservation::Pending, false);
        report.observation = match runtime.observation {
            CloseObservation::Complete => CloseObservation::Pending,
            observation => observation,
        };
        report.runtime = runtime;
        report.extensions = extensions;
        if report.local_drained() {
            report.observation = CloseObservation::Complete;
        }
        report
    }
}
impl Agent {
    /// Stop fresh local admission, signal all retained work and observe original
    /// execution, resource and extension joins under one caller deadline.
    /// Timeout or caller cancellation retains cleanup. Repeated close observes
    /// the same owners; no local result certifies durable settlement or deletion.
    /// Unstable until the durable-host consumer contract is qualified.
    pub async fn close(&self, bound: Duration) -> HostCloseReport {
        let started = Instant::now();
        let deadline = started.checked_add(bound);
        self.closed.store(true, Ordering::SeqCst);
        let runtime = self.runtime.begin_close();
        // These checks and cuts run in the actual caller's context before detach.
        let remaining = deadline.map_or(Duration::ZERO, |d| {
            d.saturating_duration_since(Instant::now())
        });
        let extensions = self
            .contextual
            .as_ref()
            .map(|p| p.begin_close_all(remaining))
            .transpose();
        let observation = Observation {
            runtime,
            extensions,
        };
        let failed = self.close_observer.failed.load(Ordering::Acquire);
        if failed {
            return observation.snapshot(CloseObservation::Pending, true);
        }
        let pass_deadline = deadline.unwrap_or(started);
        if observation.runtime.is_self_close()
            || matches!(&observation.extensions, Err(ExtensionError::SelfClose))
        {
            // Runtime and contextual cleanup retain their own original handles.
            // Keep this caller's reentry diagnostic out of the shared host pass.
            let _ = observation.runtime.observe_until(started).await;
            return observation.snapshot(CloseObservation::SelfClose, failed);
        }
        loop {
            let pass = self.close_observer.pass(&observation, pass_deadline);
            if deadline.is_none() {
                return observation.snapshot(CloseObservation::UnrepresentableDeadline, false);
            }
            match tokio::time::timeout_at(pass_deadline, pass.observe()).await {
                Ok(Some(mut report)) => {
                    report.observer_failed |= self.close_observer.failed.load(Ordering::Acquire);
                    if report.observer_failed && report.observation == CloseObservation::Complete {
                        report.observation = CloseObservation::Pending;
                    }
                    // A canceled or shorter caller may have left an already
                    // completed pass. Reobserve the same owners for this bound.
                    if !report.local_drained()
                        && !report.observer_failed
                        && pass.deadline < pass_deadline
                        && Instant::now() < pass_deadline
                    {
                        continue;
                    }
                    return report;
                }
                Ok(None) => return observation.snapshot(CloseObservation::Pending, true),
                Err(_) => {
                    return observation.snapshot(
                        CloseObservation::DeadlineExpired,
                        self.close_observer.failed.load(Ordering::Acquire),
                    );
                }
            }
        }
    }
}
impl CloseCoordinator {
    fn pass(&self, observation: &Observation, deadline: Instant) -> Arc<Pass> {
        let mut current = self.current.lock().unwrap();
        if let Some(pass) = current
            .as_ref()
            .filter(|p| !p.finished.load(Ordering::Acquire))
        {
            return pass.clone();
        }
        let owned = observation.clone();
        let pass = Arc::new(Pass {
            deadline,
            state: AsyncMutex::new(PassState::Running(tokio::spawn(async move {
                owned.run(deadline).await
            }))),
            finished: AtomicBool::new(false),
            failed: self.failed.clone(),
        });
        *current = Some(pass.clone());
        pass
    }
}
impl Pass {
    async fn observe(&self) -> Option<HostCloseReport> {
        let mut state = self.state.lock().await;
        let joined = match &mut *state {
            PassState::Observed(report) => return Some(report.as_ref().clone()),
            PassState::Failed => return None,
            PassState::Running(task) => task.await,
        };
        match joined {
            Ok(report) => {
                *state = PassState::Observed(Box::new(report.clone()));
                self.finished.store(true, Ordering::Release);
                Some(report)
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                *state = PassState::Failed;
                self.finished.store(true, Ordering::Release);
                drop(state);
                if let Err(payload) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(error)))
                {
                    std::mem::forget(payload);
                }
                None
            }
        }
    }
}
