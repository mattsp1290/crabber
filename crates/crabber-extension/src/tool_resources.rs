//! Locally owned tool tasks. Joining a task proves only that task terminated;
//! native code must include the resources it owns in its cleanup future.
#[cfg(test)]
#[path = "tool_resources/cleanup_tests.rs"]
mod cleanup_tests;
use futures::StreamExt;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Library task-ownership contract, included in execution plan fingerprints.
pub const TOOL_RESOURCE_CONTRACT_VERSION: u32 = 3;
/// Maximum registered tasks in one native tool scope, including retained joins.
pub const MAX_TOOL_RESOURCE_TASKS: usize = 64;

/// Finite local cleanup failure; never carries tool content or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceFailure {
    CleanupFailed,
    Panicked,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResourceRegistrationError {
    #[error("tool resource ownership is unavailable")]
    Unsupported,
    #[error("tool resource scope is sealed")]
    Sealed,
    #[error("tool resource task capacity reached")]
    Capacity,
}
/// Observation of actual local joins, not durable phase readiness.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResourceDrain {
    pub sealed: bool,
    pub joined: usize,
    pub pending: usize,
    pub failures: Vec<ResourceFailure>,
}
#[derive(Default)]
struct State {
    sealed: bool,
    tasks: Vec<Arc<ResourceTask>>,
}
struct Scope {
    state: Mutex<State>,
    closing: CancellationToken,
}
struct ResourceTask {
    join: tokio::sync::Mutex<Option<JoinHandle<Result<(), ResourceFailure>>>>,
    result: Mutex<Option<Result<(), ResourceFailure>>>,
}
/// Registration capability of one native tool call. Clones share a sealed
/// boundary: retained clones cannot register after the call leaves its scope.
#[derive(Clone, Default)]
pub struct ToolResources(Option<Arc<Scope>>);
impl std::fmt::Debug for ToolResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolResources([redacted])")
    }
}
impl ToolResources {
    /// Register all task-owned cleanup in the supplied future. The owner retains
    /// its join handle even if the tool drops this capability. No abort handle
    /// is returned, and failures remain visible to the owner.
    /// # Errors
    /// Rejects registration without an owner or after the scope is sealed.
    pub fn spawn<F>(&self, task: F) -> Result<(), ResourceRegistrationError>
    where
        F: Future<Output = Result<(), ResourceFailure>> + Send + 'static,
    {
        let scope = self
            .0
            .as_ref()
            .ok_or(ResourceRegistrationError::Unsupported)?;
        let mut state = scope
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed {
            return Err(ResourceRegistrationError::Sealed);
        }
        if state.tasks.len() >= MAX_TOOL_RESOURCE_TASKS {
            return Err(ResourceRegistrationError::Capacity);
        }
        state.tasks.push(Arc::new(ResourceTask {
            join: tokio::sync::Mutex::new(Some(tokio::spawn(task))),
            result: Mutex::new(None),
        }));
        Ok(())
    }
    /// A cooperative close signal. It never aborts a resource task or proves
    /// that an external process/effect has stopped.
    #[must_use]
    pub fn closing(&self) -> Option<tokio_util::sync::WaitForCancellationFutureOwned> {
        self.0
            .as_ref()
            .map(|scope| scope.closing.clone().cancelled_owned())
    }
}
/// Host side of a tool-task scope. Keep this owner after a timed-out drain;
/// dropping it relinquishes local join ownership and is not a clean shutdown.
#[derive(Clone)]
pub struct ToolResourceOwner(Arc<Scope>);
impl Default for ToolResourceOwner {
    fn default() -> Self {
        Self(Arc::new(Scope {
            state: Mutex::new(State::default()),
            closing: CancellationToken::new(),
        }))
    }
}
impl ToolResourceOwner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn resources(&self) -> ToolResources {
        ToolResources(Some(self.0.clone()))
    }
    /// Cleanup registration sharing this phase's sealing, cancellation and
    /// retained task joins. Task failures remain visible in the phase drain.
    #[must_use]
    pub fn cleanup_tracker(&self) -> crate::CleanupTracker {
        crate::CleanupTracker::for_resources(self.resources(), self.0.closing.clone())
    }
    /// Seal registration and request cooperative close under the same boundary
    /// used by registration. Idempotent; work keeps its actual join owner.
    pub fn close(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sealed = true;
        self.0.closing.cancel();
    }
    /// Seal on return, cancellation or panic of the enclosing tool future.
    #[must_use]
    pub fn guard(&self) -> ToolResourceGuard {
        ToolResourceGuard(self.clone())
    }
    #[must_use]
    pub fn snapshot(&self) -> ResourceDrain {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut report = ResourceDrain {
            sealed: state.sealed,
            ..ResourceDrain::default()
        };
        for task in &state.tasks {
            match *task
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                Some(Ok(())) => report.joined += 1,
                Some(Err(error)) => {
                    report.joined += 1;
                    report.failures.push(error);
                }
                None => report.pending += 1,
            }
        }
        report
    }
    /// Await actual joins under a deadline. A timeout leaves every unfinished
    /// join handle retained here; later drains resume those same joins. Failure
    /// stays visible even after all tasks have terminated.
    pub async fn drain(&self, bound: Duration) -> ResourceDrain {
        self.close();
        let tasks = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .clone();
        let _ = tokio::time::timeout(bound, async {
            let mut joins = futures::stream::FuturesUnordered::new();
            for task in tasks {
                joins.push(async move {
                    task.join().await;
                });
            }
            while joins.next().await.is_some() {}
        })
        .await;
        self.snapshot()
    }
}
/// Closes registration without discarding the owner's retained tasks.
pub struct ToolResourceGuard(ToolResourceOwner);
impl Drop for ToolResourceGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}
impl ResourceTask {
    async fn join(&self) {
        let mut slot = self.join.lock().await;
        let Some(handle) = slot.as_mut() else { return };
        let result = match handle.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Err(ResourceFailure::Cancelled),
            Err(error) => {
                let payload = error.into_panic();
                if let Err(nested) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
                {
                    std::mem::forget(nested);
                }
                Err(ResourceFailure::Panicked)
            }
        };
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        *slot = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn timed_out_join_retains_tasks_and_seals_retained_registration() {
        let owner = ToolResourceOwner::new();
        let resources = owner.resources();
        let released = Arc::new(Notify::new());
        let wait = released.clone();
        resources
            .spawn(async move {
                wait.notified().await;
                Ok(())
            })
            .unwrap();
        let report = owner.drain(Duration::ZERO).await;
        assert!(report.sealed);
        assert_eq!(report.pending, 1);
        assert_eq!(
            resources.spawn(async { Ok(()) }),
            Err(ResourceRegistrationError::Sealed)
        );
        released.notify_one();
        let report = owner.drain(Duration::from_secs(1)).await;
        assert_eq!(report.joined, 1);
        assert_eq!(report.pending, 0);
        assert_eq!(report.failures, []);
    }

    #[tokio::test]
    async fn joins_report_failure_and_panic_instead_of_claiming_cleanup() {
        let owner = ToolResourceOwner::new();
        owner
            .resources()
            .spawn(async { Err(ResourceFailure::CleanupFailed) })
            .unwrap();
        owner
            .resources()
            .spawn(async {
                panic!("resource panic");
            })
            .unwrap();
        let report = owner.drain(Duration::from_secs(1)).await;
        assert_eq!(report.joined, 2);
        assert_eq!(report.pending, 0);
        assert_eq!(
            report.failures,
            [ResourceFailure::CleanupFailed, ResourceFailure::Panicked]
        );
        assert_eq!(owner.drain(Duration::ZERO).await, report);
    }

    #[tokio::test]
    async fn concurrent_registration_and_sealing_never_lose_an_accepted_task() {
        let owner = ToolResourceOwner::new();
        let resources = owner.resources();
        let spawn = tokio::spawn(async move {
            let mut accepted = 0;
            for _ in 0..1000 {
                match resources.spawn(async { Ok(()) }) {
                    Ok(()) => accepted += 1,
                    Err(
                        ResourceRegistrationError::Sealed | ResourceRegistrationError::Capacity,
                    ) => break,
                    Err(error) => panic!("unexpected registration error: {error}"),
                }
                tokio::task::yield_now().await;
            }
            accepted
        });
        tokio::task::yield_now().await;
        owner.close();
        let accepted = spawn.await.unwrap();
        let report = owner.drain(Duration::from_secs(1)).await;
        assert_eq!(report.joined, accepted);
        assert_eq!(report.pending, 0);
        assert_eq!(report.failures, []);
    }

    #[tokio::test]
    async fn registration_is_bounded_without_silently_detaching_extra_tasks() {
        let owner = ToolResourceOwner::new();
        for _ in 0..MAX_TOOL_RESOURCE_TASKS {
            owner.resources().spawn(async { Ok(()) }).unwrap();
        }
        assert_eq!(
            owner.resources().spawn(async { Ok(()) }),
            Err(ResourceRegistrationError::Capacity)
        );
        let report = owner.drain(Duration::from_secs(1)).await;
        assert_eq!(report.joined, MAX_TOOL_RESOURCE_TASKS);
        assert_eq!(report.pending, 0);
    }

    #[tokio::test]
    async fn dropped_drain_waiter_preserves_the_actual_join() {
        let owner = ToolResourceOwner::new();
        let resources = owner.resources();
        let released = Arc::new(Notify::new());
        let wait = released.clone();
        resources
            .spawn(async move {
                wait.notified().await;
                Ok(())
            })
            .unwrap();
        let mut pending = Box::pin(owner.drain(Duration::from_secs(60)));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        released.notify_one();
        // A separate waiter resumes the same retained join after the first is dropped.
        let report = owner.drain(Duration::from_secs(1)).await;
        assert_eq!(report.joined, 1);
        assert_eq!(report.pending, 0);
    }
}
