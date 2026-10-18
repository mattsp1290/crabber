//! Terminal local ingress with retained observation of the original owners.
use super::{
    Orchestrator, RuntimeDependentCloseReport, RuntimeError, close_dependents::CloseDependents,
};
use crate::{ExecutionTaskDrain, ExecutionTaskFailure, PreparationTaskId};
use crabber_session::session_owner::owned_scope::PendingSupervisionReport;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// Finite observation of local close, never a native retirement fact.
/// Unstable until the durable-host consumer contract is qualified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseObservation {
    Complete,
    Pending,
    DeadlineExpired,
    SelfClose,
    UnrepresentableDeadline,
}
/// Actual retained local work and failures across all Runtime registries.
/// Unstable until the durable-host consumer contract is qualified.
#[derive(Debug, Clone)]
pub struct RuntimeCloseReport {
    pub observation: CloseObservation,
    pub executions: ExecutionTaskDrain,
    pub dependents: RuntimeDependentCloseReport,
    pub pending_enrollments: Vec<PreparationTaskId>,
    pub failures: Vec<ExecutionTaskFailure>,
    pub run_issues: Vec<crate::RunCloseIssue>,
    pub supervision: Option<PendingSupervisionReport>,
    pub observer_failed: bool,
    pub uncertain_preparations:
        Vec<crabber_session::session_owner::preparation::PreparationReconciliation>,
}
impl RuntimeCloseReport {
    /// All known local owners joined successfully; no durable/policy claim follows.
    #[must_use]
    pub fn local_drained(&self) -> bool {
        self.pending_enrollments.is_empty()
            && self.failures.is_empty()
            && self.uncertain_preparations.is_empty()
            && self.run_issues.is_empty()
            && !self.observer_failed
            && self.executions.pending.is_empty()
            && self.executions.pending_preparations.is_empty()
            && self
                .executions
                .resources
                .iter()
                .all(|scope| drained(&scope.resources))
            && self
                .executions
                .background_tasks
                .iter()
                .all(|scope| drained(&scope.resources))
            && self.dependents.is_drained()
            && self.supervision.is_some_and(|report| report.is_complete())
    }
}
fn drained(r: &crabber_extension::ResourceDrain) -> bool {
    r.sealed && r.pending == 0 && r.failures.is_empty()
}
#[derive(Default)]
pub(super) struct CloseCoordinator {
    pass: Mutex<Option<Arc<ClosePass>>>,
    failed: Arc<std::sync::atomic::AtomicBool>,
}
struct ClosePass {
    deadline: Instant,
    failed: Arc<std::sync::atomic::AtomicBool>,
    state: tokio::sync::Mutex<PassState>,
    finished: std::sync::atomic::AtomicBool,
}
enum PassState {
    Running(tokio::task::JoinHandle<RuntimeCloseReport>),
    Observed(Box<RuntimeCloseReport>),
    Failed,
}
/// Observation only: this object cannot register work or restore ingress.
/// Unstable until the durable-host consumer contract is qualified.
#[derive(Clone)]
pub struct RuntimeCloseObservation {
    runtime: Orchestrator,
    dependents: Arc<CloseDependents>,
    self_close: bool,
}
impl Orchestrator {
    /// Stop new local work and signal every original before any asynchronous wait.
    #[must_use]
    pub fn begin_close(&self) -> RuntimeCloseObservation {
        let callback =
            self.execution_tasks.is_current_callback_parent() || self.is_current_dependent_owner();
        let self_close = self.execution_tasks.begin_close() || callback;
        let dependents = Arc::new(self.begin_dependent_close());
        dependents.signal_all();
        RuntimeCloseObservation {
            runtime: self.clone(),
            dependents,
            self_close,
        }
    }
    /// Terminally stop local admission and observe actual joins under one deadline.
    /// Unstable until the durable-host consumer contract is qualified.
    pub async fn close(&self, bound: Duration) -> RuntimeCloseReport {
        let observation = self.begin_close();
        match Instant::now().checked_add(bound) {
            Some(deadline) => observation.observe_until(deadline).await,
            None => observation.report(CloseObservation::UnrepresentableDeadline),
        }
    }
    /// Reject fresh roots without interrupting any already admitted work.
    pub fn seal_ingress(&self) {
        self.execution_tasks.seal_ingress();
    }
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.execution_tasks.is_closed()
    }
    /// Initialize only the configured concrete contextual provider under ownership.
    /// # Errors
    /// Rejects closed intake, initializer reentry, or failed contextual setup.
    pub async fn initialize_extensions(&self) -> Result<(), RuntimeError> {
        if let Some(provider) = &self.contextual_provider {
            provider.check_ready_allowed()?;
        }
        self.own_root(
            None,
            |runtime| async move { runtime.initialize_owned().await },
        )
        .await
    }
    pub(super) fn preparation_entry_refusal(&self) -> Option<RuntimeError> {
        if self.is_closed() {
            return Some(RuntimeError::HostClosed);
        }
        self.contextual_provider
            .as_ref()
            .and_then(|provider| provider.check_ready_allowed().err())
            .map(RuntimeError::ExtensionPlan)
    }
    pub(super) fn check_original_stop(&self) -> Result<(), RuntimeError> {
        if self
            .root_scope
            .as_ref()
            .is_some_and(|scope| scope.cancellation().is_cancelled())
        {
            Err(RuntimeError::HostClosed)
        } else {
            Ok(())
        }
    }
    pub(super) async fn initialize_owned(&self) -> Result<(), RuntimeError> {
        if self
            .root_scope
            .as_ref()
            .is_some_and(|scope| scope.cancellation().is_cancelled())
        {
            return Err(RuntimeError::HostClosed);
        }
        if let Some(provider) = &self.contextual_provider {
            provider.ready().await.map_err(|error| match error {
                crabber_extension::ExtensionError::RegistryClosed => RuntimeError::HostClosed,
                error => RuntimeError::ExtensionPlan(error),
            })?;
        }
        Ok(())
    }
}
impl RuntimeCloseObservation {
    /// Current safe metadata; never waits for a blocked owner's lock or join.
    #[must_use]
    pub fn snapshot(&self) -> RuntimeCloseReport {
        self.report(if self.self_close {
            CloseObservation::SelfClose
        } else {
            CloseObservation::Pending
        })
    }
    #[must_use]
    pub fn is_self_close(&self) -> bool {
        self.self_close
    }
    fn report(&self, observation: CloseObservation) -> RuntimeCloseReport {
        RuntimeCloseReport {
            observation,
            executions: self.runtime.execution_tasks.snapshot_close(),
            dependents: self.dependents.snapshot(),
            pending_enrollments: self.runtime.execution_tasks.pending_enrollments(),
            failures: self.runtime.execution_tasks.failures(),
            run_issues: self.runtime.execution_tasks.run_issues(),
            uncertain_preparations: self.runtime.execution_tasks.uncertain_preparations(),
            supervision: None,
            observer_failed: self
                .runtime
                .close_coordinator
                .failed
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }
    /// Await the same retained observer without cancelling it at the deadline.
    pub async fn observe_until(&self, deadline: Instant) -> RuntimeCloseReport {
        let mut pass = self.pass(deadline, None);
        if self.self_close {
            return self.report(CloseObservation::SelfClose);
        }
        loop {
            match tokio::time::timeout_at(deadline, pass.observe()).await {
                Ok(Some(mut report)) => {
                    report.observer_failed |= self
                        .runtime
                        .close_coordinator
                        .failed
                        .load(std::sync::atomic::Ordering::Acquire);
                    if report.observer_failed && report.observation == CloseObservation::Complete {
                        report.observation = CloseObservation::Pending;
                    }
                    if report.local_drained()
                        || pass.deadline >= deadline
                        || Instant::now() >= deadline
                    {
                        return report;
                    }
                    // The earlier actual observer joined; extend observation only.
                    pass = self.pass(deadline, Some(&pass));
                }
                Ok(None) => return self.report(CloseObservation::Pending),
                Err(_) => return self.report(CloseObservation::DeadlineExpired),
            }
        }
    }
    fn pass(&self, deadline: Instant, previous: Option<&Arc<ClosePass>>) -> Arc<ClosePass> {
        use std::sync::atomic::Ordering;
        let mut retained = self.runtime.close_coordinator.pass.lock().unwrap();
        if let Some(pass) = retained.as_ref() {
            let refresh = previous.is_some_and(|previous| Arc::ptr_eq(pass, previous))
                && pass.finished.load(Ordering::Acquire)
                && pass.deadline < deadline;
            if !refresh {
                return pass.clone();
            }
        }
        let mut original = self.clone();
        // Self-close describes this caller, never the shared observer's caller.
        original.self_close = false;
        let pass = Arc::new(ClosePass {
            deadline,
            failed: self.runtime.close_coordinator.failed.clone(),
            state: tokio::sync::Mutex::new(PassState::Running(tokio::spawn(async move {
                original.run(deadline).await
            }))),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        *retained = Some(pass.clone());
        pass
    }
    async fn run(&self, deadline: Instant) -> RuntimeCloseReport {
        let remaining = || deadline.saturating_duration_since(Instant::now());
        let (mut executions, _dependents, _supervision) = tokio::join!(
            self.runtime.execution_tasks.drain(remaining()),
            self.dependents.observe_until(deadline),
            self.runtime
                .pending_drivers
                .observe_supervision(remaining()),
        );
        // Dependent cleanup may have enrolled its actual reporting supervisor
        // after the first task selection. Observe those joins under the same
        // deadline before collecting the final membership snapshot.
        let mut cleanup = self.runtime.execution_tasks.drain(remaining()).await;
        for joined in cleanup.joined.drain(..) {
            if !executions.joined.contains(&joined) {
                executions.joined.push(joined);
            }
        }
        for joined in cleanup.joined_preparations.drain(..) {
            if !executions.joined_preparations.contains(&joined) {
                executions.joined_preparations.push(joined);
            }
        }
        for joined in cleanup.joined_unscoped_preparations.drain(..) {
            if !executions.joined_unscoped_preparations.contains(&joined) {
                executions.joined_unscoped_preparations.push(joined);
            }
        }
        let mut report = self.report(CloseObservation::Pending);
        // Earlier snapshots may precede an admitted preparation's handoff.
        executions.pending = report.executions.pending;
        executions.pending_preparations = report.executions.pending_preparations;
        executions.resources = report.executions.resources;
        executions.background_tasks = report.executions.background_tasks;
        report.executions = executions;
        report.supervision = Some(
            self.runtime
                .pending_drivers
                .observe_supervision(Duration::ZERO)
                .await,
        );
        if report.local_drained() {
            report.observation = CloseObservation::Complete;
        }
        report
    }
}
impl ClosePass {
    async fn observe(&self) -> Option<RuntimeCloseReport> {
        use std::sync::atomic::Ordering;
        let mut state = self.state.lock().await;
        let result = match &mut *state {
            PassState::Observed(report) => return Some((**report).clone()),
            PassState::Failed => return None,
            PassState::Running(handle) => handle.await,
        };
        match result {
            Ok(report) => {
                *state = PassState::Observed(Box::new(report.clone()));
                self.finished.store(true, Ordering::Release);
                Some(report)
            }
            Err(error) => {
                // Persist failure before publishing completion or disposing payloads.
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

#[cfg(test)]
mod coordinator_tests {
    use super::*;
    use tokio::sync::oneshot;

    fn runtime() -> Orchestrator {
        Orchestrator::builder()
            .store(Arc::new(crabber_session::MemoryStore::new()))
            .resolver(Arc::new(crabber_providers::FakeProvider::scripted(
                Vec::new(),
            )))
            .plan_provider(Arc::new(crabber_extension::StaticPlanProvider::new(
                Vec::new(),
                Vec::new(),
            )))
            .behavior_version("close-observation-tests")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn later_deadline_refreshes_unobserved_short_pass_over_same_actual_original() {
        let runtime = runtime();
        let (release, held) = oneshot::channel();
        let admitted = runtime
            .execution_tasks
            .admit(None, |_| async move {
                held.await.unwrap();
                Ok(())
            })
            .unwrap();
        let observation = runtime.begin_close();
        let short = observation.observe_until(Instant::now()).await;
        assert!(!short.local_drained());
        let first = runtime
            .close_coordinator
            .pass
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        // Observe only executor completion here, leaving its true Join for close.
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let finished = match &*first.state.lock().await {
                    PassState::Running(handle) => handle.is_finished(),
                    PassState::Observed(_) | PassState::Failed => true,
                };
                if finished {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!first.finished.load(std::sync::atomic::Ordering::Acquire));
        release.send(()).unwrap();
        admitted.result.await.unwrap().unwrap();
        let report = observation
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.local_drained(), "{report:?}");
        assert_eq!(report.observation, CloseObservation::Complete);
        let current = runtime
            .close_coordinator
            .pass
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &current));
        assert_eq!(
            runtime.execution_tasks.join(&admitted.task).await,
            crate::TaskTermination::Returned
        );
    }

    #[tokio::test]
    async fn original_self_close_does_not_taint_later_callers_shared_observer() {
        let runtime = runtime();
        let original = runtime.clone();
        let admitted = runtime
            .execution_tasks
            .admit(None, move |_| async move {
                let report = original.close(Duration::from_secs(1)).await;
                assert_eq!(report.observation, CloseObservation::SelfClose);
                Ok(())
            })
            .unwrap();
        admitted.result.await.unwrap().unwrap();
        let report = runtime.close(Duration::from_secs(1)).await;
        assert!(report.local_drained(), "{report:?}");
        assert_eq!(report.observation, CloseObservation::Complete);
        assert_eq!(
            runtime.execution_tasks.join(&admitted.task).await,
            crate::TaskTermination::Returned
        );
    }

    #[tokio::test]
    async fn close_joins_actual_cleanup_enrolled_after_its_first_task_selection() {
        let runtime = runtime();
        let tasks = runtime.execution_tasks.clone();
        let (release_root, held_root) = oneshot::channel();
        let (child_entered, entered) = oneshot::channel();
        let (release_child, held_child) = oneshot::channel();
        let admitted = runtime
            .execution_tasks
            .admit(None, move |scope| async move {
                held_root.await.unwrap();
                tasks.spawn_reporting_in(
                    &scope,
                    crabber_core::SessionId::new(),
                    None,
                    move |_| async move {
                        child_entered.send(()).unwrap();
                        held_child.await.unwrap();
                        crate::TaskTermination::Returned
                    },
                )
            })
            .unwrap();
        let original = runtime.clone();
        let closing = tokio::spawn(async move { original.close(Duration::from_secs(1)).await });
        // Only schedule the counterexample after drain owns its first selected
        // task reference. This count is not used as task completion evidence.
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&admitted.task) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release_root.send(()).unwrap();
        entered.await.unwrap();
        let child = admitted.result.await.unwrap().unwrap();
        release_child.send(()).unwrap();
        let report = closing.await.unwrap();
        assert!(report.local_drained(), "{report:?}");
        assert_eq!(report.pending_enrollments, []);
        assert_eq!(report.executions.joined_preparations.len(), 1);
        assert_eq!(report.executions.joined_unscoped_preparations.len(), 1);
        assert_eq!(
            runtime.execution_tasks.join(&child).await,
            crate::TaskTermination::Returned
        );
    }
}
