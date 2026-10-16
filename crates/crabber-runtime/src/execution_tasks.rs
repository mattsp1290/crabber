//! Local task ownership. These joins say nothing about durable outcome or
//! extension-owned resources; those must be reconciled independently.
mod background;
mod correlation;
use background::OwnedBackgroundResources;
pub use background::{WorkerResourceDrain, WorkerResourceKind};
use correlation::TaskCorrelation;
use crabber_core::{RunId, SessionId, ToolCallId};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;

/// Local task termination, observed by awaiting its actual Tokio join handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskTermination {
    Returned,
    Panicked,
    Cancelled,
}

/// A local execution task joined during this drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedExecutionTask {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub termination: TaskTermination,
}

/// Local identity of a preparation task, never a run ID or execution authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PreparationTaskId(String);
impl PreparationTaskId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedPreparationTask {
    pub session_id: SessionId,
    pub task_id: PreparationTaskId,
    pub termination: TaskTermination,
}

/// A snapshot drain, not an ingress barrier or proof of durable settlement.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExecutionTaskDrain {
    /// Native tool-task joins and retained leftovers, independent of worker joins.
    pub resources: Vec<ToolResourceDrain>,
    /// Actual library background-task joins, separate from tool scopes.
    pub background_tasks: Vec<WorkerResourceDrain>,
    pub joined: Vec<JoinedExecutionTask>,
    /// Tasks still retained after the deadline, including newly started tasks.
    pub pending: Vec<RunId>,
    /// Preparation can own an admission whose response has not arrived yet.
    pub joined_preparations: Vec<JoinedPreparationTask>,
    pub pending_preparations: Vec<PreparationTaskId>,
}

/// Local resource ownership for a tool call; never durable phase authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResourceDrain {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub phase: Option<crabber_session::PhaseTarget>,
    pub resources: crabber_extension::ResourceDrain,
}
struct OwnedToolResources {
    session_id: SessionId,
    run_id: RunId,
    call_id: ToolCallId,
    phase: Option<crabber_session::PhaseTarget>,
    owner: crabber_extension::ToolResourceOwner,
    worker: Option<WorkerResourceId>,
}
#[derive(Default, Clone)]
pub(crate) struct ExecutionTasks {
    owners: Arc<Mutex<ExecutionOwners>>,
}
#[derive(Default)]
struct ExecutionOwners {
    tasks: BTreeMap<RunId, Arc<ExecutionTask>>,
    resources: Vec<Arc<OwnedToolResources>>,
    background_tasks: Vec<Arc<OwnedBackgroundResources>>,
}

/// Local ownership of one actual worker's resources; never native authority.
#[derive(Clone)]
pub(crate) struct WorkerResources {
    tasks: ExecutionTasks,
    id: WorkerResourceId,
    session_id: SessionId,
    run_id: RunId,
}
#[derive(Clone)]
struct WorkerResourceId(Arc<()>);
impl WorkerResources {
    pub(crate) fn tool_resources(
        &self,
        call_id: ToolCallId,
        phase: Option<crabber_session::PhaseTarget>,
    ) -> crabber_extension::ToolResourceOwner {
        self.tasks.tool_resources_for_worker(
            self.session_id.clone(),
            self.run_id.clone(),
            call_id,
            phase,
            Some(self.id.clone()),
        )
    }
    pub(crate) fn run_matches(&self, run: &RunId) -> bool {
        self.run_id == *run
    }
    pub(crate) fn matches(&self, session: &SessionId, run: &RunId) -> bool {
        self.session_id == *session && self.run_id == *run
    }
    pub(crate) async fn join(&self, bound: Duration) -> bool {
        self.tasks
            .join_worker_resources(&self.run_id, Some(&self.id), bound)
            .await
    }
    pub(crate) async fn finish(&self) -> Result<(), ()> {
        let (tools, background) = futures::join!(
            self.tasks
                .finish_worker_resources(&self.run_id, Some(&self.id)),
            self.finish_background(),
        );
        tools.and(background)
    }
}

pub(crate) struct ExecutionTask {
    session_id: SessionId,
    local_id: RunId,
    correlation: Mutex<TaskCorrelation>,
    join: tokio::sync::Mutex<JoinState>,
}
enum JoinState {
    Running(JoinHandle<TaskTermination>),
    Joined(TaskTermination),
}
impl ExecutionTask {
    async fn join(&self) -> TaskTermination {
        let mut state = self.join.lock().await;
        let termination = match &mut *state {
            JoinState::Joined(termination) => return *termination,
            JoinState::Running(handle) => match handle.await {
                Ok(termination) => termination,
                Err(error) if error.is_cancelled() => TaskTermination::Cancelled,
                Err(error) => {
                    // A panic payload may itself panic on Drop. Never let it
                    // unwind through the owner's reconciliation path.
                    let payload = error.into_panic();
                    if let Err(nested) =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
                    {
                        std::mem::forget(nested);
                    }
                    TaskTermination::Panicked
                }
            },
        };
        *state = JoinState::Joined(termination);
        self.correlation.lock().unwrap().joined = true;
        termination
    }
}
impl ExecutionTasks {
    pub(crate) fn worker_resources(&self, session_id: SessionId, run_id: RunId) -> WorkerResources {
        WorkerResources {
            tasks: self.clone(),
            id: WorkerResourceId(Arc::new(())),
            session_id,
            run_id,
        }
    }
    #[cfg(test)]
    pub(crate) fn tool_resources(
        &self,
        session_id: SessionId,
        run_id: RunId,
        call_id: ToolCallId,
        phase: Option<crabber_session::PhaseTarget>,
    ) -> crabber_extension::ToolResourceOwner {
        self.tool_resources_for_worker(session_id, run_id, call_id, phase, None)
    }
    fn tool_resources_for_worker(
        &self,
        session_id: SessionId,
        run_id: RunId,
        call_id: ToolCallId,
        phase: Option<crabber_session::PhaseTarget>,
        worker: Option<WorkerResourceId>,
    ) -> crabber_extension::ToolResourceOwner {
        let owner = crabber_extension::ToolResourceOwner::new();
        self.owners
            .lock()
            .unwrap()
            .resources
            .push(Arc::new(OwnedToolResources {
                session_id,
                run_id,
                call_id,
                phase,
                owner: owner.clone(),
                worker,
            }));
        owner
    }
    async fn drain_resources(&self, session: Option<&SessionId>, deadline: tokio::time::Instant) {
        let scopes = self
            .owners
            .lock()
            .unwrap()
            .resources
            .iter()
            .filter(|scope| session.is_none_or(|id| scope.session_id == *id))
            .cloned()
            .collect::<Vec<_>>();
        let mut joins = FuturesUnordered::new();
        for scope in scopes {
            joins.push(async move {
                if scope.owner.snapshot().sealed {
                    scope
                        .owner
                        .drain(deadline.saturating_duration_since(tokio::time::Instant::now()))
                        .await;
                }
            });
        }
        while joins.next().await.is_some() {}
    }
    /// Called after the worker has dropped every tool future. This joins only
    /// that run's sealed resource scopes, never the worker awaiting this method.
    async fn join_worker_resources(
        &self,
        run: &RunId,
        worker: Option<&WorkerResourceId>,
        bound: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + bound;
        let scopes = self
            .owners
            .lock()
            .unwrap()
            .resources
            .iter()
            .filter(|scope| {
                scope.run_id == *run
                    && worker.is_none_or(|id| {
                        scope
                            .worker
                            .as_ref()
                            .is_some_and(|actual| Arc::ptr_eq(&actual.0, &id.0))
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        for scope in &scopes {
            if !scope.owner.snapshot().sealed {
                return false;
            }
            scope
                .owner
                .drain(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await;
        }
        // Recheck under the ownership lock so a late scope cannot be omitted.
        self.owners
            .lock()
            .unwrap()
            .resources
            .iter()
            .filter(|scope| {
                scope.run_id == *run
                    && worker.is_none_or(|id| {
                        scope
                            .worker
                            .as_ref()
                            .is_some_and(|actual| Arc::ptr_eq(&actual.0, &id.0))
                    })
            })
            .all(|scope| {
                let report = scope.owner.snapshot();
                report.sealed && report.pending == 0 && report.failures.is_empty()
            })
    }
    /// Close every actual registered resource scope after its main worker join.
    /// Closing only signals/seals; the drain still awaits actual task joins.
    #[cfg(test)]
    pub(crate) async fn finish_run_resources(&self, run: &RunId) -> Result<(), ()> {
        self.finish_worker_resources(run, None).await
    }
    async fn finish_worker_resources(
        &self,
        run: &RunId,
        worker: Option<&WorkerResourceId>,
    ) -> Result<(), ()> {
        loop {
            let scopes = self
                .owners
                .lock()
                .unwrap()
                .resources
                .iter()
                .filter(|scope| {
                    scope.run_id == *run
                        && worker.is_none_or(|id| {
                            scope
                                .worker
                                .as_ref()
                                .is_some_and(|actual| Arc::ptr_eq(&actual.0, &id.0))
                        })
                })
                .cloned()
                .collect::<Vec<_>>();
            // Signal the complete captured set before waiting on any one
            // task; cleanup may depend on another scope receiving its close.
            for scope in &scopes {
                scope.owner.close();
            }
            let mut pending = false;
            for scope in scopes {
                let report = scope.owner.drain(Duration::from_secs(5)).await;
                if !report.failures.is_empty() {
                    return Err(());
                }
                pending |= report.pending != 0;
            }
            if !pending
                && self
                    .join_worker_resources(run, worker, Duration::ZERO)
                    .await
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }
    #[cfg(test)]
    pub(crate) fn spawn(
        &self,
        session_id: SessionId,
        run_id: RunId,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        self.spawn_scoped(session_id, Some(run_id), work)
    }
    pub(crate) fn spawn_preparation(
        &self,
        session_id: SessionId,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        self.spawn_scoped(session_id, None, work)
    }
    /// Retain the supervisor join and the actual inner worker's observation.
    pub(crate) fn spawn_reporting(
        &self,
        session_id: SessionId,
        run_id: Option<RunId>,
        work: impl Future<Output = TaskTermination> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        let mut owners = self.owners.lock().unwrap();
        let task = Arc::new(ExecutionTask {
            session_id,
            local_id: RunId::new(),
            correlation: Mutex::new(TaskCorrelation {
                run_id,
                joined: false,
            }),
            join: tokio::sync::Mutex::new(JoinState::Running(tokio::spawn(work))),
        });
        owners.tasks.insert(task.local_id.clone(), task.clone());
        task
    }
    fn spawn_scoped(
        &self,
        session_id: SessionId,
        run_id: Option<RunId>,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        self.spawn_reporting(session_id, run_id, async move {
            work.await;
            TaskTermination::Returned
        })
    }
    pub(crate) async fn join(&self, task: &Arc<ExecutionTask>) -> TaskTermination {
        let termination = task.join().await;
        let mut owners = self.owners.lock().unwrap();
        if owners
            .tasks
            .get(&task.local_id)
            .is_some_and(|retained| Arc::ptr_eq(retained, task))
        {
            owners.tasks.remove(&task.local_id);
        }
        termination
    }
    fn pending(
        &self,
        session: Option<&SessionId>,
    ) -> (
        Vec<RunId>,
        Vec<PreparationTaskId>,
        Vec<ToolResourceDrain>,
        Vec<WorkerResourceDrain>,
    ) {
        // Preparation -> worker -> tool-resource handoffs are observed under
        // one lock, so a deadline cannot miss both the former and new owner.
        let mut owners = self.owners.lock().unwrap();
        let mut runs = Vec::new();
        let mut preparations = Vec::new();
        for task in owners
            .tasks
            .values()
            .filter(|task| session.is_none_or(|id| task.session_id == *id))
        {
            if let Some(run) = &task.correlation.lock().unwrap().run_id {
                runs.push(run.clone());
            } else {
                preparations.push(PreparationTaskId(task.local_id.0.clone()));
            }
        }
        let resources = owners
            .resources
            .iter()
            .filter(|scope| session.is_none_or(|id| scope.session_id == *id))
            .map(|scope| ToolResourceDrain {
                session_id: scope.session_id.clone(),
                run_id: scope.run_id.clone(),
                call_id: scope.call_id.clone(),
                phase: scope.phase.clone(),
                resources: scope.owner.snapshot(),
            })
            .collect::<Vec<_>>();
        owners.resources.retain(|scope| {
            let report = scope.owner.snapshot();
            !report.sealed || report.pending != 0 || !report.failures.is_empty()
        });
        let background_tasks = background::snapshots(&mut owners, session);
        (runs, preparations, resources, background_tasks)
    }
    pub(crate) async fn drain(&self, bound: Duration) -> ExecutionTaskDrain {
        self.drain_scoped(None, bound).await
    }
    pub(crate) async fn drain_session(
        &self,
        session: &SessionId,
        bound: Duration,
    ) -> ExecutionTaskDrain {
        self.drain_scoped(Some(session), bound).await
    }
    async fn drain_scoped(
        &self,
        session: Option<&SessionId>,
        bound: Duration,
    ) -> ExecutionTaskDrain {
        let deadline = tokio::time::Instant::now() + bound;
        let tasks = self
            .owners
            .lock()
            .unwrap()
            .tasks
            .values()
            .filter(|task| session.is_none_or(|id| task.session_id == *id))
            .cloned()
            .collect::<Vec<_>>();
        let mut joins = tasks
            .into_iter()
            .map(|task| async move {
                let termination = self.join(&task).await;
                let run_id = task.correlation.lock().unwrap().run_id.clone();
                (
                    task.session_id.clone(),
                    run_id,
                    PreparationTaskId(task.local_id.0.clone()),
                    termination,
                )
            })
            .collect::<FuturesUnordered<_>>();
        let mut report = ExecutionTaskDrain::default();
        let _ = tokio::time::timeout(bound, async {
            while let Some((session_id, run_id, task_id, termination)) = joins.next().await {
                if let Some(run_id) = run_id {
                    report.joined.push(JoinedExecutionTask {
                        session_id,
                        run_id,
                        termination,
                    });
                } else {
                    report.joined_preparations.push(JoinedPreparationTask {
                        session_id,
                        task_id,
                        termination,
                    });
                }
            }
        })
        .await;
        report.joined.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        report
            .joined_preparations
            .sort_by(|a, b| a.task_id.cmp(&b.task_id));
        self.drain_resources(session, deadline).await;
        self.drain_background(session, deadline).await;
        (
            report.pending,
            report.pending_preparations,
            report.resources,
            report.background_tasks,
        ) = self.pending(session);
        report
    }
}

#[cfg(test)]
mod tests;
