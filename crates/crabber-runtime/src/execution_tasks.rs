//! Local task ownership. These joins say nothing about durable outcome or
//! extension-owned resources; those must be reconciled independently.
use crabber_core::{RunId, ToolCallId};
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
    pub task_id: PreparationTaskId,
    pub termination: TaskTermination,
}

/// A snapshot drain, not an ingress barrier or proof of durable settlement.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExecutionTaskDrain {
    /// Native tool-task joins and retained leftovers, independent of worker joins.
    pub resources: Vec<ToolResourceDrain>,
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
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub resources: crabber_extension::ResourceDrain,
}
struct OwnedToolResources {
    run_id: RunId,
    call_id: ToolCallId,
    owner: crabber_extension::ToolResourceOwner,
}
#[derive(Default, Clone)]
pub(crate) struct ExecutionTasks {
    owners: Arc<Mutex<ExecutionOwners>>,
}
#[derive(Default)]
struct ExecutionOwners {
    tasks: BTreeMap<RunId, Arc<ExecutionTask>>,
    resources: Vec<Arc<OwnedToolResources>>,
}

pub(crate) struct ExecutionTask {
    local_id: RunId,
    run_id: Option<RunId>,
    join: tokio::sync::Mutex<JoinState>,
}
enum JoinState {
    Running(JoinHandle<()>),
    Joined(TaskTermination),
}
impl ExecutionTask {
    async fn join(&self) -> TaskTermination {
        let mut state = self.join.lock().await;
        let termination = match &mut *state {
            JoinState::Joined(termination) => return *termination,
            JoinState::Running(handle) => match handle.await {
                Ok(()) => TaskTermination::Returned,
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
        termination
    }
}
impl ExecutionTasks {
    pub(crate) fn tool_resources(
        &self,
        run_id: RunId,
        call_id: ToolCallId,
    ) -> crabber_extension::ToolResourceOwner {
        let owner = crabber_extension::ToolResourceOwner::new();
        self.owners
            .lock()
            .unwrap()
            .resources
            .push(Arc::new(OwnedToolResources {
                run_id,
                call_id,
                owner: owner.clone(),
            }));
        owner
    }
    async fn drain_resources(&self, deadline: tokio::time::Instant) {
        let scopes = self.owners.lock().unwrap().resources.clone();
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
    pub(crate) fn spawn(
        &self,
        run_id: RunId,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        self.spawn_scoped(Some(run_id), work)
    }
    pub(crate) fn spawn_preparation(
        &self,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        self.spawn_scoped(None, work)
    }
    fn spawn_scoped(
        &self,
        run_id: Option<RunId>,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<ExecutionTask> {
        let mut owners = self.owners.lock().unwrap();
        let task = Arc::new(ExecutionTask {
            // Local bookkeeping only; never persisted or used as authority.
            local_id: RunId::new(),
            run_id,
            join: tokio::sync::Mutex::new(JoinState::Running(tokio::spawn(work))),
        });
        owners.tasks.insert(task.local_id.clone(), task.clone());
        task
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
    fn pending(&self) -> (Vec<RunId>, Vec<PreparationTaskId>, Vec<ToolResourceDrain>) {
        // Preparation -> worker -> tool-resource handoffs are observed under
        // one lock, so a deadline cannot miss both the former and new owner.
        let mut owners = self.owners.lock().unwrap();
        let mut runs = Vec::new();
        let mut preparations = Vec::new();
        for task in owners.tasks.values() {
            if let Some(run) = &task.run_id {
                runs.push(run.clone());
            } else {
                preparations.push(PreparationTaskId(task.local_id.0.clone()));
            }
        }
        let resources = owners
            .resources
            .iter()
            .map(|scope| ToolResourceDrain {
                run_id: scope.run_id.clone(),
                call_id: scope.call_id.clone(),
                resources: scope.owner.snapshot(),
            })
            .collect::<Vec<_>>();
        owners.resources = owners
            .resources
            .iter()
            .zip(&resources)
            .filter(|(_, report)| {
                !report.resources.sealed
                    || report.resources.pending != 0
                    || !report.resources.failures.is_empty()
            })
            .map(|(owner, _)| owner.clone())
            .collect();
        (runs, preparations, resources)
    }
    pub(crate) async fn drain(&self, bound: Duration) -> ExecutionTaskDrain {
        let deadline = tokio::time::Instant::now() + bound;
        let tasks = self
            .owners
            .lock()
            .unwrap()
            .tasks
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut joins = tasks
            .into_iter()
            .map(|task| async move {
                let termination = self.join(&task).await;
                (
                    task.run_id.clone(),
                    PreparationTaskId(task.local_id.0.clone()),
                    termination,
                )
            })
            .collect::<FuturesUnordered<_>>();
        let mut report = ExecutionTaskDrain::default();
        let _ = tokio::time::timeout(bound, async {
            while let Some((run_id, task_id, termination)) = joins.next().await {
                if let Some(run_id) = run_id {
                    report.joined.push(JoinedExecutionTask {
                        run_id,
                        termination,
                    });
                } else {
                    report.joined_preparations.push(JoinedPreparationTask {
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
        self.drain_resources(deadline).await;
        (
            report.pending,
            report.pending_preparations,
            report.resources,
        ) = self.pending();
        report
    }
}

#[cfg(test)]
mod tests;
