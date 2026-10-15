//! Local task ownership. These joins say nothing about durable outcome or
//! extension-owned resources; those must be reconciled independently.
use crabber_core::RunId;
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
    pub joined: Vec<JoinedExecutionTask>,
    /// Tasks still retained after the deadline, including newly started tasks.
    pub pending: Vec<RunId>,
    /// Preparation can own an admission whose response has not arrived yet.
    pub joined_preparations: Vec<JoinedPreparationTask>,
    pub pending_preparations: Vec<PreparationTaskId>,
}

#[derive(Default, Clone)]
pub(crate) struct ExecutionTasks {
    tasks: Arc<Mutex<BTreeMap<RunId, Arc<ExecutionTask>>>>,
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
        let mut tasks = self.tasks.lock().unwrap();
        let task = Arc::new(ExecutionTask {
            // Local bookkeeping only; never persisted or used as authority.
            local_id: RunId::new(),
            run_id,
            join: tokio::sync::Mutex::new(JoinState::Running(tokio::spawn(work))),
        });
        tasks.insert(task.local_id.clone(), task.clone());
        task
    }
    pub(crate) async fn join(&self, task: &Arc<ExecutionTask>) -> TaskTermination {
        let termination = task.join().await;
        let mut tasks = self.tasks.lock().unwrap();
        if tasks
            .get(&task.local_id)
            .is_some_and(|retained| Arc::ptr_eq(retained, task))
        {
            tasks.remove(&task.local_id);
        }
        termination
    }
    fn pending(&self) -> (Vec<RunId>, Vec<PreparationTaskId>) {
        // The preparation -> admitted-worker handoff must be observed under
        // one lock: separate reads could miss both owners during a deadline.
        let tasks = self.tasks.lock().unwrap();
        let mut runs = Vec::new();
        let mut preparations = Vec::new();
        for task in tasks.values() {
            if let Some(run) = &task.run_id {
                runs.push(run.clone());
            } else {
                preparations.push(PreparationTaskId(task.local_id.0.clone()));
            }
        }
        (runs, preparations)
    }
    pub(crate) async fn drain(&self, bound: Duration) -> ExecutionTaskDrain {
        let tasks = self
            .tasks
            .lock()
            .unwrap()
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
        (report.pending, report.pending_preparations) = self.pending();
        report
            .joined_preparations
            .sort_by(|a, b| a.task_id.cmp(&b.task_id));
        report
    }
}

#[cfg(test)]
mod tests;
