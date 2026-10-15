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

/// A snapshot drain, not an ingress barrier or proof of durable settlement.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExecutionTaskDrain {
    pub joined: Vec<JoinedExecutionTask>,
    /// Tasks still retained after the deadline, including newly started tasks.
    pub pending: Vec<RunId>,
}

#[derive(Default, Clone)]
pub(crate) struct ExecutionTasks {
    tasks: Arc<Mutex<BTreeMap<RunId, Arc<ExecutionTask>>>>,
}

pub(crate) struct ExecutionTask {
    local_id: RunId,
    run_id: RunId,
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
    pub(crate) fn pending(&self) -> Vec<RunId> {
        self.tasks
            .lock()
            .unwrap()
            .values()
            .map(|task| task.run_id.clone())
            .collect()
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
                JoinedExecutionTask {
                    run_id: task.run_id.clone(),
                    termination: self.join(&task).await,
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut report = ExecutionTaskDrain::default();
        let _ = tokio::time::timeout(bound, async {
            while let Some(joined) = joins.next().await {
                report.joined.push(joined);
            }
        })
        .await;
        report.joined.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        report.pending = self.pending();
        report
    }
}

#[cfg(test)]
mod tests;
