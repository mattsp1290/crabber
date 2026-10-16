//! One-time reporting metadata, independent of native authority and task identity.
use super::{ExecutionTask, ExecutionTasks, RunId, SessionId};
use std::sync::Arc;

pub(super) struct TaskCorrelation {
    pub(super) run_id: Option<RunId>,
    pub(super) joined: bool,
}

impl ExecutionTasks {
    /// Correlate the original retained supervisor after native admission chooses
    /// its actual `RunId`. This does not create, replace or join a task, and cannot
    /// authorize execution. Snapshot classification changes under the owners lock.
    /// Never lock the async join here: a drain may already be awaiting this task.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "initial preclaim Runtime handoff follows separately"
        )
    )]
    pub(crate) fn correlate_run(
        &self,
        task: &Arc<ExecutionTask>,
        session: &SessionId,
        run: &RunId,
    ) -> Result<(), ()> {
        let owners = self.owners.lock().unwrap();
        if task.session_id != *session
            || !owners
                .tasks
                .get(&task.local_id)
                .is_some_and(|retained| Arc::ptr_eq(retained, task))
        {
            return Err(());
        }
        let mut correlation = task.correlation.lock().unwrap();
        if correlation.joined {
            return Err(());
        }
        match &correlation.run_id {
            Some(original) if original != run => Err(()),
            Some(_) => Ok(()),
            None => {
                correlation.run_id = Some(run.clone());
                Ok(())
            }
        }
    }
}
