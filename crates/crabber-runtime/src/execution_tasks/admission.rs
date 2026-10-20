//! Atomic parked root/descendant registration.
use super::correlation::TaskCorrelation;
use super::enrollment::Member;
use super::{
    ExecutionTask, ExecutionTasks, JoinState, RootScope, RunId, SessionId, TaskTermination,
};
use crate::RuntimeError;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

pub(crate) struct AdmissionWait<T> {
    pub task: Arc<ExecutionTask>,
    pub result: oneshot::Receiver<Result<T, RuntimeError>>,
}
impl ExecutionTasks {
    pub(crate) fn admit<T, F, W>(
        &self,
        session: Option<SessionId>,
        build: F,
    ) -> Result<AdmissionWait<T>, RuntimeError>
    where
        T: Send + 'static,
        F: FnOnce(RootScope) -> W + Send + 'static,
        W: Future<Output = Result<T, RuntimeError>> + Send + 'static,
    {
        let (send, result) = oneshot::channel();
        let mut owners = self.owners.lock().unwrap();
        let scope = self.new_root(&mut owners)?;
        let task = retain(&mut owners, session, None, scope, move |scope| async move {
            let original = scope.clone();
            let outcome = build(scope).await;
            original.record_result(&outcome);
            let _ = send.send(outcome);
            TaskTermination::Returned
        });
        Ok(AdmissionWait { task, result })
    }
    pub(crate) fn spawn_reporting_in<F, W>(
        &self,
        parent: &RootScope,
        session: SessionId,
        run: Option<RunId>,
        build: F,
    ) -> Result<Arc<ExecutionTask>, RuntimeError>
    where
        F: FnOnce(RootScope) -> W + Send + 'static,
        W: Future<Output = TaskTermination> + Send + 'static,
    {
        if !parent.belongs_to(self) {
            return Err(RuntimeError::HostClosed);
        }
        self.refresh_resources();
        let mut owners = self.owners.lock().unwrap();
        parent.verify(&owners)?;
        let child = parent.insert(&mut owners, Member::Task);
        Ok(retain(&mut owners, Some(session), run, child, build))
    }
    pub(crate) fn worker_resources_in(
        &self,
        scope: &RootScope,
        session: SessionId,
        run: RunId,
    ) -> Result<super::WorkerResources, RuntimeError> {
        if !scope.belongs_to(self) {
            return Err(RuntimeError::HostClosed);
        }
        let mut resources = self.worker_resources(session, run);
        resources.scope = Some(scope.retain_resources(resources.id.clone())?);
        Ok(resources)
    }
}
fn retain<F, W>(
    owners: &mut super::ExecutionOwners,
    session: Option<SessionId>,
    run: Option<RunId>,
    scope: RootScope,
    build: F,
) -> Arc<ExecutionTask>
where
    F: FnOnce(RootScope) -> W + Send + 'static,
    W: Future<Output = TaskTermination> + Send + 'static,
{
    let (release, parked) = oneshot::channel();
    let current = scope.clone();
    let task = Arc::new(ExecutionTask {
        session_id: session,
        local_id: scope.member().clone(),
        scope: Some(scope),
        correlation: Mutex::new(TaskCorrelation {
            run_id: run,
            joined: false,
        }),
        join: tokio::sync::Mutex::new(JoinState::Running(tokio::spawn(async move {
            if parked.await.is_err() {
                return TaskTermination::Cancelled;
            }
            current.scope(build(current.clone())).await
        }))),
    });
    owners.tasks.insert(task.local_id.clone(), task.clone());
    let _ = release.send(());
    task
}
impl super::WorkerResources {
    pub(crate) fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub(crate) fn root_scope(&self) -> Option<&RootScope> {
        self.scope.as_ref()
    }
}
