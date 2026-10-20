//! One shared callback/observer body, independent of how its main is registered.
use super::{Orchestrator, RunHandle, RunId, RunResult, RuntimeError, SessionId, driver};
use crate::{
    execution_events::ExecutionEventObserver,
    execution_tasks::{ExecutionTask, WorkerResources},
};
use std::{future::Future, sync::Arc};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

pub(super) struct WorkerLaunch {
    pub handle: HandleParts,
    pub business: WorkerBusiness,
    pub resources: WorkerResources,
    pub complete: driver::WorkerCompletion,
}
pub(super) struct HandleParts {
    runtime: Orchestrator,
    session: SessionId,
    run: RunId,
    done: oneshot::Receiver<Result<RunResult, RuntimeError>>,
    completion: watch::Receiver<bool>,
    events: crate::RunEvents,
    pub cancellation: CancellationToken,
    resources: Arc<WorkerResources>,
}
impl HandleParts {
    pub(super) fn finish(self, task: Arc<ExecutionTask>) -> RunHandle {
        RunHandle {
            session_id: self.session,
            run_id: self.run,
            store: self.runtime.store,
            clock: self.runtime.clock,
            done: self.done,
            task,
            tasks: self.runtime.execution_tasks,
            worker_resources: self.resources,
            completion: self.completion,
            cancellation: self.cancellation,
            events: Some(self.events),
        }
    }
}
pub(super) struct WorkerBusiness {
    runtime: Orchestrator,
    observer: Arc<ExecutionEventObserver>,
    complete: driver::WorkerCompletion,
    cancellation: CancellationToken,
}
impl WorkerBusiness {
    pub(super) async fn run<F, W>(self, work: F)
    where
        F: FnOnce(Orchestrator, CancellationToken) -> W + Send + 'static,
        W: Future<Output = Result<RunResult, RuntimeError>> + Send + 'static,
    {
        let Self {
            mut runtime,
            observer,
            complete,
            cancellation,
        } = self;
        let original = runtime.root_scope.clone();
        let run = runtime
            .worker_resources
            .as_ref()
            .map(|resources| resources.run_id().clone());
        let guard = observer.guard();
        let callbacks = runtime
            .worker_resources
            .as_ref()
            .expect("worker resources installed before dispatch")
            .background_resources(crate::WorkerResourceKind::ExtensionCallbackInvocations);
        let callback_guard = callbacks.guard();
        let result = match crabber_extension::CallbackInvocationOwner::attach(callbacks.resources())
        {
            Ok(owner) => {
                let capability = owner.capability();
                runtime.worker_callbacks = Some(capability.clone());
                crabber_extension::with_callback_invocations(capability, async move {
                    let scope = runtime.root_scope.clone();
                    match scope {
                        Some(scope) => scope.scope(Box::pin(work(runtime, cancellation))).await,
                        None => Err(RuntimeError::HostClosed),
                    }
                })
                .await
            }
            Err(_) => Err(RuntimeError::PreparationRecoveryRequired),
        };
        if let (Some(original), Some(run), Err(error)) = (original, run, &result) {
            original.record_business_failure(&run, error);
        }
        drop(callback_guard);
        drop(guard);
        complete.publish(result);
        drop(observer);
    }
}
impl Orchestrator {
    pub(super) fn worker_launch(
        &self,
        session: SessionId,
        run: RunId,
    ) -> Result<WorkerLaunch, RuntimeError> {
        self.worker_launch_with_cancellation(
            session,
            run,
            self.root_scope.as_ref().map_or_else(
                CancellationToken::new,
                crate::execution_tasks::RootScope::cancellation,
            ),
        )
    }
    pub(super) fn worker_launch_with_cancellation(
        &self,
        session: SessionId,
        run: RunId,
        cancellation: CancellationToken,
    ) -> Result<WorkerLaunch, RuntimeError> {
        let (sender, done) = oneshot::channel();
        let (complete, completion) = watch::channel(false);
        let parent = self.root_scope.as_ref().ok_or(RuntimeError::HostClosed)?;
        let resources =
            self.execution_tasks
                .worker_resources_in(parent, session.clone(), run.clone())?;
        let mut runtime = self.clone();
        runtime.worker_resources = Some(resources.clone());
        runtime.root_scope = resources.root_scope().cloned();
        let (observer, events) = ExecutionEventObserver::channel(
            runtime.observer.clone(),
            session.clone(),
            run.clone(),
            completion.clone(),
        );
        runtime.observer = observer.clone();
        let complete = driver::WorkerCompletion::new(sender, complete);
        Ok(WorkerLaunch {
            handle: HandleParts {
                runtime: self.clone(),
                session,
                run,
                done,
                completion,
                events,
                cancellation: cancellation.clone(),
                resources: Arc::new(resources.clone()),
            },
            business: WorkerBusiness {
                runtime,
                observer,
                complete: complete.clone(),
                cancellation,
            },
            resources,
            complete,
        })
    }
}
