//! Set up a run only from the original pending task's authenticated metadata.
use super::{
    Arc, ExecutionTask, Notice, Orchestrator, Ordering, PendingPreparation, RuntimeError, StdMutex,
    driver, main, oneshot, publish, worker,
};
use crate::execution_tasks::WorkerResources;
pub(super) struct Launched {
    pub resources: WorkerResources,
    pub complete: driver::WorkerCompletion,
}
pub(super) async fn prepare(
    runtime: &Orchestrator,
    pending: &PendingPreparation,
    control: &main::Control,
    notice: &StdMutex<Option<oneshot::Sender<Notice>>>,
    supervisor: Arc<ExecutionTask>,
    wrapper: oneshot::Sender<worker::WorkerBusiness>,
) -> Option<Launched> {
    let completion = tokio::select! {
        completion = control.completion() => completion,
        () = pending.cancellation.cancelled() => {
            control.request_stop();
            control.completion().await
        },
    };
    let Ok(completion) = completion else {
        return None;
    };
    *pending.completion.lock().unwrap() = Some(completion.clone());
    pending.committed.store(true, Ordering::Release);
    if completion.replayed {
        let _ = publish(notice, Notice::Replayed(completion.metadata));
        return None;
    }
    let session = pending.grant.owner_authority().session_id();
    let run = completion.metadata.run_id();
    let worker::WorkerLaunch {
        handle,
        business,
        resources,
        complete,
    } = runtime.worker_launch_with_cancellation(
        session.clone(),
        run.clone(),
        pending.cancellation.clone(),
    );
    // This locks only reporting metadata, never the async join mutex: a drain
    // can already be awaiting this exact supervisor while admission chooses RunId.
    if runtime
        .execution_tasks
        .correlate_run(&supervisor, session, run)
        .is_err()
    {
        pending.stop();
        complete.publish(Err(RuntimeError::PreparationRecoveryRequired));
    } else if wrapper.send(business).is_err() {
        complete.publish(Err(RuntimeError::PreparationRecoveryRequired));
    }
    let _ = publish(
        notice,
        Notice::Fresh {
            handle: Box::new(handle),
            metadata: completion.metadata,
        },
    );
    Some(Launched {
        resources,
        complete,
    })
}
