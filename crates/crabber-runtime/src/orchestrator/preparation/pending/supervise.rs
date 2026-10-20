//! Retire the original main only after its physical observation and resource join.
use super::{
    Arc, AtomicBool, CancellationToken, Mutex, Notice, Orchestrator, Ordering, PendingPreparation,
    RuntimeError, main, oneshot, publish, retained, start_cleanup,
};
use crate::{
    TaskTermination,
    execution_tasks::ExecutionTask,
    orchestrator::{driver, worker},
};
use crabber_session::session_owner::owned_scope::{OwnedCleanupTask, OwnedMainDisposition};
use std::sync::Mutex as StdMutex;
#[path = "supervise/launch.rs"]
mod launch;

pub(super) async fn run(
    runtime: Orchestrator,
    entry: retained::SharedEntry,
    pending: Arc<PendingPreparation>,
    main: Arc<Mutex<main::Main>>,
    notice: Arc<StdMutex<Option<oneshot::Sender<Notice>>>>,
    started: oneshot::Receiver<Arc<ExecutionTask>>,
    wrapper: oneshot::Sender<worker::WorkerBusiness>,
) -> TaskTermination {
    let Ok(supervisor) = started.await else {
        pending.stop();
        pending.failed.store(true, Ordering::Release);
        return TaskTermination::Returned;
    };
    let control = pending
        .control
        .get()
        .expect("control installed before supervisor")
        .clone();
    let launched =
        launch::prepare(&runtime, &pending, &control, &notice, supervisor, wrapper).await;
    let stop = CancellationToken::new();
    let cancellation = pending.cancellation.clone();
    let (control_done, observed_control) = oneshot::channel();
    let monitor = async {
        monitor(&runtime, &pending, &control, &cancellation, &stop).await;
        let _ = control_done.send(());
    };
    let retired = AtomicBool::new(false);
    let mut termination = TaskTermination::Returned;
    let observe = async {
        let bound = control.bound().await;
        let mut main = main.lock().await;
        // Do not take an early empty resource snapshot while main callbacks can
        // still register additional scopes. Cancellation may start closing early.
        tokio::select! {
            () = cancellation.cancelled() => (),
            observed = main.join() => { observed?; },
        }
        let (barrier, physically_joined) = oneshot::channel();
        let mut resources =
            resource_cleanup(launched.as_ref(), bound.as_ref().ok(), physically_joined);
        let observation = main.join().await?;
        let _ = barrier.send(());
        stop.cancel();
        pending.resources.close();
        start_cleanup(runtime.clone(), &pending, entry.clone());
        let disposition = observation.disposition;
        termination = match disposition {
            OwnedMainDisposition::Panicked => TaskTermination::Panicked,
            OwnedMainDisposition::Aborted => TaskTermination::Cancelled,
            _ => TaskTermination::Returned,
        };
        if let Some(launch) = &launched {
            launch.complete.observe_disposition(disposition);
        }
        if let Some(joined) = observation.main {
            let bound = bound?;
            let _ = observed_control.await;
            let proof = joined
                .observe_resources(
                    resources
                        .cleanup
                        .as_mut()
                        .ok_or(RuntimeError::PreparationRecoveryRequired)?,
                )
                .await
                .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
            driver::retirement::join(runtime.store.as_ref(), &bound.grant(), proof).await?;
            retired.store(true, Ordering::Release);
        } else {
            if observation.replayed.is_none() {
                pending.failed.store(true, Ordering::Release);
            }
            let _ = observed_control.await;
            if let Some(resources) = resources.unbound.take() {
                resources
                    .finish()
                    .await
                    .map_err(|()| RuntimeError::PreparationRecoveryRequired)?;
            }
        }
        if let Some(error) = main.take_error().await? {
            report_error(error, &notice, launched.as_ref());
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        if disposition != OwnedMainDisposition::Returned {
            let _ = publish(
                &notice,
                Notice::Failed(RuntimeError::PreparationRecoveryRequired),
            );
            if let Some(launch) = &launched {
                launch
                    .complete
                    .publish(Err(RuntimeError::PreparationRecoveryRequired));
            }
        }
        Ok::<_, RuntimeError>(())
    };
    let (observed, ()) = futures::join!(observe, monitor);
    finish_observation(
        observed.is_err(),
        retired.load(Ordering::Acquire),
        &pending,
        &notice,
        launched.as_ref(),
    );
    termination
}

fn finish_observation(
    failed: bool,
    retired: bool,
    pending: &PendingPreparation,
    notice: &StdMutex<Option<oneshot::Sender<Notice>>>,
    launched: Option<&launch::Launched>,
) {
    if failed {
        if !retired {
            pending.failed.store(true, Ordering::Release);
        }
        let _ = publish(
            notice,
            Notice::Failed(RuntimeError::PreparationRecoveryRequired),
        );
        if let Some(launch) = launched {
            launch
                .complete
                .publish(Err(RuntimeError::PreparationRecoveryRequired));
        }
    }
    pending.native_done.cancel();
}

async fn monitor(
    runtime: &Orchestrator,
    pending: &PendingPreparation,
    control: &main::Control,
    cancellation: &CancellationToken,
    stop: &CancellationToken,
) {
    let bound = tokio::select! {
        bound = control.bound() => bound,
        () = cancellation.cancelled() => { control.request_stop(); control.bound().await },
    };
    if let Ok(bound) = bound {
        let polling = driver::control::supervise(
            runtime.store.as_ref(),
            bound.authority(),
            cancellation,
            stop,
        );
        tokio::pin!(polling);
        tokio::select! {
            result = &mut polling => { if result.is_err() { pending.stop(); } },
            () = cancellation.cancelled() => {
                control.request_stop();
                stop.cancel();
                let _ = polling.await;
            }
        }
    }
}
fn report_error(
    error: crabber_session::session_owner::owned_scope::PendingSuccessorError,
    notice: &StdMutex<Option<oneshot::Sender<Notice>>>,
    launched: Option<&launch::Launched>,
) {
    use crabber_session::session_owner::owned_scope::PendingSuccessorError;
    let error = match error {
        PendingSuccessorError::Completion(error) => RuntimeError::from(error),
        PendingSuccessorError::Owner(error) => RuntimeError::from(error),
    };
    let error = match publish(notice, Notice::Failed(error)) {
        Some(Notice::Failed(error)) => error,
        _ => RuntimeError::PreparationRecoveryRequired,
    };
    if let Some(launch) = launched {
        launch.complete.publish(Err(error));
    }
}

struct ResourceCleanup {
    cleanup: Option<OwnedCleanupTask>,
    unbound: Option<crate::execution_tasks::WorkerResources>,
}
fn resource_cleanup(
    launched: Option<&launch::Launched>,
    bound: Option<&main::Bound>,
    physically_joined: oneshot::Receiver<()>,
) -> ResourceCleanup {
    match (bound, launched.map(|launch| launch.resources.clone())) {
        (Some(bound), Some(resources)) => ResourceCleanup {
            cleanup: Some(OwnedCleanupTask::from_host_task(
                bound.activation().clone(),
                tokio::spawn(driver::resources::finish(resources, physically_joined)),
            )),
            unbound: None,
        },
        (_, unbound) => ResourceCleanup {
            cleanup: None,
            unbound,
        },
    }
}
