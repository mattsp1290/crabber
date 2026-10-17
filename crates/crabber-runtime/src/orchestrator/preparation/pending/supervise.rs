use super::{
    Arc, AtomicBool, CancellationToken, Mutex, Notice, Orchestrator, Ordering, PendingExisting,
    PendingSuccessorReplay, PendingSuccessorTask, RuntimeError, oneshot, publish, retained,
    start_cleanup,
};
use crate::{TaskTermination, execution_tasks::WorkerResources, orchestrator::driver};
use crabber_session::session_owner::owned_scope::{OwnedCleanupTask, OwnedMainDisposition};
use std::sync::Mutex as StdMutex;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    runtime: Orchestrator,
    entry: retained::SharedEntry,
    pending: Arc<PendingExisting>,
    main: Arc<Mutex<PendingSuccessorTask<()>>>,
    resources: WorkerResources,
    complete: driver::WorkerCompletion,
    notice: Arc<StdMutex<Option<oneshot::Sender<Notice>>>>,
    started: oneshot::Receiver<()>,
    cancellation: CancellationToken,
) -> TaskTermination {
    let _ = started.await;
    let stop = CancellationToken::new();
    let control = pending
        .control
        .get()
        .expect("control installed before supervisor")
        .clone();
    let (control_done, observed_control) = oneshot::channel();
    let monitor = async {
        monitor(&runtime, &pending, &control, &notice, &cancellation, &stop).await;
        let _ = control_done.send(());
    };
    let retired = AtomicBool::new(false);
    let observe = async {
        // Start the actual resource drain while the original main is running.
        // Its final barrier is released only by the physical main observation.
        let bound = control.bound().await;
        let mut main = main.lock().await;
        // Signal scopes only after main completion or cooperative stop, as in
        // the ordinary driver. A pre-main empty tool drain could otherwise wait
        // on a callback while missing scopes registered by that callback.
        tokio::select! {
            () = cancellation.cancelled() => (),
            observed = main.join() => { observed?; },
        }
        let (barrier, physically_joined) = oneshot::channel();
        let mut resources = Some(resources);
        let mut cleanup = match bound {
            Ok(bound) => Some(OwnedCleanupTask::from_host_task(
                bound.activation().clone(),
                tokio::spawn(driver::resources::finish(
                    resources.take().unwrap(),
                    physically_joined,
                )),
            )),
            Err(_) => None,
        };
        let observation = main.join().await?;
        let _ = barrier.send(());
        stop.cancel();
        pending.resources.close();
        start_cleanup(runtime.clone(), &pending, entry.clone());
        if let Some(replayed) = observation.replayed() {
            pending.committed.store(true, Ordering::Release);
            let _ = publish(&notice, Notice::Replayed(replayed.clone()));
        }
        let disposition = observation.disposition();
        complete.observe_disposition(disposition);
        if let Some(joined) = observation.bound_main() {
            complete.observe_main(joined);
            let bound = control.bound().await?;
            let _ = observed_control.await;
            let proof = joined
                .observe_resources(
                    cleanup
                        .as_mut()
                        .ok_or(RuntimeError::PreparationRecoveryRequired)?,
                )
                .await
                .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
            driver::retirement::join(
                runtime.store.as_ref(),
                &driver::registration::Grant::Successor(Box::new(bound.grant().clone())),
                proof,
            )
            .await?;
            retired.store(true, Ordering::Release);
        } else {
            if observation.replayed().is_none() {
                pending.failed.store(true, Ordering::Release);
            }
            let _ = observed_control.await;
            resources
                .take()
                .ok_or(RuntimeError::PreparationRecoveryRequired)?
                .finish()
                .await
                .map_err(|()| RuntimeError::PreparationRecoveryRequired)?;
        }
        let error = main.take_error().await?;
        if let Some(error) = error {
            report_error(error, &notice, &complete);
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        publish_stopped(disposition, &notice, &complete);
        Ok::<_, RuntimeError>(())
    };
    let (observed, ()) = futures::join!(observe, monitor);
    if observed.is_err() {
        if !retired.load(Ordering::Acquire) {
            pending.failed.store(true, Ordering::Release);
        }
        let _ = publish(
            &notice,
            Notice::Failed(RuntimeError::PreparationRecoveryRequired),
        );
        complete.publish(Err(RuntimeError::PreparationRecoveryRequired));
    }
    pending.native_done.cancel();
    complete.termination()
}

async fn monitor(
    runtime: &Orchestrator,
    pending: &PendingExisting,
    control: &crabber_session::session_owner::owned_scope::PendingSuccessorControl,
    notice: &StdMutex<Option<oneshot::Sender<Notice>>>,
    cancellation: &CancellationToken,
    stop: &CancellationToken,
) {
    let completion = tokio::select! {
        completion = control.completion() => completion,
        () = cancellation.cancelled() => { control.request_stop(); control.completion().await },
    };
    if let Ok(metadata) = completion {
        if let crabber_session::session_owner::owned_scope::PendingSuccessorCompletion::Replayed(
            replayed,
        ) = &metadata
        {
            *pending.replayed.lock().unwrap() = Some(replayed.clone());
        }
        pending.committed.store(true, Ordering::Release);
        let _ = publish(notice, match metadata {
                crabber_session::session_owner::owned_scope::PendingSuccessorCompletion::Fresh(PendingSuccessorReplay::Admission(receipt)) => Notice::Fresh(Some(receipt)),
                crabber_session::session_owner::owned_scope::PendingSuccessorCompletion::Fresh(PendingSuccessorReplay::Expired(_)) => Notice::Fresh(None),
                crabber_session::session_owner::owned_scope::PendingSuccessorCompletion::Replayed(receipt) => Notice::Replayed(receipt),
            });
    }

    let bound = tokio::select! {
        bound = control.bound() => bound,
        () = cancellation.cancelled() => { control.request_stop(); control.bound().await },
    };
    if let Ok(bound) = bound {
        let polling = driver::control::supervise(
            runtime.store.as_ref(),
            bound.grant().authority(),
            cancellation,
            stop,
        );
        tokio::pin!(polling);
        tokio::select! {
            result = &mut polling => { if result.is_err() { pending.stop(); } },
            () = cancellation.cancelled() => {
                control.request_stop();
                stop.cancel();
                let _ = polling.await; // Keep the original pending Store future.
            }
        }
    }
}

fn report_error(
    error: crabber_session::session_owner::owned_scope::PendingSuccessorError,
    notice: &StdMutex<Option<oneshot::Sender<Notice>>>,
    complete: &driver::WorkerCompletion,
) {
    let error = match error {
        crabber_session::session_owner::owned_scope::PendingSuccessorError::Completion(error) => {
            RuntimeError::from(error)
        }
        crabber_session::session_owner::owned_scope::PendingSuccessorError::Owner(error) => {
            RuntimeError::from(error)
        }
    };
    let error = match publish(notice, Notice::Failed(error)) {
        Some(Notice::Failed(error)) => error,
        _ => RuntimeError::PreparationRecoveryRequired,
    };
    complete.publish(Err(error));
}

fn publish_stopped(
    disposition: OwnedMainDisposition,
    notice: &StdMutex<Option<oneshot::Sender<Notice>>>,
    complete: &driver::WorkerCompletion,
) {
    if disposition != OwnedMainDisposition::Returned {
        // Actual replay may already have delivered safe metadata above.
        let _ = publish(
            notice,
            Notice::Failed(RuntimeError::PreparationRecoveryRequired),
        );
        complete.publish(Err(RuntimeError::PreparationRecoveryRequired));
    }
}
