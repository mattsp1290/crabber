//! Never strand control transactions inside a blocked application callback.
use super::super::{ObservationLifetime, Orchestrator, RuntimeError};
use crabber_core::RunFence;
use crabber_extension::ToolResourceOwner;
use crabber_session::{ExecutionStore, RunControlError, Store};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tracing::Instrument;

type ResultSender = oneshot::Sender<Result<(), RuntimeError>>;

pub(in crate::orchestrator) struct ControlObservation {
    owner: ToolResourceOwner,
    result: oneshot::Receiver<Result<(), RuntimeError>>,
    // A cooperative host close stops observation without fabricating worker failure.
    _closed_result: Arc<Mutex<Option<ResultSender>>>,
}
impl ControlObservation {
    pub(in crate::orchestrator) fn start(
        runtime: &Orchestrator,
        execution: Arc<dyn ExecutionStore>,
        fence: RunFence,
    ) -> Result<Self, RuntimeError> {
        let resources = runtime
            .worker_resources
            .as_ref()
            .filter(|worker| worker.run_matches(&fence.run_id))
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let owner =
            resources.background_resources(crate::WorkerResourceKind::RunControlObservation);
        let closing = owner
            .resources()
            .closing()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let store = Arc::clone(&runtime.store);
        let lifetime = Arc::clone(&runtime.observation_lifetime);
        let (sender, result) = oneshot::channel();
        let closed_result = Arc::new(Mutex::new(None));
        let retained_result = Arc::clone(&closed_result);
        owner
            .resources()
            .spawn(
                async move {
                    if let Some(result) = watch(store, execution, fence, lifetime, closing).await {
                        let _ = sender.send(result);
                    } else {
                        // Keep this sender alive in the caller on ordinary close.
                        // A task panic still drops its local sender and fails closed.
                        *retained_result.lock().unwrap() = Some(sender);
                    }
                    Ok(())
                }
                .instrument(tracing::Span::current()),
            )
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        Ok(Self {
            owner,
            result,
            _closed_result: closed_result,
        })
    }

    pub(in crate::orchestrator) async fn wait(&mut self) -> Result<(), RuntimeError> {
        (&mut self.result)
            .await
            .map_err(|_| RuntimeError::TaskStopped)?
    }
}
impl Drop for ControlObservation {
    fn drop(&mut self) {
        self.owner.close();
    }
}

async fn watch(
    store: Arc<dyn Store>,
    execution: Arc<dyn ExecutionStore>,
    fence: RunFence,
    lifetime: Arc<ObservationLifetime>,
    closing: impl Future<Output = ()> + Send,
) -> Option<Result<(), RuntimeError>> {
    tokio::pin!(closing);
    let mut dormant = !store.supports_run_controls();
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if dormant || !lifetime.active() {
            // Keep the result sender alive while the main task awaits an
            // original terminal ACK; becoming quiescent is not worker failure.
            closing.await;
            return None;
        }
        tokio::select! {
            biased;
            () = &mut closing => return None,
            _ = timer.tick() => (),
        }
        if futures::poll!(&mut closing).is_ready() {
            return None;
        }
        if !lifetime.active() {
            continue;
        }
        // Close is cooperative: this SAME original I/O must finish before the
        // physically retained task joins, even if its caller already returned.
        let observed = execution.observe_run_controls(None).await;
        if futures::poll!(&mut closing).is_ready() {
            return None;
        }
        if !lifetime.active() {
            continue;
        }
        match observed {
            Ok(boundary) if boundary.cancel_observed => return Some(Ok(())),
            Ok(_) => (),
            Err(RunControlError::StaleOwner) => {
                let settled = store.get_run(&fence.run_id).await;
                if futures::poll!(&mut closing).is_ready() {
                    return None;
                }
                if !lifetime.active() {
                    continue;
                }
                if settled.ok().flatten().is_some_and(|run| {
                    run.id == fence.run_id
                        && run.claim_token == fence.claim_token
                        && run.status.is_terminal()
                }) {
                    dormant = true;
                } else {
                    return Some(Err(RuntimeError::LeaseLost));
                }
            }
            Err(error) => return Some(Err(error.into())),
        }
    }
}
