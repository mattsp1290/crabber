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
    result: Option<oneshot::Receiver<Result<(), RuntimeError>>>,
    lifetime: Arc<ObservationLifetime>,
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
        let task_lifetime = Arc::clone(&lifetime);
        let (sender, result) = oneshot::channel();
        let closed_result = Arc::new(Mutex::new(None));
        let retained_result = Arc::clone(&closed_result);
        owner
            .resources()
            .spawn(
                async move {
                    if let Some(result) =
                        watch(store, execution, fence, task_lifetime, closing).await
                    {
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
            result: Some(result),
            lifetime,
            _closed_result: closed_result,
        })
    }

    pub(in crate::orchestrator) async fn wait(&mut self) -> Result<(), RuntimeError> {
        let result = match self.result.as_mut() {
            Some(result) => result.await,
            None => return std::future::pending().await,
        };
        self.result = None;
        if !self.lifetime.active() || self.owner.snapshot().sealed {
            return std::future::pending().await;
        }
        result.map_err(|_| RuntimeError::TaskStopped)?
    }
}
impl Drop for ControlObservation {
    fn drop(&mut self) {
        self.owner.close();
    }
}

#[cfg_attr(
    test,
    tracing::instrument(name = "crabber.test.durable_cancel", skip_all, level = "debug")
)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (ControlObservation, ResultSender) {
        let (sender, receiver) = oneshot::channel();
        (
            ControlObservation {
                owner: ToolResourceOwner::new(),
                result: Some(receiver),
                lifetime: Arc::default(),
                _closed_result: Arc::default(),
            },
            sender,
        )
    }

    #[tokio::test]
    async fn cooperative_retirement_is_pending_but_unexpected_sender_loss_fails() {
        let (mut observation, sender) = fixture();
        observation.owner.close();
        sender.send(Ok(())).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), observation.wait())
                .await
                .is_err()
        );
        assert!(observation.result.is_none());
        let (mut observation, sender) = fixture();
        drop(sender);
        assert!(matches!(
            observation.wait().await,
            Err(RuntimeError::TaskStopped)
        ));
    }
    #[tokio::test]
    async fn losing_a_select_race_retains_the_original_control_receiver() {
        let (mut observation, sender) = fixture();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), observation.wait(),)
                .await
                .is_err()
        );
        assert!(observation.result.is_some());
        sender.send(Ok(())).unwrap();
        observation.wait().await.unwrap();
        assert!(observation.result.is_none());
    }
    #[tokio::test]
    async fn queued_results_cannot_supersede_quiescent_ack_or_cooperative_close() {
        for close_owner in [false, true] {
            for outcome in 0..3 {
                let (mut observation, sender) = fixture();
                match outcome {
                    0 => sender.send(Ok(())).unwrap(),
                    1 => sender.send(Err(RuntimeError::LeaseLost)).unwrap(),
                    _ => drop(sender),
                }
                // Queue while Active, then revoke observation before delivery.
                let lifetime = Arc::clone(&observation.lifetime);
                let _quiescent = if close_owner {
                    observation.owner.close();
                    None
                } else {
                    Some(lifetime.quiescent_commit())
                };
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(10), observation.wait(),)
                        .await
                        .is_err()
                );
                assert!(observation.result.is_none());
            }
        }
    }
}
