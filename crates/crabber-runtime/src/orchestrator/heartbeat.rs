//! Retain the original renewal/read future through cooperative shutdown.
use super::{ObservationLifetime, RuntimeError};
use crabber_core::{Clock, RunFence};
use crabber_session::{ExecutionStore, Store};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;

pub(super) struct HeartbeatGuard {
    owner: crabber_extension::ToolResourceOwner,
    pub(super) lost: Arc<AtomicBool>,
    pub(super) signal: watch::Receiver<bool>,
    _signal_sender: watch::Sender<bool>,
}

impl HeartbeatGuard {
    pub(super) fn start(
        execution: Arc<dyn ExecutionStore>,
        store: Arc<dyn Store>,
        fence: RunFence,
        clock: Arc<dyn Clock>,
        period: Duration,
        observation_lifetime: Arc<ObservationLifetime>,
        resources: &crate::execution_tasks::WorkerResources,
    ) -> Result<Self, RuntimeError> {
        if !resources.run_matches(&fence.run_id) {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        let owner = resources.background_resources(crate::WorkerResourceKind::LeaseHeartbeat);
        let closing = owner
            .resources()
            .closing()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let lost = Arc::new(AtomicBool::new(false));
        let lost_in_task = Arc::clone(&lost);
        let (sender, signal) = watch::channel(false);
        let task_sender = sender.clone();
        owner
            .resources()
            .spawn(async move {
                tokio::pin!(closing);
                let mut ticker =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        () = &mut closing => break,
                        _ = ticker.tick() => (),
                    }
                    if futures::poll!(&mut closing).is_ready() || !observation_lifetime.active() {
                        break;
                    }
                    if execution
                        .renew_lease(clock.now() + time::Duration::seconds(30))
                        .await
                        .is_err()
                    {
                        // A native pause can commit before its ACK returns. Recheck
                        // after the Store await before treating our own revocation as loss.
                        if futures::poll!(&mut closing).is_ready() || !observation_lifetime.active()
                        {
                            break;
                        }
                        let settled_by_this_owner = store
                            .get_run(&fence.run_id)
                            .await
                            .ok()
                            .flatten()
                            .is_some_and(|run| {
                                run.status.is_terminal() && run.claim_token == fence.claim_token
                            });
                        if settled_by_this_owner
                            || futures::poll!(&mut closing).is_ready()
                            || !observation_lifetime.active()
                        {
                            break;
                        }
                        lost_in_task.store(true, Ordering::SeqCst);
                        let _ = task_sender.send(true);
                        break;
                    }
                }
                Ok(())
            })
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        Ok(Self {
            owner,
            lost,
            signal,
            _signal_sender: sender,
        })
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.owner.close();
    }
}
