//! Actual worker completion precedes native ownership retirement.
mod control;
use super::{RunResult, RuntimeError};
use crate::execution_tasks::ExecutionTasks;
use crabber_session::{
    Store,
    session_owner::{
        driver::{ExecutionDriverOutcome, RegisterExecutionDriver},
        owned_scope::{
            OwnedCleanupFailure, OwnedCleanupTask, OwnedScopeActivationOutcome,
            OwnedScopeParentAuthority, OwnedTaskRegistration,
        },
    },
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

type ResultSender = oneshot::Sender<Result<RunResult, RuntimeError>>;
#[derive(Clone)]
pub(super) struct WorkerCompletion {
    sender: Arc<Mutex<Option<ResultSender>>>,
    completion: watch::Sender<bool>,
    main_termination: Arc<Mutex<Option<crate::TaskTermination>>>,
}
impl WorkerCompletion {
    pub(super) fn new(sender: ResultSender, completion: watch::Sender<bool>) -> Self {
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            completion,
            main_termination: Arc::default(),
        }
    }
    fn observe_main(
        &self,
        main: &crabber_session::session_owner::owned_scope::JoinedOwnedTask<()>,
    ) {
        use crabber_session::session_owner::owned_scope::OwnedMainDisposition;
        *self.main_termination.lock().unwrap() = Some(match main.disposition() {
            OwnedMainDisposition::Panicked => crate::TaskTermination::Panicked,
            OwnedMainDisposition::Aborted => crate::TaskTermination::Cancelled,
            OwnedMainDisposition::Returned | OwnedMainDisposition::NoDispatch => {
                crate::TaskTermination::Returned
            }
        });
    }
    pub(super) fn termination(&self) -> crate::TaskTermination {
        self.main_termination
            .lock()
            .unwrap()
            .unwrap_or(crate::TaskTermination::Returned)
    }
    pub(super) fn publish(&self, result: Result<RunResult, RuntimeError>) {
        if let Some(sender) = self.sender.lock().unwrap().take() {
            let _ = sender.send(result);
            let _ = self.completion.send(true);
        }
    }
}

pub(super) async fn run<F, W>(
    store: Arc<dyn Store>,
    resources: ExecutionTasks,
    request: RegisterExecutionDriver,
    cancellation: CancellationToken,
    completion: WorkerCompletion,
    work: F,
) -> Result<(), RuntimeError>
where
    F: FnOnce() -> W + Send + 'static,
    W: Future<Output = ()> + Send + 'static,
{
    let ExecutionDriverOutcome::Applied { grant, .. } =
        store.register_execution_driver(request.clone()).await?
    else {
        return Err(RuntimeError::PreparationRecoveryRequired);
    };
    let mut main = OwnedTaskRegistration::register(grant.scope_source()?, work)?;
    let activation = store
        .activate_owned_scope(
            main.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone())),
        )
        .await;
    let dispatch = match activation {
        Ok(OwnedScopeActivationOutcome::Applied { permit, .. }) => {
            main.dispatch(*permit).map_err(RuntimeError::from)
        }
        Ok(OwnedScopeActivationOutcome::Replayed(_)) => {
            Err(RuntimeError::PreparationRecoveryRequired)
        }
        Err(error) => Err(error.into()),
    };
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut observed = None;
    loop {
        tokio::select! {
            joined = main.join() => { joined?; break; }
            _ = ticker.tick() => control::poll(store.as_ref(), grant.authority(), &cancellation, &mut observed).await,
        }
    }
    // This is an independently retained, actual cleanup task. Main completion,
    // empty snapshots and native terminal publications cannot substitute for it.
    let run = request.fence.run_id;
    let failed_run = run.clone();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        main.activation().clone(),
        tokio::spawn(async move {
            resources
                .finish_run_resources(&run)
                .await
                .map_err(|()| OwnedCleanupFailure::Failed)
        }),
    );
    let joined = main.join().await?;
    completion.observe_main(joined);
    let proof = joined
        .observe_resources(&mut cleanup)
        .await
        .map_err(|_| RuntimeError::AttemptCleanupIncomplete { run_id: failed_run })?;
    let native = proof.request()?;
    let fingerprint = proof.fingerprint()?;
    let mut retry_wait = Duration::from_millis(50);
    loop {
        match store
            .join_owned_scope(proof.clone(), grant.authority().clone())
            .await
        {
            Ok(outcome) => {
                let (crabber_session::session_owner::SessionOwnerOutcome::Applied {
                    receipt,
                    authority: None,
                }
                | crabber_session::session_owner::SessionOwnerOutcome::Replayed(receipt)) = outcome
                else {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                };
                if receipt.operation != crabber_session::session_owner::SessionOwnerOperation::Join
                    || receipt.owner.state
                        != crabber_session::session_owner::SessionOwnerState::Joined
                    || receipt.owner.binding != grant.receipt().begin.owner.binding
                    || receipt.session_id != native.session_id
                    || receipt.operation_id != native.id
                    || receipt.owner.target != native.target
                    || receipt.semantic_digest != fingerprint
                {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                }
                break;
            }
            Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome) => {
                // Keep the original actual observations and authority. A retry
                // reconciles only this join and can never dispatch another main.
                tokio::time::sleep(retry_wait).await;
                retry_wait = (retry_wait * 2).min(Duration::from_secs(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
    dispatch
}
