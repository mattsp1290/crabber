//! Actual worker completion precedes native ownership retirement.
mod control;
pub(super) mod registration;
mod resources;
mod retirement;
use super::{RunResult, RuntimeError};
use crate::execution_tasks::WorkerResources;
use crabber_session::{
    Store,
    session_owner::owned_scope::{
        OwnedCleanupTask, OwnedScopeActivationOutcome, OwnedTaskRegistration,
    },
};
use futures::FutureExt;
pub(super) use registration::Request;
use std::{
    future::Future,
    sync::{Arc, Mutex},
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
    resources: WorkerResources,
    request: Request,
    cancellation: CancellationToken,
    completion: WorkerCompletion,
    work: F,
) -> Result<(), RuntimeError>
where
    F: FnOnce() -> W + Send + 'static,
    W: Future<Output = ()> + Send + 'static,
{
    let run = request.run_id().clone();
    let grant = request.register(store.as_ref()).await?;
    let mut main = OwnedTaskRegistration::register(grant.scope_source()?, work)?;
    // The registered task stays outside every untrusted Store future's unwind
    // boundary. An adapter panic cannot drop/detach its actual join handle.
    let activation = store_call(async {
        store
            .activate_owned_scope(
                main.activation().clone(),
                grant.authority().clone(),
                Some(grant.parent()),
            )
            .await
    })
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
    let stop_control = CancellationToken::new();
    // Keep an in-flight Store future alive across main/cancel selection. Close
    // can progress concurrently, but retirement awaits this original future.
    let mut polling = Box::pin(control::supervise(
        store.as_ref(),
        grant.authority(),
        &cancellation,
        &stop_control,
    ));
    let control_done = tokio::select! {
        () = cancellation.cancelled() => false,
        joined = main.join() => { joined?; false },
        result = &mut polling => {
            if result.is_err() { cancellation.cancel(); }
            true
        },
    };
    stop_control.cancel();
    // This is an independently retained, actual cleanup task. Main completion,
    // empty snapshots and native terminal publications cannot substitute for it.
    let failed_run = run.clone();
    let (main_joined, observe_main) = oneshot::channel();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        main.activation().clone(),
        tokio::spawn(resources::finish(resources, observe_main)),
    );
    let (joined, ()) = futures::join!(
        async {
            let joined = main.join().await?;
            // Only the actual main join releases cleanup's final-drain barrier.
            let _ = main_joined.send(());
            Ok::<_, crabber_session::session_owner::SessionOwnerError>(joined)
        },
        async {
            if !control_done {
                // Keep polling the original Store future while main and actual
                // cleanup progress; neither replaces this completion.
                let _ = polling.await;
            }
        }
    );
    let joined = joined?;
    completion.observe_main(joined);
    let proof = joined
        .observe_resources(&mut cleanup)
        .await
        .map_err(|_| RuntimeError::AttemptCleanupIncomplete { run_id: failed_run })?;
    retirement::join(store.as_ref(), &grant, proof).await?;
    dispatch
}

/// Catch construction and polling inside the caller's async block. No task or
/// cleanup handle is moved into this boundary; callers retain physical owners.
async fn store_call<T>(
    call: impl Future<Output = Result<T, crabber_session::session_owner::SessionOwnerError>>,
) -> Result<T, crabber_session::session_owner::SessionOwnerError> {
    match std::panic::AssertUnwindSafe(call).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => {
            if let Err(nested) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
            {
                std::mem::forget(nested);
            }
            Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome)
        }
    }
}
