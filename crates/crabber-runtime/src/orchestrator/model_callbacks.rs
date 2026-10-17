//! The actual model owner spans stream creation, polling and original disposal.
use super::{
    Callback, CancellationToken, ExtensionModelStream, Orchestrator, RunId, RunPlan, RuntimeError,
    Value, attempt,
};
use futures::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe};

impl Orchestrator {
    pub(super) async fn with_model_callback_owner<
        T,
        F: Future<Output = Result<T, RuntimeError>>,
    >(
        &self,
        run: &RunId,
        construct: impl FnOnce() -> F,
    ) -> Result<T, RuntimeError> {
        let worker = self
            .worker_resources
            .as_ref()
            .filter(|worker| worker.run_matches(run))
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let resources =
            worker.background_resources(crate::WorkerResourceKind::ModelCallbackInvocations);
        let guard = resources.guard();
        let owner = crabber_extension::CallbackInvocationOwner::attach(resources.resources())
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        let result = crabber_extension::with_callback_invocations(owner.capability(), async {
            let mut original = Box::pin(construct());
            let outcome = AssertUnwindSafe(original.as_mut()).catch_unwind().await;
            // Ready is not disposal. Drop the SAME original future while its
            // caller capability and surrounding business task locals are live.
            let disposed = std::panic::catch_unwind(AssertUnwindSafe(|| drop(original)));
            finish_original(outcome, disposed)
        })
        .await;
        drop(owner);
        drop(guard);
        let cleanup = resources.drain(attempt::RESOURCE_JOIN_BOUND).await;
        if !cleanup.sealed || cleanup.pending != 0 || !cleanup.failures.is_empty() {
            return Err(RuntimeError::AttemptCleanupIncomplete {
                run_id: run.clone(),
            });
        }
        worker.reap_model_callbacks();
        result
    }

    pub(super) async fn dispatch_model_callbacks(
        &self,
        plan: &RunPlan,
        cancellation: &CancellationToken,
        input: Value,
        terminal: Callback,
    ) -> Result<Result<Value, crabber_extension::ExtensionError>, RuntimeError> {
        tokio::select! {
            () = cancellation.cancelled() => Err(RuntimeError::Interrupted),
            result = plan.dispatcher.around::<ExtensionModelStream>(input, terminal) => Ok(result),
        }
    }
}
type Panic = Box<dyn std::any::Any + Send>;
fn finish_original<T>(outcome: Result<T, Panic>, disposed: Result<(), Panic>) -> T {
    match (outcome, disposed) {
        (Ok(result), Ok(())) => result,
        (Ok(_), Err(payload)) => std::panic::resume_unwind(payload),
        (Err(payload), disposed) => {
            if let Err(secondary) = disposed
                && let Err(nested) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(secondary)))
            {
                std::mem::forget(nested);
            }
            std::panic::resume_unwind(payload)
        }
    }
}
