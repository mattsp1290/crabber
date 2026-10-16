//! A dropped model waiter cannot certify attempt cleanup before callback Join.
use super::{
    Callback, CancellationToken, ExtensionModelStream, Orchestrator, RunId, RunPlan, RuntimeError,
    Value, attempt,
};

impl Orchestrator {
    pub(super) async fn dispatch_model_callbacks(
        &self,
        plan: &RunPlan,
        run: &RunId,
        cancellation: &CancellationToken,
        input: Value,
        terminal: Callback,
    ) -> Result<Result<Value, crabber_extension::ExtensionError>, RuntimeError> {
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
            tokio::select! {
                () = cancellation.cancelled() => Err(RuntimeError::Interrupted),
                result = plan.dispatcher.around::<ExtensionModelStream>(input, terminal) => Ok(result),
            }
        }).await;
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
}
