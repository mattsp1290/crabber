//! Interrupted notifications can outlive settlement, but retain their actual joins.
use super::{Orchestrator, RuntimeError};
use crabber_core::EventRecord;
use crabber_extension::{
    CallbackInvocationOwner, EventPublished, ResourceFailure, RunPlan, ToolResourceOwner,
    ToolSettled, with_callback_invocations,
};
use serde_json::Value;
impl Orchestrator {
    pub(super) fn notify_interrupted_settlement(
        &self,
        plan: &RunPlan,
        event: &EventRecord,
        protected: bool,
    ) -> Result<(), RuntimeError> {
        let worker = self
            .worker_resources
            .as_ref()
            .filter(|worker| worker.matches(&event.session_id, &event.run_id))
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        // Each delivery has its own scope: early cleanup cannot seal ingress
        // for another sibling whose settlement has not committed yet.
        let plan = plan.clone();
        let state_sink = crabber_extension::current_state_sink();
        let projection = serde_json::to_value(event).unwrap_or(Value::Null);
        let registered = worker.spawn_background(
            crate::WorkerResourceKind::InterruptedSettlementNotification,
            async move {
                // The outer delivery may outlive worker/phase closure. Keep a
                // separate live inner ingress through actual delivery, then
                // propagate original callback join/disposal failures even though
                // Notify deliberately contains its ordinary callback errors.
                let callbacks = ToolResourceOwner::new();
                let guard = callbacks.guard();
                let owner = CallbackInvocationOwner::attach(callbacks.resources())
                    .map_err(|_| ResourceFailure::CleanupFailed)?;
                let delivery = async {
                    plan.dispatcher
                        .notify::<EventPublished>(projection.clone())
                        .await;
                    if protected {
                        plan.dispatcher.notify::<ToolSettled>(projection).await;
                    }
                };
                // Keep the original sink and plan through actual callback delivery;
                // terminal writes remain rejected by the original execution fence.
                with_callback_invocations(owner.capability(), async {
                    if let Some(sink) = state_sink {
                        crabber_extension::with_state_sink(sink, delivery).await;
                    } else {
                        delivery.await;
                    }
                })
                .await;
                drop(owner);
                drop(guard);
                let cleanup = callbacks.drain(super::attempt::RESOURCE_JOIN_BOUND).await;
                if !cleanup.sealed || cleanup.pending != 0 || !cleanup.failures.is_empty() {
                    return Err(ResourceFailure::CleanupFailed);
                }
                plan.release();
                Ok(())
            },
        );
        registered.map_err(|_| RuntimeError::AttemptCleanupIncomplete {
            run_id: event.run_id.clone(),
        })
    }
}
