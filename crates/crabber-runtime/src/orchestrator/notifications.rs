//! Interrupted notifications can outlive settlement, but retain their actual joins.
use super::{Orchestrator, RuntimeError};
use crabber_core::EventRecord;
use crabber_extension::{EventPublished, RunPlan, ToolSettled};
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
                if let Some(sink) = state_sink {
                    crabber_extension::with_state_sink(sink, delivery).await;
                } else {
                    delivery.await;
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
