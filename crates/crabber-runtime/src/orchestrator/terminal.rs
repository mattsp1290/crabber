//! Shared failure settlement for initial and resumed workers.
use super::{
    AtomicBool, EventPublished, ExecutionStore, OffsetDateTime, Orchestrator, Ordering, RunFence,
    RunPlan, RunResult, RunSettled, RunStatus, RuntimeError, SessionId, TurnId, Usage, Value,
    storable_error_text,
};

pub(super) struct FailureContext<'a> {
    pub execution: &'a dyn ExecutionStore,
    pub fence: &'a RunFence,
    pub session: &'a SessionId,
    pub plan: &'a RunPlan,
    pub started_at: OffsetDateTime,
    pub lost: &'a AtomicBool,
    pub observed_lost: &'a AtomicBool,
}
impl Orchestrator {
    pub(super) async fn finish_failed_run(
        &self,
        scope: FailureContext<'_>,
        error: RuntimeError,
        turn: Option<&TurnId>,
    ) -> Result<RunResult, RuntimeError> {
        let FailureContext {
            execution,
            fence,
            session,
            plan,
            started_at,
            lost,
            observed_lost,
        } = scope;
        let run = &fence.run_id;
        if matches!(error, RuntimeError::Paused) {
            return Ok(RunResult {
                session_id: session.clone(),
                run_id: run.clone(),
                status: RunStatus::Paused,
                usage: Usage::default(),
            });
        }
        // An unknown terminal write is reconciled from durable state, never
        // followed by a second attempt to manufacture another outcome.
        if matches!(
            error,
            RuntimeError::TerminalOutcomeUnknown { .. }
                | RuntimeError::ApprovalBoundaryUnknown { .. }
                | RuntimeError::AttemptBoundaryUnknown { .. }
                | RuntimeError::AttemptCleanupIncomplete { .. }
                | RuntimeError::ToolOutcomeUnknown { .. }
                | RuntimeError::PhaseOutcomeUnknown { .. }
        ) {
            return Err(error);
        }
        if lost.load(Ordering::SeqCst)
            || matches!(error, RuntimeError::LeaseLost)
            || self.verified_lease_loss(fence).await
        {
            observed_lost.store(true, Ordering::SeqCst);
            return Err(error);
        }
        if !self
            .execution_tasks
            .join_run_resources(run, super::attempt::RESOURCE_JOIN_BOUND)
            .await
        {
            return Err(RuntimeError::AttemptCleanupIncomplete {
                run_id: run.clone(),
            });
        }
        self.settle_unfinished_calls(execution, session, run, plan, true, turn)
            .await?;
        let status = if matches!(error, RuntimeError::Interrupted) {
            RunStatus::Interrupted
        } else {
            RunStatus::Failed
        };
        let settled = self
            .conclude_owned_attempt(
                execution,
                run,
                crabber_session::RunBoundary::Terminal {
                    status,
                    error: Some(storable_error_text(&error)),
                    usage: Usage::default(),
                },
                None,
                started_at,
            )
            .await?;
        self.observer.emit(&settled);
        let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(projection.clone())
            .await;
        plan.dispatcher.notify::<RunSettled>(projection).await;
        if status == RunStatus::Interrupted {
            Ok(RunResult {
                session_id: session.clone(),
                run_id: run.clone(),
                status,
                usage: Usage::default(),
            })
        } else {
            Err(error)
        }
    }
}
