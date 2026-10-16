//! Retain checkpoints locally until every started sibling and resource is joined.
#[path = "tool_suspension/commit.rs"]
mod commit;
#[path = "tool_suspension/freeze.rs"]
mod freeze;
use super::{Orchestrator, RuntimeError};
use super::{tool_batch::Batch, tool_phase::OwnedPhase};
use crabber_core::{
    ContinuationGeneration, ContinuationId, ControlId, InputFingerprint, InteractionId,
    PhaseGeneration, PhaseId, ResumableToolIdentity, RunId, ToolCallRecord, ToolSuspension,
};
use crabber_session::{
    AttemptTarget, CallApproval, ContinuationTarget, FrozenToolPhase, GrantedApproval,
    GrantedDecision, PhaseTarget, PreparedApproval, RequiredToolInteraction,
    ToolContinuationError as Error, ToolContinuationSuspension, ToolSuspensionAuthority,
};
#[derive(Clone)]
pub(super) struct PendingSuspension {
    owner: OwnedPhase,
    record: ToolCallRecord,
    identity: ResumableToolIdentity,
    proposal: ToolSuspension,
}
impl Orchestrator {
    pub(super) fn has_tool_suspension(&self) -> bool {
        !self
            .tool_suspensions
            .lock()
            .expect("tool suspensions poisoned")
            .is_empty()
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn retain_tool_suspension(
        &self,
        run: &RunId,
        record: &ToolCallRecord,
        owner: &OwnedPhase,
        identity: ResumableToolIdentity,
        proposal: ToolSuspension,
        resources: &crabber_extension::ToolResourceOwner,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<(), RuntimeError> {
        identity.validate()?;
        if !identity.accepts(proposal.checkpoint()) {
            return Err(Error::SchemaMismatch.into());
        }
        let cleanup = resources.drain(super::attempt::RESOURCE_JOIN_BOUND).await;
        if !cleanup.sealed || cleanup.pending != 0 || !cleanup.failures.is_empty() {
            return Err(RuntimeError::AttemptCleanupIncomplete {
                run_id: run.clone(),
            });
        }
        if cancel.is_cancelled() {
            return Err(RuntimeError::Interrupted);
        }
        self.tool_suspensions
            .lock()
            .expect("tool suspensions poisoned")
            .push(PendingSuspension {
                owner: owner.clone(),
                record: record.clone(),
                identity,
                proposal,
            });
        Ok(())
    }
    pub(super) async fn fail_tool_suspensions(&self, scope: &Batch<'_>) -> RuntimeError {
        let pending = self
            .tool_suspensions
            .lock()
            .expect("tool suspensions poisoned")
            .clone();
        let mut error = RuntimeError::TaskStopped;
        for suspension in pending {
            error = self
                .stopped_tool_phase(
                    scope.execution,
                    &scope.session.id,
                    scope.run,
                    &suspension.owner,
                    true,
                )
                .await;
        }
        error
    }
    pub(super) async fn commit_tool_suspensions(
        &self,
        scope: &Batch<'_>,
        approvals: Vec<GrantedApproval>,
    ) -> Result<(), RuntimeError> {
        if scope.cancellation.is_cancelled() {
            return Err(self.fail_tool_suspensions(scope).await);
        }
        if !self
            .execution_tasks
            .join_run_resources(scope.run, super::attempt::RESOURCE_JOIN_BOUND)
            .await
        {
            return Err(RuntimeError::AttemptCleanupIncomplete {
                run_id: scope.run.clone(),
            });
        }
        let (batch, authority) = self.freeze_tool_suspensions(scope, approvals).await?;
        let pause = self
            .persist_tool_suspension(scope, batch, authority)
            .await?;
        self.observer.emit(&pause);
        scope
            .plan
            .dispatcher
            .notify::<crabber_extension::EventPublished>(
                serde_json::to_value(&pause).unwrap_or(serde_json::Value::Null),
            )
            .await;
        Err(RuntimeError::Paused)
    }
}
