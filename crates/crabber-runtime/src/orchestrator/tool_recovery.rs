//! Original-claim recovery never restarts a possibly dispatched resume phase.
use super::tool_continuation::ToolResumeControl;
use super::{ContinueTool, Orchestrator, RuntimeError, ToolContinuation};
use crabber_core::{ControlId, Run, SessionRevision};
use crabber_session::{
    ToolContinuationError as Error, ToolContinuationOutcome, ToolContinuationReceipt,
    ToolContinuationRecovery,
};
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverTool {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub original: ContinueTool,
}
impl Orchestrator {
    async fn tool_recovery_request(
        &self,
        request: &RecoverTool,
        run: &Run,
    ) -> Result<ToolContinuationRecovery, RuntimeError> {
        self.tool_recovery_request_for_claim(request, request.original.claim(run)?)
            .await
    }
    async fn tool_recovery_request_for_claim(
        &self,
        request: &RecoverTool,
        claim: crabber_session::ToolContinuationClaim,
    ) -> Result<ToolContinuationRecovery, RuntimeError> {
        let original = self
            .lookup_tool_continuation(&request.original)
            .await?
            .ok_or(Error::NotFound)?;
        let ToolContinuationOutcome::Claimed { attempt, .. } = original.outcome else {
            return Err(Error::SemanticConflict.into());
        };
        Ok(ToolContinuationRecovery {
            id: request.id.clone(),
            expected_revision: request.expected_revision,
            original: claim,
            attempt: attempt.target,
            owner: crabber_core::RunId::new().to_string(),
            lease_ms: 30000,
        })
    }
    /// Reconcile original recovery semantics after any acknowledgement loss.
    /// # Errors
    /// Rejects changed original claims, foreign targets and unverifiable metadata.
    pub async fn lookup_tool_recovery(
        &self,
        request: &RecoverTool,
    ) -> Result<Option<ToolContinuationReceipt>, RuntimeError> {
        let Some(receipt) = self
            .store
            .lookup_tool_continuation(&request.original.session_id, &request.id)
            .await?
        else {
            return Ok(None);
        };
        if receipt.run_id != request.original.run_id
            || !matches!(&receipt.outcome, ToolContinuationOutcome::ReclaimedUnstarted { continuation, .. } if *continuation == request.original.continuation)
        {
            return Err(Error::SemanticConflict.into());
        }
        let original_plan = self
            .store
            .original_tool_continuation_plan(
                &request.original.session_id,
                &request.original.run_id,
                &request.original.continuation,
            )
            .await?;
        self.tool_recovery_request_for_claim(
            request,
            request.original.claim_with_plan(original_plan),
        )
        .await?
        .verify_receipt(&receipt)?;
        Ok(Some(receipt))
    }
    /// Explicitly reclaim an expired, proven Unstarted tool continuation owner.
    /// # Errors
    /// A Started parent or phase requires reconciliation and is never repeated.
    pub async fn recover_tool_continuation(
        &self,
        request: RecoverTool,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(receipt) = self.lookup_tool_recovery(&request).await? {
                return Ok(ToolContinuation::Replayed(receipt));
            }
            return Err(refusal);
        }
        self.own_preparation(
            request.original.session_id.clone(),
            move |runtime| async move { runtime.prepare_tool_recovery(request).await },
        )
        .await
    }
    async fn prepare_tool_recovery(
        &self,
        request: RecoverTool,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(receipt) = self.lookup_tool_recovery(&request).await? {
            return Ok(ToolContinuation::Replayed(receipt));
        }
        let run = self
            .store
            .get_run(&request.original.run_id)
            .await?
            .ok_or(Error::NotFound)?;
        let recovery = self.tool_recovery_request(&request, &run).await?;
        let validated = self.validated_session(&run).await?;
        self.spawn_tool_resume(run, validated, ToolResumeControl::Recover(recovery))
            .await
    }
}
