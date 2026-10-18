//! External result sets enter the same owned continuation worker as native checkpoints.
use super::tool_continuation::ToolResumeControl;
use super::{ContinueTool, Orchestrator, RuntimeError, ToolContinuation};
use crabber_core::{ControlId, FrontendToolResult, Run, SessionRevision};
use crabber_session::{
    FrontendContinuationClaim, FrontendContinuationRecovery, FrontendResolverClaim,
    ToolContinuationError as Error, ToolContinuationOutcome, ToolContinuationReceipt,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinueFrontend {
    pub continuation: ContinueTool,
    pub resolver: FrontendResolverClaim,
    pub results: Vec<FrontendToolResult>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverFrontend {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub original: ContinueFrontend,
}
impl ContinueFrontend {
    fn claim(&self, run: &Run) -> Result<FrontendContinuationClaim, RuntimeError> {
        self.claim_from_native(self.continuation.claim(run)?)
    }
    fn claim_from_native(
        &self,
        claim: crabber_session::ToolContinuationClaim,
    ) -> Result<FrontendContinuationClaim, RuntimeError> {
        FrontendContinuationClaim::fingerprint_parts(&claim, &self.resolver, &self.results)?;
        Ok(FrontendContinuationClaim {
            claim,
            resolver: self.resolver.clone(),
            results: self.results.clone(),
        })
    }
}
impl Orchestrator {
    /// Check the complete external response set using only authoritative public
    /// metadata. This grants no authority and does not restore extensions.
    /// # Errors
    /// Rejects incomplete results, stale generations/revisions and bad schemas.
    pub async fn check_frontend_continuation(
        &self,
        request: &ContinueFrontend,
    ) -> Result<(), RuntimeError> {
        if !self.store.supports_frontend_tools() {
            return Err(Error::Unsupported.into());
        }
        let scope = &request.continuation;
        let run = self
            .store
            .get_run(&scope.run_id)
            .await?
            .ok_or(Error::NotFound)?;
        let claim = scope.claim(&run)?;
        // Validate the entire borrowed result set before materializing any
        // external output. Closed-host validation remains metadata only.
        FrontendContinuationClaim::fingerprint_parts(&claim, &request.resolver, &request.results)?;
        let view = self
            .store
            .tool_continuation_snapshot(&scope.session_id, &scope.run_id)
            .await?
            .ok_or(Error::NotFound)?;
        if view.revision != scope.expected_revision {
            return Err(Error::RevisionConflict.into());
        }
        if view.continuation != scope.continuation {
            return Err(Error::GenerationConflict.into());
        }
        if view.state != crabber_session::ToolContinuationState::Waiting
            || run.status != crabber_core::RunStatus::Paused
        {
            return Err(Error::InvalidTransition.into());
        }
        if view
            .interactions
            .iter()
            .any(|q| q.state == crabber_core::InteractionState::Pending || q.consumed)
        {
            return Err(Error::Pending.into());
        }
        if view.frontend_calls.is_empty() || view.frontend_calls.len() != request.results.len() {
            return Err(Error::SemanticConflict.into());
        }
        for frozen in &view.frontend_calls {
            if frozen.consumed {
                return Err(Error::InvalidTransition.into());
            }
            let result = request
                .results
                .iter()
                .find(|result| result.call_id() == &frozen.call_id)
                .ok_or(Error::SemanticConflict)?;
            crabber_session::validate_frontend_result(&frozen.descriptor, result)?;
        }
        Ok(())
    }
    /// Pure exact-key reconciliation, including after extension shutdown.
    /// # Errors
    /// Rejects changed external results, authority assertions or original scope.
    pub async fn lookup_frontend_continuation(
        &self,
        request: &ContinueFrontend,
    ) -> Result<Option<ToolContinuationReceipt>, RuntimeError> {
        let scope = &request.continuation;
        let Some(receipt) = self
            .store
            .lookup_tool_continuation(&scope.session_id, &scope.id)
            .await?
        else {
            return Ok(None);
        };
        if receipt.run_id != scope.run_id
            || !matches!(&receipt.outcome, ToolContinuationOutcome::FrontendClaimed { continuation, .. } if *continuation == scope.continuation)
        {
            return Err(Error::SemanticConflict.into());
        }
        let original_plan = self
            .store
            .original_tool_continuation_plan(&scope.session_id, &scope.run_id, &scope.continuation)
            .await?;
        let native = scope.claim_with_plan(original_plan);

        if receipt.semantic_digest
            != FrontendContinuationClaim::fingerprint_parts(
                &native,
                &request.resolver,
                &request.results,
            )?
        {
            return Err(Error::SemanticConflict.into());
        }
        Ok(Some(receipt))
    }
    /// Consume one complete authenticated external result set and start owned
    /// result protection. No backend tool execution middleware is invoked.
    /// # Errors
    /// Retain the exact original request after unknown acknowledgement.
    pub async fn continue_frontend(
        &self,
        request: ContinueFrontend,
    ) -> Result<ToolContinuation, RuntimeError> {
        self.continue_frontend_with_context(request, None).await
    }
    /// Trace data does not grant external-result authority.
    /// # Errors
    /// Returns the same authoritative eligibility and preparation failures.
    pub async fn continue_frontend_with_context(
        &self,
        request: ContinueFrontend,
        context: Option<crabber_core::TraceContext>,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(receipt) = self.lookup_frontend_continuation(&request).await? {
                return Ok(ToolContinuation::Replayed(receipt));
            }
            if matches!(refusal, RuntimeError::HostClosed) {
                self.check_frontend_continuation(&request).await?;
            }
            return Err(refusal);
        }
        self.own_preparation(
            request.continuation.session_id.clone(),
            move |runtime| async move {
                let runtime = runtime.with_context(context);
                if let Some(receipt) = runtime.lookup_frontend_continuation(&request).await? {
                    return Ok(ToolContinuation::Replayed(receipt));
                }
                runtime.check_frontend_continuation(&request).await?;
                let run = runtime
                    .store
                    .get_run(&request.continuation.run_id)
                    .await?
                    .ok_or(Error::NotFound)?;
                let claim = request.claim(&run)?;
                let validated = runtime.validated_session(&run).await?;
                runtime
                    .spawn_tool_resume(run, validated, ToolResumeControl::FrontendClaim(claim))
                    .await
            },
        )
        .await
    }
    async fn frontend_recovery_request(
        &self,
        request: &RecoverFrontend,
        run: &Run,
    ) -> Result<FrontendContinuationRecovery, RuntimeError> {
        self.frontend_recovery_for_claim(request, request.original.claim(run)?)
            .await
    }
    async fn frontend_recovery_for_claim(
        &self,
        request: &RecoverFrontend,
        claim: FrontendContinuationClaim,
    ) -> Result<FrontendContinuationRecovery, RuntimeError> {
        let original = self
            .lookup_frontend_continuation(&request.original)
            .await?
            .ok_or(Error::NotFound)?;
        let ToolContinuationOutcome::FrontendClaimed { attempt, .. } = original.outcome else {
            return Err(Error::SemanticConflict.into());
        };
        Ok(FrontendContinuationRecovery {
            id: request.id.clone(),
            expected_revision: request.expected_revision,
            original: claim,
            attempt: attempt.target,
            owner: crabber_core::RunId::new().to_string(),
            lease_ms: 30000,
        })
    }
    /// Reconcile the exact original positive-Unstarted recovery.
    /// # Errors
    /// Rejects changed original metadata and malformed receipt evidence.
    pub async fn lookup_frontend_recovery(
        &self,
        request: &RecoverFrontend,
    ) -> Result<Option<ToolContinuationReceipt>, RuntimeError> {
        let scope = &request.original.continuation;
        let Some(receipt) = self
            .store
            .lookup_tool_continuation(&scope.session_id, &request.id)
            .await?
        else {
            return Ok(None);
        };
        if receipt.run_id != scope.run_id
            || !matches!(&receipt.outcome, ToolContinuationOutcome::FrontendReclaimedUnstarted { continuation, .. } if *continuation == scope.continuation)
        {
            return Err(Error::SemanticConflict.into());
        }
        let original_plan = self
            .store
            .original_tool_continuation_plan(&scope.session_id, &scope.run_id, &scope.continuation)
            .await?;
        let claim = request
            .original
            .claim_from_native(scope.claim_with_plan(original_plan))?;
        self.frontend_recovery_for_claim(request, claim)
            .await?
            .verify_receipt(&receipt)?;
        Ok(Some(receipt))
    }
    /// Rotate only positively Unstarted result-processing authority.
    /// # Errors
    /// Started result protection remains uncertain and is never repeated.
    pub async fn recover_frontend_continuation(
        &self,
        request: RecoverFrontend,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(receipt) = self.lookup_frontend_recovery(&request).await? {
                return Ok(ToolContinuation::Replayed(receipt));
            }
            return Err(refusal);
        }
        self.own_preparation(
            request.original.continuation.session_id.clone(),
            move |runtime| async move {
                if let Some(receipt) = runtime.lookup_frontend_recovery(&request).await? {
                    return Ok(ToolContinuation::Replayed(receipt));
                }
                let run = runtime
                    .store
                    .get_run(&request.original.continuation.run_id)
                    .await?
                    .ok_or(Error::NotFound)?;
                let recovery = runtime.frontend_recovery_request(&request, &run).await?;
                let validated = runtime.validated_session(&run).await?;
                runtime
                    .spawn_tool_resume(run, validated, ToolResumeControl::FrontendRecover(recovery))
                    .await
            },
        )
        .await
    }
}
