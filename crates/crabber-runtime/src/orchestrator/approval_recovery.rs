//! Explicit recovery of a consumed approval whose reserved attempt never began.
use super::continuation::{ApprovalControl, ApprovalResume};
use super::{Continuation, ContinueApproval, Orchestrator, RuntimeError};
use crabber_core::{ControlId, Run, SessionRevision, TraceContext};
use crabber_session::{
    ApprovalContinuationRecovery, ApprovalError, ApprovalOutcome, ApprovalReceipt, StoreError,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverApproval {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub original: ContinueApproval,
}
impl Orchestrator {
    async fn recovery_request(
        &self,
        request: &RecoverApproval,
        run: &Run,
    ) -> Result<ApprovalContinuationRecovery, RuntimeError> {
        let original = self
            .lookup_approval_continuation(&request.original)
            .await?
            .ok_or(ApprovalError::NotFound)?;
        let ApprovalOutcome::Claimed { attempt, .. } = original.outcome else {
            return Err(ApprovalError::SemanticConflict.into());
        };
        Ok(ApprovalContinuationRecovery {
            id: request.id.clone(),
            expected_revision: request.expected_revision,
            original: request.original.claim(run)?,
            attempt: attempt.target,
            owner: crabber_core::RunId::new().to_string(),
            lease_ms: 30_000,
        })
    }
    /// Pure receipt reconciliation; replay cannot initialize or start a worker.
    /// # Errors
    /// Rejects changed semantics, foreign scope or unverifiable retained provenance.
    pub async fn lookup_approval_recovery(
        &self,
        request: &RecoverApproval,
    ) -> Result<Option<ApprovalReceipt>, RuntimeError> {
        let Some(receipt) = self
            .store
            .lookup_approval(&request.original.session_id, &request.id)
            .await?
        else {
            return Ok(None);
        };
        let run = self
            .store
            .get_run(&request.original.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        self.recovery_request(request, &run)
            .await?
            .verify_receipt(&receipt)?;
        Ok(Some(receipt))
    }
    /// Reclaim an expired, positively Unstarted approval continuation explicitly.
    /// Original claim and recovery retries return metadata only; Started work is refused.
    /// # Errors
    /// Retain both original and recovery request identities to reconcile unknown ACKs.
    pub async fn recover_approval_continuation(
        &self,
        request: RecoverApproval,
    ) -> Result<Continuation, RuntimeError> {
        self.recover_approval_continuation_with_context(request, None)
            .await
    }
    /// Host correlation does not grant recovery authority.
    /// # Errors
    /// Returns the same finite eligibility/preparation errors as recovery.
    pub async fn recover_approval_continuation_with_context(
        &self,
        request: RecoverApproval,
        context: Option<TraceContext>,
    ) -> Result<Continuation, RuntimeError> {
        let runtime = self.with_context(context);
        self.own_preparation(request.original.session_id.clone(), async move {
            runtime.prepare_approval_recovery(request).await
        })
        .await
    }
    async fn prepare_approval_recovery(
        &self,
        request: RecoverApproval,
    ) -> Result<Continuation, RuntimeError> {
        if let Some(receipt) = self.lookup_approval_recovery(&request).await? {
            return Ok(Continuation::Replayed(receipt));
        }
        let run = self
            .store
            .get_run(&request.original.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let recovery = self.recovery_request(&request, &run).await?;
        let validated = self.validated_session(&run).await?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let control = ApprovalResume {
            control: ApprovalControl::Recover(recovery),
            sender,
        };
        let handle = self.spawn_worker(
            run.session_id.clone(),
            run.id.clone(),
            move |runtime, cancellation| async move {
                Box::pin(runtime.resume_loaded(run, validated, cancellation, Some(control))).await
            },
        );
        super::continuation::claim_result(handle, receiver).await
    }
}
