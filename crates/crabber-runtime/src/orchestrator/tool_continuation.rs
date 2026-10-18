//! Public continuation admission restores exact native state without replaying start.
use super::continuation::{ApprovalControl, ApprovalResume, ApprovedCalls, ResumeAuthority};
use super::{Orchestrator, RunHandle, RuntimeError};
use crabber_core::{ControlId, InputFingerprint, Run, RunId, SessionId, SessionRevision};
use crabber_session::{
    ContinuationTarget, StoreError, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationError as Error, ToolContinuationOutcome, ToolContinuationReceipt,
};
use tokio::sync::oneshot;
#[path = "tool_continuation/intent.rs"]
mod intent;
pub(super) use intent::Expected as ToolReceiptExpectation;
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinueTool {
    pub id: ControlId,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub expected_revision: SessionRevision,
}
pub enum ToolContinuation {
    Started {
        receipt: ToolContinuationReceipt,
        handle: RunHandle,
    },
    Replayed(ToolContinuationReceipt),
}
pub(super) enum ToolResumeControl {
    Claim(ToolContinuationClaim),
    Recover(crabber_session::ToolContinuationRecovery),
    FrontendClaim(crabber_session::FrontendContinuationClaim),
    FrontendRecover(crabber_session::FrontendContinuationRecovery),
}
pub(super) enum ToolClaimed {
    Fresh(ToolContinuationReceipt),
    Replay(ToolContinuationReceipt),
}
impl ContinueTool {
    pub(super) fn claim(&self, run: &Run) -> Result<ToolContinuationClaim, RuntimeError> {
        if run.session_id != self.session_id || run.id != self.run_id {
            return Err(Error::NotFound.into());
        }
        Ok(self.claim_with_plan(InputFingerprint::new(run.plan_fingerprint.clone())?))
    }
    pub(super) fn claim_with_plan(
        &self,
        expected_plan: crabber_core::InputFingerprint,
    ) -> ToolContinuationClaim {
        ToolContinuationClaim {
            id: self.id.clone(),
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            continuation: self.continuation.clone(),
            expected_revision: self.expected_revision,
            expected_plan,
            owner: RunId::new().to_string(),
            lease_ms: 30000,
        }
    }
}
impl Orchestrator {
    /// Read safe pending metadata without extension initialization.
    /// # Errors
    /// Rejects foreign scope, malformed stored provenance or unsupported stores.
    pub async fn tool_continuation_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber_session::ToolContinuationSnapshot>, Error> {
        self.store.tool_continuation_snapshot(session, run).await
    }
    /// Record a response only after the host authenticates the claimed authority.
    /// # Errors
    /// Rejects stale revision/generation, schema/authority mismatch and changed retries.
    pub async fn answer_tool_interaction(
        &self,
        answer: crabber_session::ToolInteractionAnswer,
    ) -> Result<ToolContinuationReceipt, Error> {
        self.store.answer_tool_interaction(answer).await
    }
    /// Pure exact-key reconciliation; never restores a plan or grants a handle.
    /// # Errors
    /// Rejects changed retry semantics or malformed receipt provenance.
    pub async fn lookup_tool_continuation(
        &self,
        request: &ContinueTool,
    ) -> Result<Option<ToolContinuationReceipt>, RuntimeError> {
        let Some(receipt) = self
            .store
            .lookup_tool_continuation(&request.session_id, &request.id)
            .await?
        else {
            return Ok(None);
        };
        if receipt.run_id != request.run_id
            || !matches!(&receipt.outcome, ToolContinuationOutcome::Claimed { continuation, .. } if *continuation == request.continuation)
        {
            return Err(Error::SemanticConflict.into());
        }
        let original_plan = self
            .store
            .original_tool_continuation_plan(
                &request.session_id,
                &request.run_id,
                &request.continuation,
            )
            .await?;
        if receipt.semantic_digest != request.claim_with_plan(original_plan).fingerprint()? {
            return Err(Error::SemanticConflict.into());
        }
        Ok(Some(receipt))
    }
    /// Start one owned worker for an exact ready explicit checkpoint generation.
    /// # Errors
    /// Unknown acknowledgement must be reconciled with this original request.
    pub async fn continue_tool(
        &self,
        request: ContinueTool,
    ) -> Result<ToolContinuation, RuntimeError> {
        self.continue_tool_with_context(request, None).await
    }
    /// Host trace correlation never grants continuation authority.
    /// # Errors
    /// Returns the same bounded eligibility/preparation failures as continuation.
    pub async fn continue_tool_with_context(
        &self,
        request: ContinueTool,
        context: Option<crabber_core::TraceContext>,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(receipt) = self.lookup_tool_continuation(&request).await? {
                return Ok(ToolContinuation::Replayed(receipt));
            }
            return Err(refusal);
        }
        self.own_preparation(request.session_id.clone(), move |runtime| async move {
            let runtime = runtime.with_context(context);
            runtime.prepare_tool_continuation(request).await
        })
        .await
    }
    async fn prepare_tool_continuation(
        &self,
        request: ContinueTool,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(receipt) = self.lookup_tool_continuation(&request).await? {
            return Ok(ToolContinuation::Replayed(receipt));
        }
        let run = self
            .store
            .get_run(&request.run_id)
            .await?
            .ok_or(Error::NotFound)?;
        let claim = request.claim(&run)?;
        let validated = self.validated_session(&run).await?;
        self.spawn_tool_resume(run, validated, ToolResumeControl::Claim(claim))
            .await
    }
    pub(super) async fn spawn_tool_resume(
        &self,
        run: Run,
        validated: (super::RunSession, Option<super::Request>),
        control: ToolResumeControl,
    ) -> Result<ToolContinuation, RuntimeError> {
        let expected = ToolReceiptExpectation::new(&control)?;
        let binding = self.stored_binding(&run.session_id, &run.id).await?;
        if crabber_session::session_owner::preparation::resource_contract(
            &binding.runtime_semantics,
        )?
        .is_some()
        {
            return Box::pin(self.spawn_pending_tool(run, validated, binding, control, expected))
                .await;
        }
        let (sender, receiver) = oneshot::channel();
        let (dummy, _) = oneshot::channel();
        let control = ApprovalResume {
            prepared: None,
            control: ApprovalControl::Tool {
                control,
                sender,
                expected,
            },
            sender: dummy,
        };
        let handle = self.spawn_worker(
            run.session_id.clone(),
            run.id.clone(),
            move |runtime, cancellation| async move {
                Box::pin(runtime.resume_loaded(run, validated, cancellation, Some(control))).await
            },
        )?;
        match receiver.await {
            Ok(ToolClaimed::Fresh(receipt)) => Ok(ToolContinuation::Started { receipt, handle }),
            Ok(ToolClaimed::Replay(receipt)) => {
                let _ = handle.done().await;
                Ok(ToolContinuation::Replayed(receipt))
            }
            Err(_) => match handle.done().await {
                Err(error) => Err(error),
                Ok(_) => Err(Error::UnknownStoreOutcome.into()),
            },
        }
    }
    pub(super) async fn acquire_tool_resume(
        &self,
        run: &Run,
        control: ToolResumeControl,
        sender: oneshot::Sender<ToolClaimed>,
        prepared: Option<crabber_session::session_owner::preparation::PreparationCompletion>,
        expected: Box<ToolReceiptExpectation>,
    ) -> Result<Option<ResumeAuthority>, RuntimeError> {
        let outcome = if let Some(prepared) = prepared {
            let crabber_session::session_owner::preparation::PreparationCompletion::ToolContinuation(outcome) = prepared else {
                return Err(StoreError::AdmissionConflict.into());
            };
            outcome
        } else {
            match control {
                ToolResumeControl::FrontendClaim(claim) => {
                    self.store.claim_frontend_continuation(claim).await?
                }
                ToolResumeControl::FrontendRecover(request) => {
                    let outcome = self
                        .store
                        .recover_frontend_continuation(request.clone())
                        .await?;
                    let receipt = match &outcome {
                        ToolContinuationClaimOutcome::Applied { receipt, .. }
                        | ToolContinuationClaimOutcome::Replayed(receipt) => receipt,
                    };
                    request.verify_receipt(receipt)?;
                    outcome
                }
                ToolResumeControl::Claim(claim) => {
                    self.store.claim_tool_continuation(claim).await?
                }
                ToolResumeControl::Recover(request) => {
                    let outcome = self
                        .store
                        .recover_tool_continuation(request.clone())
                        .await?;
                    let receipt = match &outcome {
                        ToolContinuationClaimOutcome::Applied { receipt, .. }
                        | ToolContinuationClaimOutcome::Replayed(receipt) => receipt,
                    };
                    request.verify_receipt(receipt)?;
                    outcome
                }
            }
        };
        let receipt = match &outcome {
            ToolContinuationClaimOutcome::Applied { receipt, .. }
            | ToolContinuationClaimOutcome::Replayed(receipt) => receipt,
        };
        expected.verify(receipt)?;
        match outcome {
            ToolContinuationClaimOutcome::Replayed(receipt) => {
                let _ = sender.send(ToolClaimed::Replay(receipt));
                Ok(None)
            }
            ToolContinuationClaimOutcome::Applied { receipt, grant } => {
                let authority = prepared_tool_authority(run, &receipt, *grant)?;
                let _ = sender.send(ToolClaimed::Fresh(receipt));
                Ok(Some(authority))
            }
        }
    }
}

pub(super) fn prepared_tool_authority(
    run: &Run,
    receipt: &ToolContinuationReceipt,
    grant: crabber_session::ToolContinuationGrant,
) -> Result<ResumeAuthority, RuntimeError> {
    grant.verify_native_ack(receipt)?;
    let (ToolContinuationOutcome::Claimed { attempt, .. }
    | ToolContinuationOutcome::ReclaimedUnstarted { attempt, .. }
    | ToolContinuationOutcome::FrontendClaimed { attempt, .. }
    | ToolContinuationOutcome::FrontendReclaimedUnstarted { attempt, .. }) = &receipt.outcome
    else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    if grant.fence.run_id != run.id
        || grant.usage != run.usage
        || grant
            .phases
            .iter()
            .any(|phase| phase.target.attempt != attempt.target)
        || grant
            .calls
            .iter()
            .any(|call| call.target.attempt != attempt.target)
        || grant
            .frontend
            .iter()
            .any(|phase| phase.target.attempt != attempt.target)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(ResumeAuthority {
        fence: grant.fence,
        native_unstarted: false,
        granted: Some(super::attempt::OwnedAttempt {
            target: attempt.target.clone(),
            authority: grant.authority,
        }),
        approved: Some(ApprovedCalls {
            calls: grant.calls,
            turn: grant.source_turn_id,
            usage: grant.usage,
            turns_used: usize::try_from(grant.turns_used)
                .map_err(|_| Error::UnknownStoreOutcome)?,
        }),
        tool_phases: grant.phases,
        frontend_phases: grant.frontend,
    })
}
