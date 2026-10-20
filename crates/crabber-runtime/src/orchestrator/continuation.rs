//! Durable approval continuation: a receipt replay never returns another handle.
use super::{Orchestrator, RunHandle, RunStatus, RuntimeError};
use crate::telemetry_targets::targets;
use crabber_core::{
    ControlId, Run, RunFence, RunId, SessionId, SessionRevision, TraceContext, TurnId, Usage,
};
use crabber_session::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalError, ApprovalGrant, ApprovalOutcome,
    ApprovalReceipt, AttemptCleanup, AttemptError, AttemptState, ContinuationTarget,
    GrantedApproval, StoreError,
};
use tokio::sync::oneshot;
use tracing::Instrument;

#[cfg(test)]
#[path = "continuation/telemetry_tests.rs"]
mod telemetry_tests;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinueApproval {
    pub id: ControlId,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub expected_revision: SessionRevision,
}
/// A fresh claim returns an observation handle; native dispatch still requires
/// a fresh Begin inside the retained worker. Replay returns metadata only.
pub enum Continuation {
    Started {
        receipt: ApprovalReceipt,
        handle: RunHandle,
    },
    Replayed(ApprovalReceipt),
}
pub(super) enum ApprovalResume {
    Approval {
        prepared: Option<crabber_session::session_owner::preparation::PreparationCompletion>,
        control: ApprovalControl,
        sender: oneshot::Sender<Claimed>,
    },
    Tool {
        prepared: Option<crabber_session::session_owner::preparation::PreparationCompletion>,
        control: super::tool_continuation::ToolResumeControl,
        sender: oneshot::Sender<super::tool_continuation::ToolClaimed>,
        expected: Box<super::tool_continuation::ToolReceiptExpectation>,
    },
}
impl ApprovalResume {
    pub(super) fn set_prepared(
        &mut self,
        completed: crabber_session::session_owner::preparation::PreparationCompletion,
    ) {
        match self {
            Self::Approval { prepared, .. } | Self::Tool { prepared, .. } => {
                *prepared = Some(completed);
            }
        }
    }
}
pub(super) enum Claimed {
    Fresh(ApprovalReceipt),
    Replay(ApprovalReceipt),
}
pub(super) enum ApprovalControl {
    Claim(ApprovalContinuationClaim),
    Recover(crabber_session::ApprovalContinuationRecovery),
}
pub(super) struct ApprovedCalls {
    pub calls: Vec<GrantedApproval>,
    pub turn: Option<TurnId>,
    pub usage: Usage,
    pub turns_used: usize,
}
pub(super) struct ResumeAuthority {
    pub fence: RunFence,
    pub native_unstarted: bool,
    pub continuation: Option<crabber_core::continuation_source::ContinuationIdentity>,
    pub granted: Option<super::attempt::OwnedAttempt>,
    pub approved: Option<ApprovedCalls>,
    pub tool_phases: Vec<crabber_session::ToolPhaseResume>,
    pub frontend_phases: Vec<crabber_session::FrontendPhaseResume>,
}
impl ContinueApproval {
    pub(super) fn claim(&self, run: &Run) -> Result<ApprovalContinuationClaim, RuntimeError> {
        if self.session_id != run.session_id || self.run_id != run.id {
            return Err(StoreError::NotFound.into());
        }
        Ok(self.claim_with_plan(
            crabber_core::InputFingerprint::new(run.plan_fingerprint.clone())
                .map_err(|_| ApprovalError::InvalidRequest)?,
        ))
    }
    pub(super) fn claim_with_plan(
        &self,
        expected_plan: crabber_core::InputFingerprint,
    ) -> ApprovalContinuationClaim {
        ApprovalContinuationClaim {
            id: self.id.clone(),
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            continuation: self.continuation.clone(),
            expected_revision: self.expected_revision,
            expected_plan,
            owner: RunId::new().to_string(),
            lease_ms: 30_000,
        }
    }
}
impl Orchestrator {
    /// Bounded coherent approval metadata; no factories or execution authority.
    /// # Errors
    /// Returns unsupported, missing scope or storage integrity failures.
    pub async fn approval_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber_session::ApprovalSnapshot>, ApprovalError> {
        self.store.approval_snapshot(session, run).await
    }
    /// Record a response only after the host has verified identity and authority.
    /// Model text and frontend tool results are never approval credentials.
    /// # Errors
    /// Returns finite scope, authority, expiry, revision or retry conflicts.
    pub async fn answer_approval(
        &self,
        answer: crabber_session::ApprovalAnswer,
    ) -> Result<ApprovalReceipt, ApprovalError> {
        self.store.answer_approval(answer).await
    }

    /// Reconcile the original continuation identity without initializing factories.
    /// # Errors
    /// Rejects changed semantics, foreign scope or unverifiable retained metadata.
    pub async fn lookup_approval_continuation(
        &self,
        request: &ContinueApproval,
    ) -> Result<Option<ApprovalReceipt>, RuntimeError> {
        let Some(receipt) = self
            .store
            .lookup_approval(&request.session_id, &request.id)
            .await?
        else {
            return Ok(None);
        };
        if receipt.run_id != request.run_id
            || !matches!(&receipt.outcome, ApprovalOutcome::Claimed { continuation, .. } if *continuation == request.continuation)
        {
            return Err(ApprovalError::SemanticConflict.into());
        }
        let original_plan = self
            .store
            .original_approval_plan(&request.session_id, &request.run_id, &request.continuation)
            .await?;
        if request.claim_with_plan(original_plan).fingerprint()? != receipt.semantic_digest {
            return Err(ApprovalError::SemanticConflict.into());
        }
        Ok(Some(receipt))
    }
    /// Resume the exact frozen approval batch once, under retained worker ownership.
    /// # Errors
    /// Returns authoritative control/preparation failures; an unknown outcome must
    /// be reconciled using the original request identity.
    pub async fn continue_approval(
        &self,
        request: ContinueApproval,
    ) -> Result<Continuation, RuntimeError> {
        self.continue_approval_with_context(request, None).await
    }
    /// Host correlation is presentation only and never continuation authority.
    /// # Errors
    /// Returns the same failures as `continue_approval`.
    pub async fn continue_approval_with_context(
        &self,
        request: ContinueApproval,
        context: Option<TraceContext>,
    ) -> Result<Continuation, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(receipt) = self.lookup_approval_continuation(&request).await? {
                return Ok(Continuation::Replayed(receipt));
            }
            return Err(refusal);
        }
        self.own_preparation(request.session_id.clone(), move |runtime| async move {
            let runtime = runtime.with_context(context);
            runtime.prepare_continuation(request).await
        })
        .await
    }
    async fn prepare_continuation(
        &self,
        request: ContinueApproval,
    ) -> Result<Continuation, RuntimeError> {
        if let Some(receipt) = self.lookup_approval_continuation(&request).await? {
            return Ok(Continuation::Replayed(receipt));
        }
        let run = self
            .store
            .get_run(&request.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let claim = request.claim(&run)?;
        let validated = self.validated_session(&run).await?;
        let binding = self.stored_binding(&run.session_id, &run.id).await?;
        if crabber_session::session_owner::preparation::resource_contract(
            &binding.runtime_semantics,
        )?
        .is_some()
        {
            return Box::pin(self.spawn_pending_approval(
                run,
                validated,
                binding,
                ApprovalControl::Claim(claim),
            ))
            .await;
        }
        let (sender, receiver) = oneshot::channel();
        let control = ApprovalResume::Approval {
            prepared: None,
            control: ApprovalControl::Claim(claim),
            sender,
        };
        let handle = self.spawn_worker(
            run.session_id.clone(),
            run.id.clone(),
            move |runtime, cancellation| async move {
                Box::pin(runtime.resume_loaded(
                    run,
                    validated,
                    cancellation,
                    super::recovery::ResumeRequest::Continuation(Box::new(control)),
                ))
                .await
            },
        )?;
        claim_result(handle, receiver).await
    }
    async fn acquire_approval_resume(
        &self,
        run: &Run,
        prepared: Option<crabber_session::session_owner::preparation::PreparationCompletion>,
        control: ApprovalControl,
        sender: oneshot::Sender<Claimed>,
    ) -> Result<Option<ResumeAuthority>, RuntimeError> {
        let span = tracing::info_span!(target: targets::RUNTIME, "crabber.approval.continue", "session.id" = %run.session_id, "run.id" = %run.id, outcome = "failed", "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
        let result: Result<Option<ResumeAuthority>, RuntimeError> = async {
            let outcome = if let Some(prepared) = prepared {
                let crabber_session::session_owner::preparation::PreparationCompletion::Approval(
                    outcome,
                ) = prepared
                else {
                    return Err(StoreError::AdmissionConflict.into());
                };
                outcome
            } else {
                match &control {
                    ApprovalControl::Claim(claim) => {
                        self.store
                            .claim_approval_continuation(claim.clone())
                            .await?
                    }
                    ApprovalControl::Recover(request) => {
                        let outcome = self
                            .store
                            .reclaim_approval_continuation(request.clone())
                            .await?;
                        let receipt = match &outcome {
                            ApprovalClaimOutcome::Applied { receipt, .. }
                            | ApprovalClaimOutcome::Replayed(receipt) => receipt,
                        };
                        request.verify_receipt(receipt)?;
                        outcome
                    }
                }
            };
            match outcome {
                ApprovalClaimOutcome::Replayed(receipt) => {
                    span.record("outcome", "replayed");
                    let _ = sender.send(Claimed::Replay(receipt));
                    Ok(None)
                }
                ApprovalClaimOutcome::Applied { receipt, grant } => {
                    let outcome = if grant
                        .calls
                        .iter()
                        .any(|call| call.decision == crabber_session::GrantedDecision::Deny)
                    {
                        "denied"
                    } else {
                        "approved"
                    };
                    let authority = prepared_approval_authority(run, &receipt, *grant)?;
                    span.record("outcome", outcome);
                    let _ = sender.send(Claimed::Fresh(receipt));
                    Ok(Some(authority))
                }
            }
        }
        .instrument(span.clone())
        .await;
        match &result {
            Ok(_) => {
                span.record("otel.status_code", "OK");
            }
            Err(error) => {
                span.record("outcome", "failed");
                span.record("error.kind", error.error_kind());
                span.record("otel.status_code", "ERROR");
            }
        }
        result
    }
    pub(super) async fn acquire_resume(
        &self,
        run: &Run,
        request: super::recovery::ResumeRequest,
    ) -> Result<Option<ResumeAuthority>, RuntimeError> {
        if let super::recovery::ResumeRequest::Continuation(control) = request {
            let (prepared, control, sender) = match *control {
                ApprovalResume::Tool {
                    control,
                    sender,
                    prepared,
                    expected,
                } => {
                    return self
                        .acquire_tool_resume(run, control, sender, prepared, expected)
                        .await;
                }
                ApprovalResume::Approval {
                    control,
                    prepared,
                    sender,
                } => (prepared, control, sender),
            };
            return self
                .acquire_approval_resume(run, prepared, control, sender)
                .await;
        }
        let native_unstarted = match self.store.latest_execution_attempt(&run.id).await? {
            Some(previous) if previous.attempt.state == AttemptState::Unstarted => true,
            Some(previous)
                if previous.attempt.state == AttemptState::Suspended
                    && previous.attempt.cleanup == AttemptCleanup::Complete =>
            {
                false
            }
            Some(_) => return Err(AttemptError::Busy.into()),
            None if run.status == RunStatus::Running => true,
            None => return Err(AttemptError::InvalidTransition.into()),
        };
        let fence = self
            .store
            .claim_expired_run(&run.id, &RunId::new().to_string())
            .await?;
        Ok(Some(ResumeAuthority {
            fence,
            native_unstarted,
            continuation: None,
            granted: None,
            approved: None,
            tool_phases: Vec::new(),
            frontend_phases: Vec::new(),
        }))
    }
}
pub(super) fn prepared_approval_authority(
    run: &Run,
    receipt: &ApprovalReceipt,
    grant: ApprovalGrant,
) -> Result<ResumeAuthority, RuntimeError> {
    if receipt.run_id != run.id
        || receipt.session_id != run.session_id
        || grant.usage != run.usage
        || Some(grant.turns_used)
            != run
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.get("turns_used"))
                .and_then(serde_json::Value::as_u64)
    {
        return Err(ApprovalError::UnknownStoreOutcome.into());
    }
    resume_authority(receipt, grant)
}

fn resume_authority(
    receipt: &ApprovalReceipt,
    grant: ApprovalGrant,
) -> Result<ResumeAuthority, RuntimeError> {
    let (ApprovalOutcome::Claimed {
        attempt,
        continuation,
    }
    | ApprovalOutcome::ReclaimedUnstarted {
        attempt,
        continuation,
        ..
    }) = &receipt.outcome
    else {
        return Err(ApprovalError::UnknownStoreOutcome.into());
    };
    if grant.fence.run_id != receipt.run_id
        || attempt.run_id != receipt.run_id
        || attempt.session_id != receipt.session_id
        || grant
            .calls
            .iter()
            .any(|call| call.target.attempt != attempt.target)
    {
        return Err(ApprovalError::UnknownStoreOutcome.into());
    }
    Ok(ResumeAuthority {
        fence: grant.fence,
        native_unstarted: false,
        continuation: Some(crabber_core::continuation_source::ContinuationIdentity {
            id: continuation.id.clone(),
            generation: continuation.generation,
        }),
        granted: Some(super::attempt::OwnedAttempt {
            target: attempt.target.clone(),
            authority: grant.authority,
            begin_cursor: None,
        }),
        tool_phases: Vec::new(),
        frontend_phases: Vec::new(),
        approved: Some(ApprovedCalls {
            calls: grant.calls,
            turn: grant.source_turn_id,
            usage: grant.usage,
            turns_used: usize::try_from(grant.turns_used)
                .map_err(|_| ApprovalError::UnknownStoreOutcome)?,
        }),
    })
}

pub(super) async fn claim_result(
    handle: RunHandle,
    receiver: oneshot::Receiver<Claimed>,
) -> Result<Continuation, RuntimeError> {
    match receiver.await {
        Ok(Claimed::Fresh(receipt)) => Ok(Continuation::Started { receipt, handle }),
        Ok(Claimed::Replay(receipt)) => {
            let _ = handle.done().await;
            Ok(Continuation::Replayed(receipt))
        }
        Err(_) => match handle.done().await {
            Err(error) => Err(error),
            Ok(_) => Err(ApprovalError::UnknownStoreOutcome.into()),
        },
    }
}
