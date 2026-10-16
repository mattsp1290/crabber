//! Durable approval continuation: a receipt replay never returns another handle.
use super::{Orchestrator, RunHandle, RunStatus, RuntimeError};
use crabber_core::{
    ControlId, Run, RunFence, RunId, SessionId, SessionRevision, TraceContext, TurnId, Usage,
};
use crabber_session::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalError, ApprovalGrant, ApprovalOutcome,
    ApprovalReceipt, AttemptCleanup, AttemptError, AttemptState, ContinuationTarget,
    GrantedApproval, StoreError,
};
use tokio::sync::oneshot;

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
pub(super) struct ApprovalResume {
    pub(super) control: ApprovalControl,
    pub(super) sender: oneshot::Sender<Claimed>,
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
    pub granted: Option<super::attempt::OwnedAttempt>,
    pub approved: Option<ApprovedCalls>,
}
impl ContinueApproval {
    pub(super) fn claim(&self, run: &Run) -> Result<ApprovalContinuationClaim, RuntimeError> {
        if self.session_id != run.session_id || self.run_id != run.id {
            return Err(StoreError::NotFound.into());
        }
        Ok(ApprovalContinuationClaim {
            id: self.id.clone(),
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            continuation: self.continuation.clone(),
            expected_revision: self.expected_revision,
            expected_plan: crabber_core::InputFingerprint::new(run.plan_fingerprint.clone())
                .map_err(|_| ApprovalError::InvalidRequest)?,
            owner: RunId::new().to_string(),
            lease_ms: 30_000,
        })
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
            || !matches!(receipt.outcome, ApprovalOutcome::Claimed { .. })
        {
            return Err(ApprovalError::SemanticConflict.into());
        }
        let run = self
            .store
            .get_run(&request.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        if request.claim(&run)?.fingerprint()? != receipt.semantic_digest {
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
        let runtime = self.with_context(context);
        self.own_preparation(request.session_id.clone(), async move {
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
        let (sender, receiver) = oneshot::channel();
        let control = ApprovalResume {
            control: ApprovalControl::Claim(claim),
            sender,
        };
        let handle = self.spawn_worker(
            run.session_id.clone(),
            run.id.clone(),
            move |runtime, cancellation| async move {
                Box::pin(runtime.resume_loaded(run, validated, cancellation, Some(control))).await
            },
        );
        claim_result(handle, receiver).await
    }
    pub(super) async fn acquire_resume(
        &self,
        run: &Run,
        control: Option<ApprovalResume>,
    ) -> Result<Option<ResumeAuthority>, RuntimeError> {
        if let Some(control) = control {
            let outcome = match &control.control {
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
            };
            return match outcome {
                ApprovalClaimOutcome::Replayed(receipt) => {
                    let _ = control.sender.send(Claimed::Replay(receipt));
                    Ok(None)
                }
                ApprovalClaimOutcome::Applied { receipt, grant } => {
                    if grant.usage != run.usage
                        || Some(grant.turns_used)
                            != run
                                .checkpoint
                                .as_ref()
                                .and_then(|checkpoint| checkpoint.get("turns_used"))
                                .and_then(serde_json::Value::as_u64)
                    {
                        return Err(ApprovalError::UnknownStoreOutcome.into());
                    }
                    let authority = resume_authority(&receipt, *grant)?;
                    let _ = control.sender.send(Claimed::Fresh(receipt));
                    Ok(Some(authority))
                }
            };
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
            granted: None,
            approved: None,
        }))
    }
}
fn resume_authority(
    receipt: &ApprovalReceipt,
    grant: ApprovalGrant,
) -> Result<ResumeAuthority, RuntimeError> {
    let (ApprovalOutcome::Claimed { attempt, .. }
    | ApprovalOutcome::ReclaimedUnstarted { attempt, .. }) = &receipt.outcome
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
        granted: Some(super::attempt::OwnedAttempt {
            target: attempt.target.clone(),
            authority: grant.authority,
        }),
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
