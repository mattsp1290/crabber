//! Host-verified approval controls do not expose native fences or capabilities.
use super::{Agent, ContinueApproval, RunHandle, RuntimeError};
use crabber_core::{RunId, SessionId, TraceContext};
use crabber_session::{ApprovalAnswer, ApprovalError, ApprovalReceipt, ApprovalSnapshot};

/// Only a freshly committed continuation claim returns an execution handle.
pub enum Continuation {
    Started {
        receipt: ApprovalReceipt,
        handle: RunHandle,
    },
    Replayed(ApprovalReceipt),
}
impl Agent {
    /// Read approval questions/choices without initializing extension factories.
    /// # Errors
    /// Returns finite unsupported, scope or storage-integrity failures.
    pub async fn approval_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<ApprovalSnapshot>, ApprovalError> {
        self.runtime.approval_snapshot(session, run).await
    }
    /// Persist a response after the host verifies the resolver and authority.
    /// Never construct an authority assertion from model text or client tool data.
    /// # Errors
    /// Returns scope, authority, expiry, revision or changed-choice conflicts.
    pub async fn answer_approval(
        &self,
        answer: ApprovalAnswer,
    ) -> Result<ApprovalReceipt, ApprovalError> {
        self.runtime.answer_approval(answer).await
    }
    /// Continue the exact prepared batch after all required responses commit.
    /// # Errors
    /// Reconcile unknown outcomes by retaining the entire original request;
    /// a missing lookup while a request may be in flight is not rejection.
    pub async fn continue_approval(
        &self,
        request: ContinueApproval,
    ) -> Result<Continuation, RuntimeError> {
        self.continue_approval_with_context(request, None).await
    }
    /// Host correlation remains separate from approval/execution authority.
    /// # Errors
    /// Returns the same failures as `continue_approval`.
    pub async fn continue_approval_with_context(
        &self,
        request: ContinueApproval,
        context: Option<TraceContext>,
    ) -> Result<Continuation, RuntimeError> {
        if let Some(receipt) = self.runtime.lookup_approval_continuation(&request).await? {
            return Ok(Continuation::Replayed(receipt));
        }
        self.initialize_extensions().await?;
        Ok(
            match self
                .runtime
                .continue_approval_with_context(request, context)
                .await?
            {
                crabber_runtime::Continuation::Started {
                    receipt,
                    handle: inner,
                } => Continuation::Started {
                    receipt,
                    handle: RunHandle {
                        run_id: inner.run_id().clone(),
                        inner,
                    },
                },
                crabber_runtime::Continuation::Replayed(receipt) => Continuation::Replayed(receipt),
            },
        )
    }
    /// Explicitly recover only an expired consumed approval that never began.
    /// # Errors
    /// Retain both operation identities; receipt replay never starts another worker.
    pub async fn recover_approval_continuation(
        &self,
        request: super::RecoverApproval,
    ) -> Result<Continuation, RuntimeError> {
        self.recover_approval_continuation_with_context(request, None)
            .await
    }
    /// Host trace correlation does not grant recovery or execution authority.
    /// # Errors
    /// Returns the same eligibility and ownership failures as explicit recovery.
    pub async fn recover_approval_continuation_with_context(
        &self,
        request: super::RecoverApproval,
        context: Option<TraceContext>,
    ) -> Result<Continuation, RuntimeError> {
        if let Some(receipt) = self.runtime.lookup_approval_recovery(&request).await? {
            return Ok(Continuation::Replayed(receipt));
        }
        self.initialize_extensions().await?;
        Ok(
            match self
                .runtime
                .recover_approval_continuation_with_context(request, context)
                .await?
            {
                crabber_runtime::Continuation::Started {
                    receipt,
                    handle: inner,
                } => Continuation::Started {
                    receipt,
                    handle: RunHandle {
                        run_id: inner.run_id().clone(),
                        inner,
                    },
                },
                crabber_runtime::Continuation::Replayed(receipt) => Continuation::Replayed(receipt),
            },
        )
    }
}
