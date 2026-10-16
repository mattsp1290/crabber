//! Host-facing explicit tool state-machine continuation without native tokens.
use super::{Agent, ContinueTool, RunHandle, RuntimeError};
use crabber_core::{RunId, SessionId, TraceContext};
use crabber_session::{
    ToolContinuationError, ToolContinuationReceipt, ToolContinuationSnapshot, ToolInteractionAnswer,
};
pub enum ToolContinuation {
    Started {
        receipt: ToolContinuationReceipt,
        handle: RunHandle,
    },
    Replayed(ToolContinuationReceipt),
}
impl Agent {
    /// Read safe pending interaction metadata, including after extension close.
    /// # Errors
    /// Returns scope, unsupported or authoritative integrity errors.
    pub async fn tool_continuation_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<ToolContinuationSnapshot>, ToolContinuationError> {
        self.runtime.tool_continuation_snapshot(session, run).await
    }
    /// Persist a response after host authentication and authorization.
    /// # Errors
    /// Rejects stale, mismatched, expired or changed original responses.
    pub async fn answer_tool_interaction(
        &self,
        answer: ToolInteractionAnswer,
    ) -> Result<ToolContinuationReceipt, ToolContinuationError> {
        self.runtime.answer_tool_interaction(answer).await
    }
    /// Resume the exact next checkpoint phase once; identical replay is metadata.
    /// # Errors
    /// Retain the original request to reconcile unknown acknowledgement.
    pub async fn continue_tool(
        &self,
        request: ContinueTool,
    ) -> Result<ToolContinuation, RuntimeError> {
        self.continue_tool_with_context(request, None).await
    }
    /// Trace correlation is presentation, never answer or dispatch authority.
    /// # Errors
    /// Returns the same eligibility and preparation failures as continuation.
    pub async fn continue_tool_with_context(
        &self,
        request: ContinueTool,
        context: Option<TraceContext>,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(receipt) = self.runtime.lookup_tool_continuation(&request).await? {
            return Ok(ToolContinuation::Replayed(receipt));
        }
        self.check_ingress()?;
        Ok(
            match self
                .runtime
                .continue_tool_with_context(request, context)
                .await?
            {
                crabber_runtime::ToolContinuation::Started {
                    receipt,
                    handle: inner,
                } => ToolContinuation::Started {
                    receipt,
                    handle: RunHandle {
                        run_id: inner.run_id().clone(),
                        inner,
                    },
                },
                crabber_runtime::ToolContinuation::Replayed(receipt) => {
                    ToolContinuation::Replayed(receipt)
                }
            },
        )
    }
}

impl Agent {
    /// Reclaim only an expired continuation whose reserved attempt never began.
    /// # Errors
    /// Retain original claim and recovery identities to reconcile lost ACKs.
    pub async fn recover_tool_continuation(
        &self,
        request: crabber_runtime::RecoverTool,
    ) -> Result<ToolContinuation, RuntimeError> {
        if let Some(receipt) = self.runtime.lookup_tool_recovery(&request).await? {
            return Ok(ToolContinuation::Replayed(receipt));
        }
        self.check_ingress()?;
        Ok(
            match self.runtime.recover_tool_continuation(request).await? {
                crabber_runtime::ToolContinuation::Started {
                    receipt,
                    handle: inner,
                } => ToolContinuation::Started {
                    receipt,
                    handle: RunHandle {
                        run_id: inner.run_id().clone(),
                        inner,
                    },
                },
                crabber_runtime::ToolContinuation::Replayed(receipt) => {
                    ToolContinuation::Replayed(receipt)
                }
            },
        )
    }
}
