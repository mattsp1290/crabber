//! Public external results are host assertions, not tool execution capabilities.
use super::{Agent, RuntimeError, ToolContinuation};
use crabber_runtime::{ContinueFrontend, RecoverFrontend};
impl Agent {
    /// Consume the complete external result set once. Exact replay stays pure,
    /// including after the Agent has closed its extensions.
    /// # Errors
    /// Rejects stale, incomplete, mismatched or unauthenticated host assertions.
    pub async fn continue_frontend(
        &self,
        request: ContinueFrontend,
    ) -> Result<ToolContinuation, RuntimeError> {
        self.continue_frontend_with_context(request, None).await
    }
    /// Trace presentation grants no result authority.
    /// # Errors
    /// Returns the same validation and owned preparation errors as continuation.
    pub async fn continue_frontend_with_context(
        &self,
        request: ContinueFrontend,
        context: Option<crabber_core::TraceContext>,
    ) -> Result<ToolContinuation, RuntimeError> {
        Ok(self
            .runtime
            .continue_frontend_with_context(request, context)
            .await?
            .into())
    }
    /// Recover only an expired reservation whose result protection never began.
    /// # Errors
    /// Started or uncertain phases require host reconciliation, never replay.
    pub async fn recover_frontend_continuation(
        &self,
        request: RecoverFrontend,
    ) -> Result<ToolContinuation, RuntimeError> {
        Ok(self
            .runtime
            .recover_frontend_continuation(request)
            .await?
            .into())
    }
}
