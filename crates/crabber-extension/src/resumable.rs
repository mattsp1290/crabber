//! Opt-in native state machines. A resume never calls the start entry point.
use crate::{ExtensionError, ToolContext, ToolDefinition, ToolExecutor};
use async_trait::async_trait;
use crabber_core::{
    AcceptedToolResponse, ResumableToolIdentity, ToolCheckpoint, ToolInfo, ToolPhaseOutcome,
};
use serde_json::Value;
use std::sync::Arc;

#[async_trait]
pub trait ResumableTool: Send + Sync {
    /// Immutable behavior/configuration and checkpoint schema declaration.
    fn identity(&self) -> ResumableToolIdentity;
    async fn start(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError>;
    async fn resume(
        &self,
        context: ToolContext,
        arguments: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError>;
}
struct Adapter {
    tool: Arc<dyn ResumableTool>,
    identity: ResumableToolIdentity,
}
#[async_trait]
impl ToolExecutor for Adapter {
    async fn execute(&self, _: Value) -> Result<crabber_core::ToolOutput, ExtensionError> {
        Err(ExtensionError::DurableInteractionUnsupported(
            crate::DurableInteractionUnsupported,
        ))
    }
    fn resumable_identity(&self) -> Option<ResumableToolIdentity> {
        Some(self.identity.clone())
    }
    fn resumable(&self) -> Option<&dyn ResumableTool> {
        Some(&*self.tool)
    }
}
impl ToolDefinition {
    /// Register an explicit versioned state machine. Ordinary executors remain
    /// run-to-completion and cannot obtain this capability dynamically.
    /// # Errors
    /// Rejects an unbounded or incomplete behavior/schema declaration.
    pub fn resumable(info: ToolInfo, tool: Arc<dyn ResumableTool>) -> Result<Self, ExtensionError> {
        let identity = tool.identity();
        identity
            .validate()
            .map_err(|_| ExtensionError::Tool("invalid resumable tool identity".into()))?;
        Ok(Self {
            info,
            executor: Arc::new(Adapter { tool, identity }),
        })
    }
    pub(crate) fn resumable_identity(&self) -> Option<ResumableToolIdentity> {
        self.executor.resumable_identity()
    }
}
