//! Host capabilities and per-invocation native tool context.
use async_trait::async_trait;
use crabber_core::{ContentBlock, RunId, SessionId, ToolCallId};
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub type ProgressSink = Arc<dyn Fn(ContentBlock) + Send + Sync>;
#[async_trait]
pub trait ApprovalFacade: Send + Sync {
    async fn request(&self, reason: &str) -> bool;
}
#[async_trait]
pub trait UserPrompter: Send + Sync {
    async fn ask(&self, prompt: &str) -> Result<String, String>;
}
#[async_trait]
pub trait Subprocess: Send + Sync {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String>;
}
#[async_trait]
pub trait WorkspaceFs: Send + Sync {
    async fn read(&self, relative_path: &str) -> Result<Vec<u8>, String>;
}
#[derive(Clone, Default)]
pub struct HostServices {
    pub user_prompter: Option<Arc<dyn UserPrompter>>,
    pub subprocess: Option<Arc<dyn Subprocess>>,
    pub workspace_fs: Option<Arc<dyn WorkspaceFs>>,
}
#[derive(Clone)]
pub struct ToolContext {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub cancel: CancellationToken,
    pub host: HostServices,
    progress: ProgressSink,
    approval: Option<Arc<dyn ApprovalFacade>>,
}
impl ToolContext {
    #[must_use]
    pub fn new(
        session_id: SessionId,
        run_id: RunId,
        call_id: ToolCallId,
        cancel: CancellationToken,
        host: HostServices,
        progress: ProgressSink,
        approval: Option<Arc<dyn ApprovalFacade>>,
    ) -> Self {
        Self {
            session_id,
            run_id,
            call_id,
            cancel,
            host,
            progress,
            approval,
        }
    }
    pub fn progress(&self, content: ContentBlock) {
        (self.progress)(content);
    }
    #[must_use]
    pub fn approval(&self) -> Option<&Arc<dyn ApprovalFacade>> {
        self.approval.as_ref()
    }
}
