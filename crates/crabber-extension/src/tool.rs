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
/// Persisted workspace identity of the session a run belongs to.
///
/// The values are routing data read from the stored session record, never
/// authorization. A field the session persisted as an empty string is
/// unavailable: it reads as `None` and is never replaced by a host default,
/// the process working directory or model-supplied arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceContext {
    workspace_id: Option<String>,
    directory: Option<String>,
}
impl WorkspaceContext {
    /// JSON key of the workspace ID in the `ContextAssemble` payload.
    pub const WORKSPACE_ID_KEY: &'static str = "workspace_id";
    /// JSON key of the workspace directory in the `ContextAssemble` payload.
    pub const DIRECTORY_KEY: &'static str = "workspace_directory";

    /// Builds the context from persisted session fields. Empty means unavailable.
    #[must_use]
    pub fn from_persisted(workspace_id: &str, directory: &str) -> Self {
        let available = |value: &str| (!value.is_empty()).then(|| value.to_owned());
        Self {
            workspace_id: available(workspace_id),
            directory: available(directory),
        }
    }
    /// A context with neither field available.
    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            workspace_id: None,
            directory: None,
        }
    }
    /// The persisted workspace ID, or `None` when the session has none.
    #[must_use]
    pub fn workspace_id(&self) -> Option<&str> {
        self.workspace_id.as_deref()
    }
    /// The persisted workspace directory, or `None` when the session has none.
    /// It is compared and exposed verbatim: never normalized or resolved.
    #[must_use]
    pub fn directory(&self) -> Option<&str> {
        self.directory.as_deref()
    }
}
#[derive(Clone)]
pub struct ToolContext {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub cancel: CancellationToken,
    pub host: HostServices,
    workspace: WorkspaceContext,
    progress: ProgressSink,
    approval: Option<Arc<dyn ApprovalFacade>>,
}
impl ToolContext {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: SessionId,
        run_id: RunId,
        call_id: ToolCallId,
        cancel: CancellationToken,
        host: HostServices,
        workspace: WorkspaceContext,
        progress: ProgressSink,
        approval: Option<Arc<dyn ApprovalFacade>>,
    ) -> Self {
        Self {
            session_id,
            run_id,
            call_id,
            cancel,
            host,
            workspace,
            progress,
            approval,
        }
    }
    /// Authoritative workspace identity of this call's session. Read-only.
    #[must_use]
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }
    pub fn progress(&self, content: ContentBlock) {
        (self.progress)(content);
    }
    #[must_use]
    pub fn approval(&self) -> Option<&Arc<dyn ApprovalFacade>> {
        self.approval.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_persisted_fields_are_unavailable_independently() {
        let both = WorkspaceContext::from_persisted("ws", "/root");
        assert_eq!(both.workspace_id(), Some("ws"));
        assert_eq!(both.directory(), Some("/root"));
        let id_only = WorkspaceContext::from_persisted("ws", "");
        assert_eq!(id_only.workspace_id(), Some("ws"));
        assert_eq!(id_only.directory(), None);
        let directory_only = WorkspaceContext::from_persisted("", "/root");
        assert_eq!(directory_only.workspace_id(), None);
        assert_eq!(directory_only.directory(), Some("/root"));
        assert_eq!(
            WorkspaceContext::from_persisted("", ""),
            WorkspaceContext::unavailable()
        );
    }

    #[test]
    fn tool_context_exposes_the_workspace_it_was_built_with() {
        let workspace = WorkspaceContext::from_persisted("ws", "/root");
        let context = ToolContext::new(
            SessionId::new(),
            RunId::new(),
            ToolCallId::new(),
            CancellationToken::new(),
            HostServices::default(),
            workspace.clone(),
            Arc::new(|_| {}),
            None,
        );
        assert_eq!(context.workspace(), &workspace);
    }
}
