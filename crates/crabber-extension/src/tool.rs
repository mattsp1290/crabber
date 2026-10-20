//! Host capabilities and per-invocation native tool context.
use async_trait::async_trait;
use crabber_core::{ContentBlock, RunId, SessionId, ToolCallId};
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub type ProgressSink = Arc<dyn Fn(ContentBlock) + Send + Sync>;
/// Ordinary tool contexts cannot park a phase on a remote answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("durable interaction requires a resumable tool phase")]
pub struct DurableInteractionUnsupported;

/// Included in every frozen execution plan; removed hooks cannot be restored.
pub const TOOL_INTERACTION_CONTRACT_VERSION: u32 = 2;

#[async_trait]
pub trait Subprocess: Send + Sync {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String>;
}
#[async_trait]
pub trait WorkspaceFs: Send + Sync {
    async fn read(&self, relative_path: &str) -> Result<Vec<u8>, String>;
}
/// Shared native capabilities do not include a live human question hook.
/// ```compile_fail
/// fn live(host: crabber_extension::HostServices) { let _ = host.user_prompter; }
/// ```
/// ```compile_fail
/// use crabber_extension::UserPrompter;
/// ```
/// ```compile_fail
/// use crabber_extension::ApprovalFacade;
/// ```
#[derive(Clone, Default)]
pub struct HostServices {
    pub subprocess: Option<Arc<dyn Subprocess>>,
    pub workspace_fs: Option<Arc<dyn WorkspaceFs>>,
}
impl HostServices {
    /// Overlay explicitly registered capabilities on shared host defaults.
    #[must_use]
    pub fn with_overrides(&self, overrides: &Self) -> Self {
        self.clone().overlay(overrides.clone())
    }
    pub(crate) fn overlay(self, overrides: Self) -> Self {
        Self {
            subprocess: overrides.subprocess.or(self.subprocess),
            workspace_fs: overrides.workspace_fs.or(self.workspace_fs),
        }
    }
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
    ///
    /// Anyone can construct a value, for example in tests. Only the instance the
    /// runtime places in a `ToolContext` is read from the stored session.
    #[must_use]
    pub fn from_persisted(workspace_id: &str, directory: &str) -> Self {
        let available = |value: &str| (!value.is_empty()).then(|| value.to_owned());
        Self {
            workspace_id: available(workspace_id),
            directory: available(directory),
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
/// Ordinary tools have no live approval callback.
/// ```compile_fail
/// fn live(context: crabber_extension::ToolContext) { context.approval(); }
/// ```
#[derive(Clone)]
pub struct ToolContext {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub cancel: CancellationToken,
    pub host: HostServices,
    workspace: WorkspaceContext,
    progress: ProgressSink,
    resources: crate::ToolResources,
    session_invocations: crate::SessionInvocations,
    public_source: crate::PublicSourceWriter,
}
impl ToolContext {
    #[must_use]
    pub fn new(
        session_id: SessionId,
        run_id: RunId,
        call_id: ToolCallId,
        cancel: CancellationToken,
        host: HostServices,
        workspace: WorkspaceContext,
        progress: ProgressSink,
    ) -> Self {
        Self {
            session_id,
            run_id,
            call_id,
            cancel,
            host,
            workspace,
            progress,
            resources: crate::ToolResources::default(),
            session_invocations: crate::SessionInvocations::default(),
            public_source: crate::PublicSourceWriter::default(),
        }
    }
    /// Attach a host-owned task scope; standalone contexts reject registration.
    #[must_use]
    pub fn with_resources(mut self, resources: crate::ToolResources) -> Self {
        self.resources = resources;
        self
    }
    #[must_use]
    pub fn resources(&self) -> &crate::ToolResources {
        &self.resources
    }
    /// Attach a trusted host capability bound to this exact native phase.
    #[must_use]
    pub fn with_session_invocations(mut self, invocations: crate::SessionInvocations) -> Self {
        self.session_invocations = invocations;
        self
    }
    #[must_use]
    pub fn session_invocations(&self) -> &crate::SessionInvocations {
        &self.session_invocations
    }
    /// Attach the trusted runtime's original native source; standalone contexts
    /// retain the unavailable default.
    #[must_use]
    pub fn with_public_source(mut self, source: crate::PublicSourceWriter) -> Self {
        self.public_source = source;
        self
    }
    #[must_use]
    pub fn public_source(&self) -> &crate::PublicSourceWriter {
        &self.public_source
    }
    /// Authoritative workspace identity of this call's session. Read-only.
    #[must_use]
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }
    /// Best-effort display progress for the current call. The Runtime forwards
    /// bounded text only; non-text and oversized updates are dropped. Use the
    /// native public-source writer for durable UI updates; native tool progress
    /// there also validates text only. Return typed tool output for media results.
    pub fn progress(&self, content: ContentBlock) {
        (self.progress)(content);
    }
    /// Ordinary tools return promptly when requesting a capability that needs
    /// durable suspension. Opt-in tools instead return a typed suspension from
    /// their phase handler; no host callback is invoked from this context.
    /// # Errors
    /// Always returns the finite unsupported error without creating a future.
    pub fn request_interaction(
        &self,
        _description: &crabber_core::InteractionDescription,
    ) -> Result<std::convert::Infallible, DurableInteractionUnsupported> {
        Err(DurableInteractionUnsupported)
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
            WorkspaceContext {
                workspace_id: None,
                directory: None,
            }
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
        );
        assert_eq!(context.workspace(), &workspace);
    }
}
