//! Host runner request and response carriers. Runtime attribution is explicit.
use super::{Limits, failure};
use crabber::{
    core::{RunId, SessionId, ToolCallId},
    extension::{ExtensionError, SessionInvocations, ToolContext, WorkspaceContext},
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// Authoritative request; no public constructor permits forged context.
/// Hosts can test runners through a mounted tool and `ToolContext::new`.
#[derive(Clone)]
pub struct Request {
    session_id: SessionId,
    run_id: RunId,
    call_id: ToolCallId,
    workspace: WorkspaceContext,
    task: String,
    profile: String,
    cancellation: CancellationToken,
    deadline: Duration,
    background: bool,
    pub(super) invocations: SessionInvocations,
}
impl Request {
    pub(super) fn new(
        context: &ToolContext,
        (task, profile): (String, String),
        cancellation: CancellationToken,
        deadline: Duration,
        background: bool,
    ) -> Self {
        Self {
            session_id: context.session_id.clone(),
            run_id: context.run_id.clone(),
            call_id: context.call_id.clone(),
            workspace: context.workspace().clone(),
            task,
            profile,
            cancellation,
            deadline,
            background,
            invocations: context.session_invocations().clone(),
        }
    }

    /// Spawn nested jobs through this handle to retain actual parent attribution.
    /// A separate child Agent does not inherit this link automatically.
    pub fn session_invocations(&self) -> &SessionInvocations {
        &self.invocations
    }

    /// Parent session identity supplied by Crabber.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    /// Parent run identity supplied by Crabber.
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    /// Parent tool-call identity supplied by Crabber.
    pub fn call_id(&self) -> &ToolCallId {
        &self.call_id
    }
    /// Persisted workspace routing data, not a filesystem capability.
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }
    /// Bounded task preserved exactly from model input.
    pub fn task(&self) -> &str {
        &self.task
    }
    /// Opaque host profile preserved exactly from model input.
    pub fn profile(&self) -> &str {
        &self.profile
    }
    /// Inline tokens cancel on interruption, deadline or executor exit.
    /// Background tokens cancel on their deadline, extension/owner close or a host
    /// drain_session_invocations cancellation request.
    /// Cancelling this token does not cancel the parent run.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
    /// Whether this request runs independently of its parent invocation.
    pub fn background(&self) -> bool {
        self.background
    }
    /// Runner deadline duration: inline max_wait or max_background_duration.
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
    /// Alias for [`Self::deadline`].
    pub fn max_wait(&self) -> Duration {
        self.deadline()
    }
}

/// Host-trusted output, stored verbatim within the configured byte bound.
pub enum Response {
    /// Task completed successfully.
    Completed(String),
    /// Task failed with host-visible explanation.
    Failed(String),
    /// Host rejected the task or profile.
    Rejected(String),
    /// Host runner is unavailable.
    Unavailable(String),
}
impl Response {
    pub(super) fn status_output(&self) -> (&'static str, &str) {
        match self {
            Self::Completed(out) => ("completed", out),
            Self::Failed(out) => ("failed", out),
            Self::Rejected(out) => ("rejected", out),
            Self::Unavailable(out) => ("unavailable", out),
        }
    }
    pub(super) fn checked(self, limits: &Limits) -> Result<Self, ExtensionError> {
        let (_, output) = self.status_output();
        if output.len() > limits.max_result_bytes || output.contains('\0') {
            return Err(failure("runner"));
        }
        Ok(self)
    }
}

/// Host callback: inline work is extension-owned; background work is a runtime
/// session invocation.
///
/// Background tokens fire on the deadline, extension/owner close or a host
/// session-invocation drain cancellation, never on parent call/run completion.
/// Invoked concurrently up to `Limits::max_in_flight`. The future must not
/// block while polled, must observe `Request::cancellation`, and return promptly
/// after cancellation. Capacity stays held through future disposal and the final
/// background activity publication attempt. The token
/// may already be cancelled at first poll: check it before starting side effects.
/// The host owns any child agents, sandbox, model/tool policy, credentials and
/// resource limits; `profile` is opaque routing data. Inputs and outputs are
/// durable and must not contain secrets. Side effects are not transactional.
/// Any `Err` payload is discarded and replaced by a fixed failure; use
/// `Response::Failed` or `Response::Rejected` for host-visible failure text.
/// Valid response text is host-trusted and stored unredacted within its bound.
pub type Runner = Arc<
    dyn Fn(Request) -> Pin<Box<dyn Future<Output = Result<Response, ExtensionError>> + Send>>
        + Send
        + Sync,
>;
