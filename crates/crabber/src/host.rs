use crate::{Admission, Agent, RunHandle, RuntimeError};
use crabber_core::{AdmissionOptions, SessionContext, TraceContext};
use crabber_providers::Selection;

/// Unstable until the durable-host consumer contract is qualified.
///
/// An explicit session/workspace and host-allowed model selection for admission.
///
/// Registrations, policy, store, prompt and runtime settings remain owned by the
/// configured Agent. A context is immutable and is never a workspace authorization
/// check. The host admits roots and model choices before constructing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    session: SessionContext,
    selection: Selection,
}

impl ExecutionContext {
    #[must_use]
    pub fn new(session: SessionContext, selection: Selection) -> Self {
        Self { session, selection }
    }
    #[must_use]
    pub fn session(&self) -> &SessionContext {
        &self.session
    }
    #[must_use]
    pub fn selection(&self) -> &Selection {
        &self.selection
    }
}

impl Agent {
    /// Initialize the configured shared registrations once, rolling back a
    /// partial mount on failure. Contextual instances are acquired at admission.
    /// Unstable until the durable-host consumer contract is qualified.
    /// # Errors
    /// Returns initialization errors, or `HostClosed` after closing starts.
    pub async fn ready(&self) -> Result<(), RuntimeError> {
        self.initialize_extensions().await
    }

    /// Start using an explicit host-admitted workspace/session and model choice.
    /// The configured tool/extension/policy/store registrations are retained.
    /// This unkeyed entry requires an existing session. Use `prompt_keyed_in`
    /// to atomically create a new host-selected session ID with its first input.
    /// Unstable until the durable-host consumer contract is qualified.
    /// # Errors
    /// Returns identity, admission, readiness, provider or plan failures.
    pub async fn prompt_in(
        &self,
        execution: &ExecutionContext,
        text: impl Into<String>,
        trace: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        self.start_request(self.context_request(execution, text.into()), trace)
            .await
    }

    /// Keyed admission in an explicit context. Preserve this context and input
    /// for exact retries; a replay grants no new executor.
    /// Unstable until the durable-host consumer contract is qualified.
    /// # Errors
    /// Returns the same readiness, identity and admission errors as `prompt_keyed`.
    pub async fn prompt_keyed_in(
        &self,
        execution: &ExecutionContext,
        text: impl Into<String>,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.start_keyed_request(self.context_request(execution, text.into()), options, trace)
            .await
    }

    /// Recover a retained unstarted admission using its original explicit context.
    /// Unstable until the durable-host consumer contract is qualified.
    /// # Errors
    /// Returns eligibility, identity, semantic and readiness errors.
    pub async fn recover_admission_in(
        &self,
        execution: &ExecutionContext,
        text: impl Into<String>,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.recover_request(self.context_request(execution, text.into()), options, trace)
            .await
    }
}
