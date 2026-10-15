use crate::{Admission, Agent, RunHandle, RuntimeError};
use async_trait::async_trait;
use crabber_core::InputFingerprint;
use crabber_core::{AdmissionOptions, SessionContext, TraceContext};
use crabber_extension::{ComponentIdentity, Extension, ExtensionError, HostServices, Registrar};
use crabber_providers::Selection;

pub(crate) struct SharedServices {
    pub identity: ComponentIdentity,
    pub config_hash: InputFingerprint,
    pub services: HostServices,
}
#[async_trait]
impl Extension for SharedServices {
    fn id(&self) -> &str {
        &self.identity.id
    }
    fn version(&self) -> &str {
        &self.identity.version
    }
    fn config_hash(&self) -> String {
        self.config_hash.as_str().into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.host_services(self.services.clone());
        Ok(())
    }
}

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

    /// Admit host-validated ordered input in an explicit execution context.
    /// The host authorizes roles/history/artifacts and supplies the revision of
    /// its coherent comparison, even if the wire request omitted If-Match.
    /// Unstable until durable-host qualification.
    /// # Errors
    /// Returns stale-revision, unsupported-input, identity or execution failures.
    pub async fn prompt_input_in(
        &self,
        execution: &ExecutionContext,
        input: crabber_core::InputEnvelope,
        trace: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let mut request = self.context_request(execution, String::new());
        request.input_envelope = Some(input);
        self.start_request(request, trace).await
    }
    /// Admit or reconcile the exact original typed envelope under a stable key.
    /// Retain the envelope for retries; do not recompute it from current state.
    /// # Errors
    /// Returns semantic/revision/identity conflicts or execution failures.
    pub async fn prompt_input_keyed_in(
        &self,
        execution: &ExecutionContext,
        input: crabber_core::InputEnvelope,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let mut request = self.context_request(execution, String::new());
        request.input_envelope = Some(input);
        self.start_keyed_request(request, options, trace).await
    }
    /// Recover a durably Unstarted admission using its exact original typed input.
    /// # Errors
    /// Returns eligibility, semantic/revision, readiness or ownership failures.
    pub async fn recover_input_admission_in(
        &self,
        execution: &ExecutionContext,
        input: crabber_core::InputEnvelope,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let mut request = self.context_request(execution, String::new());
        request.input_envelope = Some(input);
        self.recover_request(request, options, trace).await
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

impl Agent {
    /// Close factory-owned session resources under a caller deadline. This does
    /// not cancel or join active executions. Unstable until host qualification.
    /// # Errors
    /// Returns owned-resource identity errors or typed retained/incomplete cleanup.
    pub async fn close_session_resources(
        &self,
        context: &SessionContext,
        bound: std::time::Duration,
    ) -> Result<(), RuntimeError> {
        if let Some(provider) = &self.contextual {
            provider.close_session(context, bound).await?;
        }
        Ok(())
    }
    /// Close factory-owned workspace and session resources, never workspace files.
    /// This does not cancel active executions. Unstable until host qualification.
    /// # Errors
    /// Returns owned-resource identity errors or typed retained/incomplete cleanup.
    pub async fn close_workspace_resources(
        &self,
        context: &SessionContext,
        bound: std::time::Duration,
    ) -> Result<(), RuntimeError> {
        if let Some(provider) = &self.contextual {
            provider.close_workspace(context, bound).await?;
        }
        Ok(())
    }
}
