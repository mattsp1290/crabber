//! Convenience admission that consumes managed defaults instead of an override.
use super::{Admission, Agent, RunHandle, RuntimeError};
use crabber_core::{AdmissionOptions, InputEnvelope, SessionContext, SessionId, TraceContext};
use crabber_session::AdmissionSelectionPolicy;
impl Agent {
    fn default_request(&self, session: SessionId, text: String) -> crabber_runtime::Request {
        self.default_policy(self.request(Some(session), text))
    }
    pub(super) fn default_request_in(
        &self,
        context: &SessionContext,
        text: String,
    ) -> crabber_runtime::Request {
        self.default_policy(self.request_at(
            Some(context.session_id().clone()),
            text,
            context.workspace_id(),
            context.directory(),
            &self.config.selection,
        ))
    }
    fn default_policy(&self, mut request: crabber_runtime::Request) -> crabber_runtime::Request {
        request.selection_policy = AdmissionSelectionPolicy::session_defaults(
            self.config.selection.provider_id.clone(),
            self.config.selection.model_id.clone(),
        );
        request
    }
    /// Start with stored session defaults, falling back independently to each
    /// `AgentConfig` provider/model value when that stored value is absent.
    /// Explicit `ExecutionContext` methods retain their explicit selection.
    /// # Errors
    /// Returns unsupported management, identity, revision or execution failures.
    pub async fn prompt_session(
        &self,
        session: SessionId,
        text: impl Into<String>,
        trace: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        self.start_request(self.default_request(session, text.into()), trace)
            .await
    }
    /// Admit or reconcile the original default-selected binding. Preserve the
    /// original input for exact retries. Current defaults and host fallbacks do
    /// not replace the winner's original resolved binding.
    /// Defaults changed after admission affect only a new operation key.
    /// # Errors
    /// Returns unsupported management, semantic/revision or execution failures.
    pub async fn prompt_session_keyed(
        &self,
        session: SessionId,
        text: impl Into<String>,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.start_keyed_request(self.default_request(session, text.into()), options, trace)
            .await
    }
    /// Start typed ordinary input with defaults captured at its exact revision.
    /// # Errors
    /// Returns unsupported management, stale input or execution failures.
    pub async fn prompt_session_input(
        &self,
        session: SessionId,
        input: InputEnvelope,
        trace: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let mut request = self.default_request(session, String::new());
        request.input_envelope = Some(input);
        self.start_request(request, trace).await
    }
    /// Reconcile typed input against the original default-selected binding.
    /// Never rebuild the original envelope from current session state.
    /// # Errors
    /// Returns unsupported management, semantic/revision or execution failures.
    pub async fn prompt_session_input_keyed(
        &self,
        session: SessionId,
        input: InputEnvelope,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let mut request = self.default_request(session, String::new());
        request.input_envelope = Some(input);
        self.start_keyed_request(request, options, trace).await
    }
    /// Recover a genuinely Unstarted keyed default-selected admission using its
    /// original frozen binding, without rereading current session defaults.
    /// # Errors
    /// Returns eligibility, semantic, readiness or ownership failures.
    pub async fn recover_session_admission(
        &self,
        session: SessionId,
        text: impl Into<String>,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.recover_request(self.default_request(session, text.into()), options, trace)
            .await
    }
    /// Recover a retained Unstarted typed admission using the exact original
    /// envelope and frozen default-selected binding.
    /// # Errors
    /// Returns eligibility, semantic, readiness or ownership failures.
    pub async fn recover_session_input_admission(
        &self,
        session: SessionId,
        input: InputEnvelope,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let mut request = self.default_request(session, String::new());
        request.input_envelope = Some(input);
        self.recover_request(request, options, trace).await
    }
    /// Consume defaults in an explicit host-admitted workspace/session context.
    /// The context carries routing identity, not filesystem authorization.
    /// # Errors
    /// Returns unsupported management, identity, revision or execution failures.
    pub async fn prompt_session_in(
        &self,
        context: &SessionContext,
        text: impl Into<String>,
        trace: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        self.start_request(self.default_request_in(context, text.into()), trace)
            .await
    }
    /// Keyed default-selected admission in an explicit routing context.
    /// # Errors
    /// Returns unsupported management, semantic/revision or execution failures.
    pub async fn prompt_session_keyed_in(
        &self,
        context: &SessionContext,
        text: impl Into<String>,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.start_keyed_request(
            self.default_request_in(context, text.into()),
            options,
            trace,
        )
        .await
    }
    /// Typed ordinary input with coherent defaults in an explicit context.
    /// # Errors
    /// Returns unsupported management, stale input or execution failures.
    pub async fn prompt_session_input_in(
        &self,
        context: &SessionContext,
        input: InputEnvelope,
        trace: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let mut request = self.default_request_in(context, String::new());
        request.input_envelope = Some(input);
        self.start_request(request, trace).await
    }
    /// Keyed typed input retaining the original context and default binding.
    /// # Errors
    /// Returns unsupported management, semantic/revision or execution failures.
    pub async fn prompt_session_input_keyed_in(
        &self,
        context: &SessionContext,
        input: InputEnvelope,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let mut request = self.default_request_in(context, String::new());
        request.input_envelope = Some(input);
        self.start_keyed_request(request, options, trace).await
    }
    /// Restore eligible Unstarted work in its original explicit routing context.
    /// # Errors
    /// Returns eligibility, semantic, readiness or ownership failures.
    pub async fn recover_session_admission_in(
        &self,
        context: &SessionContext,
        text: impl Into<String>,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.recover_request(
            self.default_request_in(context, text.into()),
            options,
            trace,
        )
        .await
    }
    /// Restore eligible Unstarted typed work with its exact original envelope.
    /// # Errors
    /// Returns eligibility, semantic, readiness or ownership failures.
    pub async fn recover_session_input_admission_in(
        &self,
        context: &SessionContext,
        input: InputEnvelope,
        options: AdmissionOptions,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let mut request = self.default_request_in(context, String::new());
        request.input_envelope = Some(input);
        self.recover_request(request, options, trace).await
    }
}
