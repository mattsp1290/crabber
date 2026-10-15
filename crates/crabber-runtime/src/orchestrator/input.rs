//! Capture typed input once; every restart reuses the original semantic envelope.
use super::{Orchestrator, Request, RuntimeError};
use crabber_core::{AdmissionKind, InputEnvelope, SessionId, SessionRevision};
use crabber_session::{AdmissionExecutionCapsule, ExecutionBindingCapsule, StoreError};

impl Request {
    pub(super) fn from_binding(binding: &ExecutionBindingCapsule) -> Self {
        let mut request = Self::from_admission(binding.request.clone());
        request.input_envelope.clone_from(&binding.input_envelope);
        request
    }
    pub(super) fn from_execution(capsule: &AdmissionExecutionCapsule) -> Self {
        let mut request = Self::from_admission(capsule.request.clone());
        request.input_envelope.clone_from(&capsule.input_envelope);
        request
    }
}
impl Orchestrator {
    pub(super) async fn capture_input(
        &self,
        request: &Request,
        session: &SessionId,
    ) -> Result<InputEnvelope, RuntimeError> {
        let input = if let Some(input) = &request.input_envelope {
            if !request.text.is_empty() && input.legacy_text() != Some(request.text.as_str()) {
                return Err(RuntimeError::InvalidConfiguration(
                    "text and typed input disagree",
                ));
            }
            input.clone()
        } else {
            let revision = match self.store.session_revision(session).await {
                Ok(revision) => revision,
                Err(StoreError::NotFound) => SessionRevision::INITIAL,
                Err(error) => return Err(error.into()),
            };
            InputEnvelope::text(request.text.clone(), revision)?
        };
        // Waiting frontend calls and continuation authority are installed with
        // the durable interaction driver; never silently treat them as ordinary.
        if !input.frontend_tools().is_empty()
            || !matches!(input.admission_kind(), AdmissionKind::Ordinary)
        {
            return Err(StoreError::HostUnsupported.into());
        }
        Ok(input)
    }
}
