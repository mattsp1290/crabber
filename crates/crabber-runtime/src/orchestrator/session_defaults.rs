//! Default selection is captured before preparation and frozen by admission CAS.
use super::{Orchestrator, Request, RuntimeError};
use crabber_extension::{ComponentIdentity, RunPlan, compute_fingerprint};
use crabber_session::{
    AdmissionSelectionPolicy, ExecutionBindingCapsule, ManagementError, SELECTION_POLICY_COMPONENT,
    SessionDefaults, StoreError,
};
impl Orchestrator {
    /// Resolve managed defaults without initializing providers or extensions.
    /// Each absent value uses its original host fallback independently. A second
    /// capture must match the first revision, including a caller's typed input.
    /// # Errors
    /// Returns unsupported management, identity, stale revision or storage errors.
    pub async fn resolve_session_defaults(
        &self,
        mut request: Request,
    ) -> Result<Request, RuntimeError> {
        let AdmissionSelectionPolicy::SessionDefaults { fallback } = &request.selection_policy
        else {
            return Ok(request);
        };
        let provider_id = &fallback.provider_id;
        let model_id = &fallback.model_id;
        request.selection_policy.validate()?;
        let session = request
            .session_id
            .as_ref()
            .ok_or(StoreError::AdmissionConflict)?;
        let (defaults, revision) = match self.store.session_summary(session).await {
            Ok(summary) => {
                if summary.session_id != *session
                    || summary.workspace_id != request.workspace_id
                    || summary.directory != request.directory
                {
                    return Err(StoreError::SessionIdentityMismatch.into());
                }
                (summary.defaults, summary.revision)
            }
            // A missing session is captured as INITIAL. Never combine missing
            // defaults with a later revision read: concurrent Create may install
            // nonempty defaults before that read. Admission CAS rejects the race.
            Err(ManagementError::NotFound) => (
                SessionDefaults::default(),
                crabber_core::SessionRevision::INITIAL,
            ),
            Err(error) => return Err(error.into()),
        };
        if request
            .captured_revision
            .is_some_and(|captured| captured != revision)
            || request
                .input_envelope
                .as_ref()
                .is_some_and(|input| input.expected_revision() != revision)
        {
            return Err(StoreError::AdmissionRevisionConflict.into());
        }
        request.selection = crabber_providers::Selection {
            provider_id: defaults.provider_id.unwrap_or_else(|| provider_id.clone()),
            model_id: defaults.model_id.unwrap_or_else(|| model_id.clone()),
        };
        request.captured_revision = Some(revision);
        Ok(request)
    }
    pub(super) fn bind_selection_policy(
        mut plan: RunPlan,
        policy: &AdmissionSelectionPolicy,
    ) -> Result<RunPlan, RuntimeError> {
        policy.validate()?;
        if plan
            .components
            .iter()
            .any(|component| component.id == SELECTION_POLICY_COMPONENT)
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        if !policy.is_explicit() {
            plan.components.push(ComponentIdentity {
                id: SELECTION_POLICY_COMPONENT.into(),
                version: serde_json::to_string(policy)
                    .map_err(|_| StoreError::AdmissionConflict)?,
            });
            plan.fingerprint = compute_fingerprint(&plan.components);
        }
        Ok(plan)
    }
    pub(super) fn original_default_request(
        request: &Request,
        binding: &ExecutionBindingCapsule,
    ) -> Result<Request, RuntimeError> {
        if request.selection_policy.is_explicit() != binding.request.selection_policy.is_explicit()
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        let mut original = request.clone();
        if !request.selection_policy.is_explicit() {
            original.selection_policy = binding.request.selection_policy.clone();
            original.selection = crabber_providers::Selection {
                provider_id: binding.request.provider_id.clone(),
                model_id: binding.request.model_id.clone(),
            };
        }
        Ok(original)
    }
}
