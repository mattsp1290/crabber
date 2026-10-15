//! Captured configuration and exact provider restoration, with no route fallback.
use super::{Orchestrator, Request, RuntimeError};
use crabber_core::{InputFingerprint, ProviderBindingDescriptor, RunId, SessionId};
use crabber_extension::{ComponentIdentity, RunPlan, compute_fingerprint};
use crabber_providers::{BoundProvider, ProviderBindingError, Selection};
use crabber_session::{
    ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT, PROVIDER_BINDING_COMPONENT, StoreError,
};

impl Orchestrator {
    pub(super) fn describe_provider(
        &self,
        plan: &RunPlan,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, RuntimeError> {
        let descriptor = if let Some(provider) = plan
            .providers
            .iter()
            .find(|p| p.info().id == selection.provider_id)
        {
            provider.binding_descriptor(selection)?
        } else {
            self.resolver.binding_descriptor(selection)?
        };
        if descriptor.provider_id() != selection.provider_id
            || descriptor.model_id() != selection.model_id
        {
            return Err(ProviderBindingError::InvalidDescriptor.into());
        }
        Ok(descriptor)
    }
    pub(super) fn bind_plan_provider(
        mut plan: RunPlan,
        descriptor: &ProviderBindingDescriptor,
        behavior: &InputFingerprint,
    ) -> Result<RunPlan, RuntimeError> {
        if plan.components.iter().any(|c| {
            c.id == PROVIDER_BINDING_COMPONENT
                || c.id == HOST_BEHAVIOR_COMPONENT
                || c.id == super::attempt::DRIVER_COMPONENT
        }) {
            return Err(ProviderBindingError::InvalidDescriptor.into());
        }
        plan.components.push(ComponentIdentity {
            id: PROVIDER_BINDING_COMPONENT.into(),
            version: serde_json::to_string(descriptor)
                .map_err(|_| ProviderBindingError::InvalidDescriptor)?,
        });
        plan.components.push(ComponentIdentity {
            id: HOST_BEHAVIOR_COMPONENT.into(),
            version: behavior.as_str().into(),
        });
        plan.components.push(ComponentIdentity {
            id: super::attempt::DRIVER_COMPONENT.into(),
            version: super::attempt::DRIVER_VERSION.into(),
        });
        plan.fingerprint = compute_fingerprint(&plan.components);
        Ok(plan)
    }
    pub(super) async fn restore_provider(
        &self,
        plan: &RunPlan,
        descriptor: &ProviderBindingDescriptor,
    ) -> Result<BoundProvider, RuntimeError> {
        let restored = if let Some(provider) = plan
            .providers
            .iter()
            .find(|p| p.info().id == descriptor.provider_id())
        {
            provider.restore_binding(descriptor).await?
        } else {
            self.resolver.restore_binding(descriptor).await?
        };
        if restored.descriptor() != descriptor {
            return Err(ProviderBindingError::VersionConflict.into());
        }
        Ok(restored)
    }
    pub(super) async fn stored_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<ExecutionBindingCapsule, RuntimeError> {
        let binding = self
            .store
            .execution_binding(session, run)
            .await?
            .ok_or(crabber_session::AdmissionExecutionError::MissingEvidence)?
            .capsule;
        if binding.version != 3 || binding.input_envelope.is_none() {
            return Err(crabber_session::AdmissionExecutionError::Unsupported.into());
        }
        Ok(binding)
    }
    pub(super) fn verify_binding_request(
        &self,
        request: &Request,
        session: &SessionId,
        binding: &ExecutionBindingCapsule,
    ) -> Result<(), RuntimeError> {
        let mut stable = request.clone();
        stable.session_id = Some(session.clone());
        let input = binding
            .input_envelope
            .as_ref()
            .ok_or(crabber_session::AdmissionExecutionError::Unsupported)?;
        let input_matches = request.input_envelope.as_ref().map_or_else(
            || input.legacy_text() == Some(request.text.as_str()),
            |provided| provided == input,
        );
        if !input_matches
            || stable.admission_data()? != binding.request
            || binding.behavior_fingerprint != self.behavior_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        Ok(())
    }
    pub(super) fn verify_declared_provider(
        &self,
        binding: &ExecutionBindingCapsule,
    ) -> Result<(), RuntimeError> {
        // An extension's frozen registration declares its provider configuration.
        // Actual instance equivalence is checked before restoring execution;
        // metadata replay never initializes an unavailable factory.
        let extension = binding
            .runtime_semantics
            .get(14)
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| {
                values.iter().any(|v| {
                    v.get(0).and_then(serde_json::Value::as_str)
                        == Some(binding.provider.provider_id())
                })
            });
        if !extension {
            let selection = Selection {
                provider_id: binding.provider.provider_id().into(),
                model_id: binding.provider.model_id().into(),
            };
            if self.resolver.binding_descriptor(&selection)? != binding.provider {
                return Err(StoreError::AdmissionConflict.into());
            }
        }
        Ok(())
    }
}
