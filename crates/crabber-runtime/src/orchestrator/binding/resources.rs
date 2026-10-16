//! Frozen provider acquisition policy is distinct from whole-Run coverage.
use super::{
    BoundProvider, InputFingerprint, Orchestrator, ProviderBindingDescriptor, ProviderBindingError,
    RunPlan, RuntimeError, Selection,
};
use crabber_providers::ProviderResourceContext;
pub(super) const COMPONENT: &str = "contract:crabber/provider-binding-resources.v1";

impl Orchestrator {
    pub(in crate::orchestrator) fn provider_resource_fingerprint(
        &self,
        plan: &RunPlan,
        descriptor: &ProviderBindingDescriptor,
    ) -> Result<Option<InputFingerprint>, RuntimeError> {
        let selection = Selection {
            provider_id: descriptor.provider_id().into(),
            model_id: descriptor.model_id().into(),
        };
        let contract = if let Some(provider) = plan
            .providers
            .iter()
            .find(|p| p.info().id == descriptor.provider_id())
        {
            provider.binding_resource_contract(&selection)?
        } else {
            self.resolver.binding_resource_contract(&selection)?
        };
        Ok(contract.map(|contract| contract.fingerprint()))
    }

    pub(in crate::orchestrator) fn provider_resource_context(
        &self,
        plan: &RunPlan,
        descriptor: &ProviderBindingDescriptor,
    ) -> Result<Option<ProviderResourceContext>, RuntimeError> {
        let mut components = plan
            .components
            .iter()
            .filter(|component| component.id == COMPONENT);
        let frozen = components
            .next()
            .map(|component| InputFingerprint::new(component.version.clone()))
            .transpose()
            .map_err(|_| ProviderBindingError::InvalidDescriptor)?;
        if components.next().is_some()
            || frozen != self.provider_resource_fingerprint(plan, descriptor)?
        {
            return Err(ProviderBindingError::VersionConflict.into());
        }
        frozen
            .map(|_| {
                self.preparation_scope
                    .as_ref()
                    .map(|scope| scope.context.provider_resources())
                    .ok_or_else(|| ProviderBindingError::ResourcesUnsupported.into())
            })
            .transpose()
    }

    /// Initial work carries owned bindings across the native admission commit;
    /// legacy providers retain their existing acquisition timing.
    pub(in crate::orchestrator) async fn prebind_owned_provider(
        &self,
        plan: &RunPlan,
        descriptor: &ProviderBindingDescriptor,
    ) -> Result<Option<BoundProvider>, RuntimeError> {
        if self.provider_resource_context(plan, descriptor)?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.restore_provider(plan, descriptor).await?))
    }
}
