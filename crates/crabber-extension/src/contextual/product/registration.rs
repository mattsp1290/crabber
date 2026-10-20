use super::{
    super::{ContextualPlanProvider, FactoryBody, FactoryRegistration},
    ContextualProductFactory,
};
use crate::{ContextualGenerationHost, ExtensionError, Registry};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
pub(in crate::contextual) fn capture(
    factory: Arc<dyn ContextualProductFactory>,
    names: &mut BTreeSet<String>,
) -> Result<FactoryRegistration, ExtensionError> {
    let (identity, config, scope) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (factory.identity(), factory.config_hash(), factory.scope())
    }))
    .map_err(|payload| {
        crate::dispatch::discard_panic_payload(payload);
        ExtensionError::ContextInitializationFailed {
            component: "root-factory-metadata".into(),
        }
    })?;
    if identity.id.is_empty()
        || identity.id.len() > 128
        || identity.id.chars().any(char::is_control)
        || identity.version.is_empty()
        || identity.version.len() > 128
        || identity.version.chars().any(char::is_control)
        || !names.insert(identity.id.clone())
        || config.len() > 16384
    {
        return Err(ExtensionError::Plan(
            "invalid or duplicate root factory metadata".into(),
        ));
    }
    let config_hash = crate::plan::registration_hash(serde_json::json!([
        "crabber.contextual-product-root-cell.v1",
        match scope {
            super::FactoryScope::Workspace => "workspace",
            super::FactoryScope::Session => "session",
        },
        identity,
        crate::plan::registration_config(&config)
    ]))?
    .as_str()
    .to_owned();
    let root_component = Some(crate::ComponentIdentity {
        id: "contextual.product-root-cell".into(),
        version: format!("v1:{config_hash}"),
    });
    Ok(FactoryRegistration {
        root_component,
        identity,
        config_hash,
        scope,
        factory: FactoryBody::ProductRoot(factory),
    })
}

impl ContextualPlanProvider {
    /// Create a separate non-exposing Box-root factory family. The local host
    /// gates the actual constructor. No root object or callback is exposed.
    /// Existing cleanup-policy metadata still describes registered cleanup only;
    /// original Box disposal is separate typed evidence, never native/physical
    /// permission, complete resource inventory or a whole-Run policy.
    /// # Errors
    /// Refuses invalid/duplicate metadata; no constructor is invoked here.
    pub fn new_with_product_roots(
        registry: Registry,
        factories: Vec<Arc<dyn ContextualProductFactory>>,
        initialization_timeout: Duration,
        host: Arc<dyn ContextualGenerationHost>,
    ) -> Result<Self, ExtensionError> {
        let mut names = BTreeSet::new();
        let mut registrations = Vec::new();
        for factory in factories {
            registrations.push(capture(factory, &mut names)?);
        }
        let mut provider =
            Self::new_with_generation_host(registry, vec![], vec![], initialization_timeout, host)?;
        Arc::get_mut(&mut provider.inner)
            .ok_or(ExtensionError::ContextInitializationIncomplete)?
            .factories = registrations;
        Ok(provider)
    }
}

impl ContextualPlanProvider {
    pub(in crate::contextual) fn append_root_components(&self, plan: &mut crate::RunPlan) {
        let mut roots = self
            .inner
            .factories
            .iter()
            .filter(|factory| factory.root_component.is_some())
            .collect::<Vec<_>>();
        roots.sort_by_key(|factory| match factory.scope {
            super::FactoryScope::Workspace => 0,
            super::FactoryScope::Session => 1,
        });
        if !roots.is_empty() {
            plan.components.extend(
                roots
                    .into_iter()
                    .filter_map(|factory| factory.root_component.clone()),
            );
            plan.fingerprint = crate::compute_fingerprint(&plan.components);
        }
    }
}
