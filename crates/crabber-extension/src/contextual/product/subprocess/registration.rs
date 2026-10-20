use super::{ContextualSubprocessProductFactory, Node};
use crate::contextual::{FactoryBody, FactoryRegistration};
use crate::{
    ComponentIdentity, ContextualGenerationHost, ContextualPlanProvider, Extension, ExtensionError,
    Registrar, Registry,
};
use async_trait::async_trait;
use std::{
    collections::BTreeSet,
    sync::{Arc, Weak},
    time::Duration,
};
pub(in crate::contextual) fn capture(
    factory: Arc<dyn ContextualSubprocessProductFactory>,
    names: &mut BTreeSet<String>,
) -> Result<FactoryRegistration, ExtensionError> {
    let (identity, config, scope) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (factory.identity(), factory.config_hash(), factory.scope())
    }))
    .map_err(|payload| {
        crate::dispatch::discard_panic_payload(payload);
        ExtensionError::RegistrationDescriptorUnavailable
    })?;
    super::super::metadata::validate(
        &identity,
        &config,
        names,
        "invalid Box subprocess factory metadata",
    )?;
    let config_hash = crate::plan::registration_hash(serde_json::json!([
        "crabber.contextual-subprocess-product-cell.v1",
        identity,
        match scope {
            crate::FactoryScope::Workspace => "workspace",
            crate::FactoryScope::Session => "session",
        },
        crate::plan::registration_config(&config)
    ]))?
    .as_str()
    .to_owned();
    Ok(FactoryRegistration {
        root_component: Some(ComponentIdentity {
            id: "contextual.subprocess-product-cell".into(),
            version: format!("v1:{config_hash}"),
        }),
        identity,
        config_hash,
        scope,
        factory: FactoryBody::SubprocessProduct(factory),
    })
}

impl ContextualPlanProvider {
    /// Opt into Box subprocess nodes with final projected-reference disposal.
    /// Legacy Arc/root-only factories and policy meanings remain unchanged.
    /// # Errors
    /// Refuses invalid or duplicate factory metadata before constructor execution.
    pub fn new_with_subprocess_products(
        registry: Registry,
        factories: Vec<Arc<dyn ContextualSubprocessProductFactory>>,
        timeout: Duration,
        host: Arc<dyn ContextualGenerationHost>,
    ) -> Result<Self, ExtensionError> {
        let mut names = BTreeSet::new();
        let mut registrations = Vec::new();
        for factory in factories {
            registrations.push(capture(factory, &mut names)?);
        }
        let mut provider = Self::new_with_generation_host(registry, vec![], vec![], timeout, host)?;
        Arc::get_mut(&mut provider.inner)
            .ok_or(ExtensionError::ContextInitializationIncomplete)?
            .factories = registrations;
        Ok(provider)
    }
}
pub(in crate::contextual) struct SubprocessRegistration {
    pub(in crate::contextual) node: Weak<Node>,
    pub(in crate::contextual) identity: ComponentIdentity,
    pub(in crate::contextual) config: String,
}
#[async_trait]
impl Extension for SubprocessRegistration {
    fn id(&self) -> &str {
        &self.identity.id
    }
    fn version(&self) -> &str {
        &self.identity.version
    }
    fn config_hash(&self) -> String {
        self.config.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.product_subprocess(
            self.node
                .upgrade()
                .ok_or(ExtensionError::ContextOwnerClosed)?,
        )
    }
}
