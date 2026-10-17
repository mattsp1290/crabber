use super::{ContextualToolGuardProductFactory, Node};
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
impl ContextualPlanProvider {
    /// Opt into Box `ToolGuard` nodes with final projected-reference disposal.
    /// Legacy Arc/root-only factories and policy meanings remain unchanged.
    /// # Errors
    /// Refuses invalid or duplicate factory metadata before constructor execution.
    pub fn new_with_tool_guard_products(
        registry: Registry,
        factories: Vec<Arc<dyn ContextualToolGuardProductFactory>>,
        timeout: Duration,
        host: Arc<dyn ContextualGenerationHost>,
    ) -> Result<Self, ExtensionError> {
        let mut names = BTreeSet::new();
        let mut registrations = Vec::new();
        for factory in factories {
            let (identity, config, scope) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    (factory.identity(), factory.config_hash(), factory.scope())
                }))
                .map_err(|payload| {
                    crate::dispatch::discard_panic_payload(payload);
                    ExtensionError::RegistrationDescriptorUnavailable
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
                    "invalid Box `ToolGuard` factory metadata".into(),
                ));
            }
            let config_hash = crate::plan::registration_hash(serde_json::json!([
                "crabber.contextual-tool-guard-product-cell.v1",
                identity,
                match scope {
                    crate::FactoryScope::Workspace => "workspace",
                    crate::FactoryScope::Session => "session",
                },
                crate::plan::registration_config(&config)
            ]))?
            .as_str()
            .to_owned();
            registrations.push(FactoryRegistration {
                root_component: Some(ComponentIdentity {
                    id: "contextual.tool-guard-product-cell".into(),
                    version: format!("v1:{config_hash}"),
                }),
                identity,
                config_hash,
                scope,
                factory: FactoryBody::ToolGuardProduct(factory),
            });
        }
        let mut provider = Self::new_with_generation_host(registry, vec![], vec![], timeout, host)?;
        Arc::get_mut(&mut provider.inner)
            .ok_or(ExtensionError::ContextInitializationIncomplete)?
            .factories = registrations;
        Ok(provider)
    }
}
pub(in crate::contextual) struct ToolGuardRegistration {
    pub(in crate::contextual) node: Weak<Node>,
    pub(in crate::contextual) identity: ComponentIdentity,
    pub(in crate::contextual) config: String,
}
#[async_trait]
impl Extension for ToolGuardRegistration {
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
        registrar.product_tool_guard(
            self.node
                .upgrade()
                .ok_or(ExtensionError::ContextOwnerClosed)?,
        )
    }
}
