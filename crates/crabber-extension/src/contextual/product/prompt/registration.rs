use super::{ContextualPromptProductFactory, Node};
use crate::contextual::{FactoryBody, FactoryRegistration};
use crate::{ComponentIdentity, Extension, ExtensionError, Registrar};
use async_trait::async_trait;
use std::{
    collections::BTreeSet,
    sync::{Arc, Weak},
};
pub(in crate::contextual) fn capture(
    factory: Arc<dyn ContextualPromptProductFactory>,
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
        "invalid Box `Prompt` factory metadata",
    )?;
    let config_hash = crate::plan::registration_hash(serde_json::json!([
        "crabber.contextual-prompt-product-cell.v1",
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
            id: "contextual.prompt-product-cell".into(),
            version: format!("v1:{config_hash}"),
        }),
        identity,
        config_hash,
        scope,
        factory: FactoryBody::PromptProduct(factory),
    })
}

pub(in crate::contextual) struct PromptRegistration {
    pub(in crate::contextual) node: Weak<Node>,
    pub(in crate::contextual) identity: ComponentIdentity,
    pub(in crate::contextual) config: String,
}
#[async_trait]
impl Extension for PromptRegistration {
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
        registrar.product_prompt(
            self.node
                .upgrade()
                .ok_or(ExtensionError::ContextOwnerClosed)?,
        )
    }
}
