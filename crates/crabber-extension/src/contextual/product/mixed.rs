//! Explicit composition of the existing Box factory routes and original graph.
use super::{provider, subprocess, tool, tool_guard, workspace_fs, workspace_reader};
use crate::{
    ContextualGenerationHost, ContextualPlanProvider, ContextualProductFactory,
    ContextualProviderProductFactory, ContextualSubprocessProductFactory,
    ContextualToolGuardProductFactory, ContextualToolProductFactory,
    ContextualWorkspaceFsProductFactory, ContextualWorkspaceReaderResolverProductFactory,
    ExtensionError, Registry,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

/// One explicitly selected existing Box product factory.
///
/// Factories retain their Workspace/Session scope and original registration
/// order within that scope. Factory Arcs and hidden resources remain unknown;
/// selecting a variant supplies no cleanup, native or resource-policy evidence.
pub enum ContextualOwnedProductFactory {
    Root(Arc<dyn ContextualProductFactory>),
    Tool(Arc<dyn ContextualToolProductFactory>),
    Provider(Arc<dyn ContextualProviderProductFactory>),
    WorkspaceFs(Arc<dyn ContextualWorkspaceFsProductFactory>),
    Subprocess(Arc<dyn ContextualSubprocessProductFactory>),
    WorkspaceReaderResolver(Arc<dyn ContextualWorkspaceReaderResolverProductFactory>),
    ToolGuard(Arc<dyn ContextualToolGuardProductFactory>),
}

impl ContextualPlanProvider {
    /// Compose existing Box product families in one original generation engine.
    ///
    /// Metadata uses each family's unchanged encoding. Factory IDs are unique
    /// across every family and scope; each selected Workspace/Session generation
    /// retains the existing 64-member limit, with no total-provider limit of 64.
    /// This supplies no native permission or complete resource inventory.
    ///
    /// # Errors
    /// Refuses invalid, duplicate or panicking factory metadata before any
    /// constructor runs. Selected-generation capacity is checked before its Work
    /// registration, exactly as with the existing single-family constructors.
    pub fn new_with_owned_products(
        registry: Registry,
        factories: Vec<ContextualOwnedProductFactory>,
        initialization_timeout: Duration,
        host: Arc<dyn ContextualGenerationHost>,
    ) -> Result<Self, ExtensionError> {
        let mut names = BTreeSet::new();
        let registrations = factories
            .into_iter()
            .map(|factory| match factory {
                ContextualOwnedProductFactory::Root(factory) => super::capture(factory, &mut names),
                ContextualOwnedProductFactory::Tool(factory) => tool::capture(factory, &mut names),
                ContextualOwnedProductFactory::Provider(factory) => {
                    provider::capture(factory, &mut names)
                }
                ContextualOwnedProductFactory::WorkspaceFs(factory) => {
                    workspace_fs::capture(factory, &mut names)
                }
                ContextualOwnedProductFactory::Subprocess(factory) => {
                    subprocess::capture(factory, &mut names)
                }
                ContextualOwnedProductFactory::WorkspaceReaderResolver(factory) => {
                    workspace_reader::capture(factory, &mut names)
                }
                ContextualOwnedProductFactory::ToolGuard(factory) => {
                    tool_guard::capture(factory, &mut names)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut provider =
            Self::new_with_generation_host(registry, vec![], vec![], initialization_timeout, host)?;
        Arc::get_mut(&mut provider.inner)
            .ok_or(ExtensionError::ContextInitializationIncomplete)?
            .factories = registrations;
        Ok(provider)
    }
}
