//! Fixed-route unique provider products. Only actual ambient-owned calls run.
mod facade;
mod install;
mod node;
pub(super) mod original;
mod registration;
pub(in crate::contextual) use registration::capture;
mod stream;
mod value;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope};
use crabber_core::ProviderBindingDescriptor;
use crabber_providers::{ProviderBindingError, ProviderInfo, Selection, Streamer};
use futures::future::BoxFuture;
pub(in crate::contextual) use install::install;
pub(crate) use node::Node;

/// Move-only provider result. Construction only moves the original body; its
/// metadata is checked after installation in the original retained cell.
pub struct ContextualProviderProduct {
    info: ProviderInfo,
    routes: Vec<ProviderBindingDescriptor>,
    body: Box<dyn ContextualProviderBindingBody>,
}
impl ContextualProviderProduct {
    #[must_use]
    pub fn new(
        info: ProviderInfo,
        routes: Vec<ProviderBindingDescriptor>,
        body: Box<dyn ContextualProviderBindingBody>,
    ) -> Self {
        Self { info, routes, body }
    }
}
/// Actual borrowed binding body of this fixed-route family. Its unique Box
/// never escapes through an adapter or a downcast.
pub trait ContextualProviderBindingBody: Send + Sync {
    fn bind_box(
        &self,
        selection: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>>;
}
/// Move-only returned streamer; install before validating/discarding the result
/// or disposing the original binding future.
pub struct ContextualStreamerProduct {
    descriptor: ProviderBindingDescriptor,
    body: Box<dyn Streamer>,
}
impl ContextualStreamerProduct {
    #[must_use]
    pub fn new(descriptor: ProviderBindingDescriptor, body: Box<dyn Streamer>) -> Self {
        Self { descriptor, body }
    }
}
/// Separate Workspace/Session-only Box family. Resource contracts remain None;
/// original Box/stream disposal is no claim about hidden globals or internals.
pub trait ContextualProviderProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_provider(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualProviderProduct, ExtensionError>>;
}

#[cfg(test)]
mod tests;
