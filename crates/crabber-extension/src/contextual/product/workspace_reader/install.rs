use super::{Node, construction, registration::WorkspaceReaderResolverRegistration};
use crate::contextual::{FactoryBody, FactoryRegistration, Slot};
use crate::{ContextualPlanProvider, ExtensionError, FactoryContext, FactoryScope};
use std::sync::Arc;
pub(in crate::contextual) async fn install(
    provider: &ContextualPlanProvider,
    slot: &Arc<Slot>,
    scope: FactoryScope,
    index: usize,
    registration: &FactoryRegistration,
    context: FactoryContext,
) -> Result<(), ExtensionError> {
    let FactoryBody::WorkspaceReaderResolverProduct(factory) = &registration.factory else {
        unreachable!("typed reader resolver product registration")
    };
    let node = Node::new(slot.owner.clone());
    slot.state
        .lock()
        .unwrap()
        .producer
        .as_ref()
        .expect("reader resolver product has host")
        .record_workspace_reader_resolver(index, node.clone());
    construction::create(factory, context, &node).await?;
    provider.check_slot(slot)?;
    let scope = super::super::mount::scope(slot, scope);
    let extension = Arc::new(WorkspaceReaderResolverRegistration {
        node: Arc::downgrade(&node),
        identity: registration.identity.clone(),
        config: registration.config_hash.clone(),
    });
    super::super::mount::publish(
        provider,
        slot,
        scope,
        index,
        extension,
        "reader resolver product has host",
    )
    .await
}
