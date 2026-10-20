use super::{Node, ToolRegistration, construction};
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
    let FactoryBody::ToolProduct(factory) = &registration.factory else {
        unreachable!("typed Tool product registration")
    };
    let node = Node::new(slot.owner.clone());
    slot.state
        .lock()
        .unwrap()
        .producer
        .as_ref()
        .expect("tool product has host")
        .record_tool(index, node.clone());
    construction::create(factory, context, &node).await?;
    provider.check_slot(slot)?;
    let scope = super::super::mount::scope(slot, scope);
    let extension = Arc::new(ToolRegistration {
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
        "tool product has host",
    )
    .await
}
