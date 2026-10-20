use super::Node;
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
    let FactoryBody::ProviderProduct(factory) = &registration.factory else {
        unreachable!("typed Tool product registration")
    };
    let node = Node::new(slot.owner.clone());
    slot.state
        .lock()
        .unwrap()
        .producer
        .as_ref()
        .expect("provider product has host")
        .record_provider(index, node.clone());
    create(factory, context, &node).await?;
    provider.check_slot(slot)?;
    let scope = super::super::mount::scope(slot, scope);
    let extension = Arc::new(super::registration::ProviderRegistration {
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
        "provider product has host",
    )
    .await
}

struct Original {
    node: Arc<Node>,
    done: bool,
}
impl Drop for Original {
    fn drop(&mut self) {
        if !self.done {
            self.node.done(true);
            self.node.close();
        }
    }
}
async fn create(
    factory: &Arc<dyn super::ContextualProviderProductFactory>,
    context: FactoryContext,
    node: &Arc<Node>,
) -> Result<(), ExtensionError> {
    let mut done = Original {
        node: node.clone(),
        done: false,
    };
    let result = super::original::original(
        || factory.create_provider(context),
        |result| result.map(|product| node.install(product)),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    node.done(result.is_err());
    done.done = true;
    result
        .map_err(|()| ExtensionError::ContextCleanupIncomplete)?
        .ok_or(ExtensionError::ContextOwnerClosed)??;
    node.freeze()
}
