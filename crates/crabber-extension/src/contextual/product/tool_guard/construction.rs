use super::{ContextualToolGuardProductFactory, Node};
use crate::{ExtensionError, FactoryContext};
use std::sync::Arc;
struct Original {
    node: Arc<Node>,
    done: bool,
}
impl Drop for Original {
    fn drop(&mut self) {
        if !self.done {
            self.node.done(true);
        }
    }
}
pub(in crate::contextual) async fn create(
    factory: &Arc<dyn ContextualToolGuardProductFactory>,
    context: FactoryContext,
    node: &Arc<Node>,
) -> Result<(), ExtensionError> {
    let mut guard = Original {
        node: node.clone(),
        done: false,
    };
    let observed = super::super::provider::original::original(
        || factory.create_tool_guard(context),
        |result| result.map(|product| node.install(product)),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    node.done(!matches!(observed, Ok(Some(_))));
    guard.done = true;
    let Ok(Some(result)) = observed else {
        return Err(ExtensionError::ContextCleanupIncomplete);
    };
    result?;
    node.freeze()
}
