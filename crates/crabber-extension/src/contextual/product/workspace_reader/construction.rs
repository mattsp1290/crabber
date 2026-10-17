use super::{ContextualWorkspaceReaderResolverProductFactory, Node};
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
    factory: &Arc<dyn ContextualWorkspaceReaderResolverProductFactory>,
    context: FactoryContext,
    node: &Arc<Node>,
) -> Result<(), ExtensionError> {
    let mut guard = Original {
        node: node.clone(),
        done: false,
    };
    let mut future = factory.create_workspace_reader_resolver(context);
    let result = future.as_mut().await;
    let result = match result {
        Ok(product) => {
            node.install(product);
            Ok(())
        }
        Err(error) => Err(error),
    };
    let disposed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)));
    node.done(disposed.is_err());
    guard.done = true;
    if let Err(payload) = disposed {
        crate::dispatch::discard_panic_payload(payload);
        return Err(ExtensionError::ContextCleanupIncomplete);
    }
    result?;
    node.freeze()
}
