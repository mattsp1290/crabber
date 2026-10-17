//! Protect a Ready root before disposing its actual original create future.
use super::{Cell, ContextualProductFactory};
use crate::{ExtensionError, FactoryContext};
use std::sync::Arc;
struct Construction {
    cell: Arc<Cell>,
    finished: bool,
}
impl Drop for Construction {
    fn drop(&mut self) {
        if !self.finished {
            self.cell.constructor_done(true);
        }
    }
}
pub(in crate::contextual) async fn create(
    factory: &Arc<dyn ContextualProductFactory>,
    context: FactoryContext,
    cell: &Arc<Cell>,
) -> Result<(), ExtensionError> {
    let mut construction = Construction {
        cell: cell.clone(),
        finished: false,
    };
    let observed = super::provider::original::original(
        || factory.create_root(context),
        |result| result.map(|root| cell.install(root)),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    construction.finished = true;
    cell.constructor_done(!matches!(observed, Ok(Some(_))));
    match observed {
        Ok(Some(result)) => result,
        Ok(None) | Err(()) => Err(ExtensionError::ContextCleanupIncomplete),
    }
}

pub(in crate::contextual) async fn create_member(
    slot: &Arc<crate::contextual::Slot>,
    index: usize,
    factory: &Arc<dyn ContextualProductFactory>,
    context: FactoryContext,
) -> Result<(), ExtensionError> {
    let root = Cell::new(slot.owner.clone());
    slot.state
        .lock()
        .unwrap()
        .producer
        .as_ref()
        .expect("root family requires actual host")
        .record_root(index, root.clone());
    create(factory, context, &root).await
}
