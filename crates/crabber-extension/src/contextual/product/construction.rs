//! Protect a Ready root before disposing its actual original create future.
use super::{Cell, ContextualProductFactory};
use crate::{ExtensionError, FactoryContext};
use std::{panic::AssertUnwindSafe, sync::Arc};
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
    let mut original = factory.create_root(context);
    let result = original.as_mut().await;
    let result = match result {
        Ok(root) => {
            cell.install(root);
            Ok(())
        }
        Err(error) => Err(error),
    };
    let disposed = std::panic::catch_unwind(AssertUnwindSafe(|| drop(original)));
    construction.finished = true;
    cell.constructor_done(disposed.is_err());
    if let Err(payload) = disposed {
        crate::dispatch::discard_panic_payload(payload);
        return Err(ExtensionError::ContextCleanupIncomplete);
    }
    result
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
