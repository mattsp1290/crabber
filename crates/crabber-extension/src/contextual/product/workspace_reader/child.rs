//! One returned reader's exact Box and original invocation subset.
use super::ContextualWorkspaceReaderProduct;
use crate::contextual::{Owner, product::reference};
use crate::registry::{Actor, ActorClose, OriginalRoster};
use crate::{ExtensionError, WorkspaceReader};
use futures::future::BoxFuture;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
pub(super) struct Value(pub(super) Box<dyn WorkspaceReader>);
pub(super) type Reference = reference::Reference<Value>;
pub(super) struct Child {
    pub(super) original: Arc<reference::Node<Value>>,
    pub(super) roster: OriginalRoster,
    actor: Mutex<Option<Arc<Actor>>>,
    installed: AtomicBool,
}
impl Child {
    pub(super) fn new(owner: Owner) -> Result<Arc<Self>, ExtensionError> {
        let original = reference::Node::new(owner);
        original.captured()?;
        let roster = OriginalRoster::local();
        original.bind(roster.clone())?;
        Ok(Arc::new(Self {
            original,
            roster,
            actor: Mutex::new(None),
            installed: AtomicBool::new(false),
        }))
    }
    pub(super) fn install(
        &self,
        product: ContextualWorkspaceReaderProduct,
    ) -> Result<(), crate::WorkspaceReadError> {
        let context = self.original.with_callback_drop_context();
        self.installed.store(true, Ordering::SeqCst);
        // The returned original Box is retained even when context capture refuses.
        self.original.install(Value(product.body));
        if context {
            Ok(())
        } else {
            self.original.done(true);
            self.close();
            Err(crate::WorkspaceReadError::new(
                crate::WorkspaceReadErrorKind::Denied,
            ))
        }
    }
    pub(super) fn close(&self) {
        self.original.close();
    }
    pub(super) fn registered(&self, actor: Arc<Actor>) {
        *self.actor.lock().unwrap() = Some(actor);
    }
    pub(super) fn cancel_reserved(&self) {
        if self.actor.lock().unwrap().is_none() {
            self.original.done(false);
        }
        self.close();
    }
    pub(super) async fn constructor_join(&self) -> Result<(), ExtensionError> {
        let actor = self.actor.lock().unwrap().clone();
        if let Some(actor) = actor {
            actor.join().await?;
            self.actor.lock().unwrap().take();
        }
        Ok(())
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        if self.installed.load(Ordering::SeqCst) {
            self.original.disposed().await?;
        } else {
            self.original.join().await?;
        }
        self.constructor_join().await
    }
}
impl ActorClose for Child {
    fn close(&self) {
        self.close();
    }
    fn observed(&self, _: bool) {}
    fn released(&self, success: bool) -> BoxFuture<'static, bool> {
        self.original.done(!success);
        Box::pin(async move { success })
    }
}
/// Installed before even `register_actor` can yield. Cancellation seals this exact
/// reserved child; it never closes another reader or retries the constructor.
pub(super) struct Pending(pub(super) Option<Arc<Child>>);
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(child) = &self.0 {
            child.cancel_reserved();
        }
    }
}
