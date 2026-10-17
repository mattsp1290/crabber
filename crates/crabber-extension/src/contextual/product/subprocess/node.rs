use super::{ContextualSubprocessProduct, facade};
use crate::contextual::{Owner, product::reference};
use crate::registry::{InvocationSource, OriginalRoster};
use crate::{ExtensionError, Subprocess};
use std::sync::Arc;
pub(super) struct Value(pub(super) Box<dyn Subprocess>);
pub(super) type Borrow = reference::Borrow<Value>;
pub(super) type Reference = reference::Reference<Value>;
pub(crate) struct Node {
    original: Arc<reference::Node<Value>>,
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        Arc::new(Self {
            original: reference::Node::new(owner),
        })
    }
    pub(super) fn install(self: &Arc<Self>, product: ContextualSubprocessProduct) {
        self.original.install(Value(product.body));
    }
    pub(super) fn done(&self, failed: bool) {
        self.original.done(failed);
    }
    pub(in crate::contextual) fn close(&self) {
        self.original.close();
    }
    pub(crate) fn bind(&self, roster: OriginalRoster) -> Result<(), ExtensionError> {
        self.original.bind(roster)
    }
    pub(super) fn borrow(self: &Arc<Self>) -> Result<Borrow, ExtensionError> {
        self.original.borrow()
    }
    pub(super) fn freeze(&self) -> Result<(), ExtensionError> {
        self.original.captured()
    }
    pub(crate) fn template(&self) -> Result<Arc<dyn Subprocess>, ExtensionError> {
        self.original.check_captured()?;
        Ok(Arc::new(facade::Template))
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        source: InvocationSource,
    ) -> Result<Arc<dyn Subprocess>, ExtensionError> {
        let reference = self.original.reference()?;
        Ok(Arc::new(facade::Process {
            node: Arc::downgrade(self),
            source,
            _reference: reference,
        }))
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        self.original.join().await
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.original.disposed().await
    }
}
