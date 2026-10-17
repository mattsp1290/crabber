use super::{ContextualToolGuardProduct, facade};
use crate::contextual::{Owner, product::reference};
use crate::registry::{InvocationSource, OriginalRoster};
use crate::{ExtensionError, ToolGuard};
use std::sync::{Arc, Mutex};
pub(super) struct Value(pub(super) Box<dyn ToolGuard>);
pub(super) type Reference = reference::Reference<Value>;
pub(crate) struct Node {
    original: Arc<reference::Node<Value>>,
    identity: Mutex<Option<String>>,
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        Arc::new(Self {
            original: reference::Node::new(owner),
            identity: Mutex::new(None),
        })
    }
    pub(in crate::contextual) fn install(self: &Arc<Self>, product: ContextualToolGuardProduct) {
        self.original.install(Value(product.body));
    }
    pub(in crate::contextual) fn done(&self, failed: bool) {
        self.original.done(failed);
    }
    pub(in crate::contextual) fn close(&self) {
        self.original.close();
    }
    pub(crate) fn bind(&self, roster: OriginalRoster) -> Result<(), ExtensionError> {
        self.original.bind(roster)
    }
    pub(super) fn borrow(self: &Arc<Self>) -> Result<reference::Borrow<Value>, ExtensionError> {
        self.original.borrow()
    }
    pub(in crate::contextual) fn freeze(self: &Arc<Self>) -> Result<(), ExtensionError> {
        let original = self.borrow()?;
        // No user method runs while a node/registrar/mount lock is held.
        let captured = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let id = original.value().0.id();
            if id.is_empty() || id.len() > 128 || id.chars().any(char::is_control) {
                return Err(ExtensionError::RegistrationDescriptorUnavailable);
            }
            Ok(id.to_owned())
        }));
        drop(original);
        let identity = match captured {
            Ok(Ok(identity)) => identity,
            Ok(Err(error)) => {
                self.done(true);
                self.close();
                return Err(error);
            }
            Err(payload) => {
                crate::dispatch::discard_panic_payload(payload);
                self.done(true);
                self.close();
                return Err(ExtensionError::RegistrationDescriptorUnavailable);
            }
        };
        *self.identity.lock().unwrap() = Some(identity);
        self.original.captured()
    }
    pub(crate) fn template(&self) -> Result<Arc<dyn ToolGuard>, ExtensionError> {
        self.original.check_captured()?;
        Ok(Arc::new(facade::Template(
            self.identity.lock().unwrap().clone().unwrap(),
        )))
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        source: InvocationSource,
    ) -> Result<Arc<dyn ToolGuard>, ExtensionError> {
        Ok(Arc::new(facade::Guard {
            node: Arc::downgrade(self),
            source,
            identity: self.identity.lock().unwrap().clone().unwrap(),
            _reference: self.original.reference()?,
        }))
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        self.original.join().await
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.original.disposed().await
    }
}
