use super::{ContextualToolProduct, facade};
use crate::contextual::{Owner, product::reference};
use crate::registry::{InvocationSource, OriginalRoster};
use crate::{ExtensionError, ToolDefinition, ToolExecutor};
use std::sync::{Arc, Mutex};

pub(super) struct Value(pub(super) Box<dyn ToolExecutor>);
pub(super) struct Borrow(reference::Borrow<Value>);
impl Borrow {
    pub(super) fn tool(&self) -> &dyn ToolExecutor {
        self.0.value().0.as_ref()
    }
}
pub(super) type Reference = reference::Reference<Value>;
pub(crate) struct Node {
    original: Arc<reference::Node<Value>>,
    info: Mutex<Option<crabber_core::ToolInfo>>,
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        Arc::new(Self {
            original: reference::Node::new(owner),
            info: Mutex::new(None),
        })
    }
    pub(in crate::contextual) fn install(self: &Arc<Self>, product: ContextualToolProduct) {
        let mut info = self.info.lock().unwrap();
        assert!(info.is_none());
        *info = Some(product.info);
        drop(info);
        self.original.install(Value(product.executor));
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
    pub(super) fn borrow(self: &Arc<Self>) -> Result<Borrow, ExtensionError> {
        self.original.borrow().map(Borrow)
    }
    pub(in crate::contextual) fn freeze(self: &Arc<Self>) -> Result<(), ExtensionError> {
        let value = self.borrow()?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if value.tool().resumable_identity().is_some() || value.tool().resumable().is_some() {
                Err(ExtensionError::Tool(
                    "Box product requires an ordinary Tool".into(),
                ))
            } else {
                Ok(())
            }
        }));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                crate::dispatch::discard_panic_payload(payload);
                Err(ExtensionError::RegistrationDescriptorUnavailable)
            }
        };
        drop(value);
        result?;
        self.original.captured()
    }
    pub(crate) fn template(self: &Arc<Self>) -> Result<Arc<ToolDefinition>, ExtensionError> {
        self.original.check_captured()?;
        Ok(Arc::new(ToolDefinition {
            info: self.info.lock().unwrap().clone().unwrap(),
            executor: Arc::new(facade::Template),
        }))
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        source: InvocationSource,
    ) -> Result<Arc<ToolDefinition>, ExtensionError> {
        let reference = self.original.reference()?;
        Ok(Arc::new(ToolDefinition {
            info: self.info.lock().unwrap().clone().unwrap(),
            executor: Arc::new(facade::Executor {
                node: Arc::downgrade(self),
                source,
                _reference: reference,
            }),
        }))
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        self.original.join().await
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.original.disposed().await
    }
}
