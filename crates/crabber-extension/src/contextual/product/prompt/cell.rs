use super::{Body, ContextualPromptProduct, facade};
use crate::contextual::{Owner, product::reference};
use crate::prompt_contribution::PromptContributorIdentity;
use crate::registry::OriginalRoster;
use crate::{ExtensionError, MountedPromptContributor};
use std::sync::{Arc, Mutex};
pub(super) struct Value(pub(super) ContextualPromptProduct);
#[derive(Clone)]
struct Metadata {
    name: String,
    order: i32,
    identity: PromptContributorIdentity,
}
pub(super) type Reference = reference::Reference<Value>;
pub(crate) struct Node {
    original: Arc<reference::Node<Value>>,
    metadata: Mutex<Option<Metadata>>,
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        Arc::new(Self {
            original: reference::Node::new(owner),
            metadata: Mutex::new(None),
        })
    }
    pub(in crate::contextual) fn install(self: &Arc<Self>, product: ContextualPromptProduct) {
        self.original.install(Value(product));
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
        let product = &original.value().0;
        if product.name.is_empty()
            || product.name.len() > crate::MAX_PROMPT_CONTRIBUTOR_NAME_BYTES
            || product.name.chars().any(char::is_control)
        {
            drop(original);
            self.done(true);
            self.close();
            return Err(ExtensionError::Plan(
                "invalid prompt contributor name".into(),
            ));
        }
        let metadata = Metadata {
            name: product.name.clone(),
            order: product.order,
            identity: match &product.body {
                Body::Native(_) => PromptContributorIdentity::Legacy,
                Body::SystemPrompt(descriptor, _) => {
                    PromptContributorIdentity::SystemPrompt(descriptor.clone())
                }
            },
        };
        drop(original);
        *self.metadata.lock().unwrap() = Some(metadata);
        self.original.captured()
    }
    pub(crate) fn template(&self) -> Result<MountedPromptContributor, ExtensionError> {
        self.original.check_captured()?;
        let metadata = self.metadata.lock().unwrap().clone().unwrap();
        Ok(MountedPromptContributor::product_template(
            metadata.name,
            metadata.order,
            metadata.identity,
        ))
    }
    pub(crate) fn project(self: &Arc<Self>) -> Result<MountedPromptContributor, ExtensionError> {
        let metadata = self.metadata.lock().unwrap().clone().unwrap();
        let facade = Arc::new(facade::Prompt {
            node: Arc::downgrade(self),
            _reference: self.original.reference()?,
        });
        Ok(MountedPromptContributor::product(
            metadata.name,
            metadata.order,
            metadata.identity,
            facade,
        ))
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        self.original.join().await
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.original.disposed().await
    }
}
