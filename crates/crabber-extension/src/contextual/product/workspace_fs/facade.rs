use super::node::{Node, Reference};
use crate::{ExtensionError, WorkspaceFs, registry::InvocationSource};
use async_trait::async_trait;
use std::sync::Weak;
pub(super) struct Template;
#[async_trait]
impl WorkspaceFs for Template {
    async fn read(&self, _: &str) -> Result<Vec<u8>, String> {
        Err(ExtensionError::ContextOwnerClosed.to_string())
    }
}
pub(super) struct Filesystem {
    pub(super) node: Weak<Node>,
    pub(super) source: InvocationSource,
    pub(super) _reference: Reference,
}
#[async_trait]
impl WorkspaceFs for Filesystem {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        if crate::registry::current_callback_invocations().is_none() {
            return Err(ExtensionError::ContextOwnerClosed.to_string());
        }
        let node = self
            .node
            .upgrade()
            .ok_or_else(|| ExtensionError::ContextOwnerClosed.to_string())?;
        let path = path.to_owned();
        self.source
            .invoke(move || {
                Box::pin(async move {
                    let original = node.borrow().map_err(|error| error.to_string())?;
                    original.value().0.read(&path).await
                })
            })
            .await
            .map_err(|error| error.to_string())?
    }
}
