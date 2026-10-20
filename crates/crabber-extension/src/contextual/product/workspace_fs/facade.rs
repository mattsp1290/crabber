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
                    match super::super::provider::original::original(
                        || original.value().0.read(&path),
                        std::convert::identity,
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await
                    {
                        Ok(Some(result)) => result,
                        // Fail the retained Source, rather than report successful
                        // observation of an original poll/Drop failure.
                        _ => panic!("original workspace filesystem observation failed"),
                    }
                })
            })
            .await
            .map_err(|error| error.to_string())?
    }
}
