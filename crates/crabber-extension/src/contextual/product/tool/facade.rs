use super::cell::{Node, Reference};
use crate::{ExtensionError, ToolContext, ToolExecutor, registry::InvocationSource};
use async_trait::async_trait;
use crabber_core::ToolOutput;
use serde_json::Value;
use std::sync::Weak;
pub(super) struct Template;
#[async_trait]
impl ToolExecutor for Template {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        Err(ExtensionError::ContextOwnerClosed)
    }
}
pub(super) struct Executor {
    pub(super) node: Weak<Node>,
    pub(super) source: InvocationSource,
    pub(super) _reference: Reference,
}
#[async_trait]
impl ToolExecutor for Executor {
    async fn execute(&self, arguments: Value) -> Result<ToolOutput, ExtensionError> {
        self.call(None, arguments).await
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolOutput, ExtensionError> {
        self.call(Some(context), arguments).await
    }
}
impl Executor {
    async fn call(
        &self,
        context: Option<ToolContext>,
        arguments: Value,
    ) -> Result<ToolOutput, ExtensionError> {
        let node = self
            .node
            .upgrade()
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        self.source
            .invoke(move || {
                Box::pin(async move {
                    let original = node.borrow()?;
                    if original.tool().resumable_identity().is_some()
                        || original.tool().resumable().is_some()
                    {
                        return Err(ExtensionError::Tool(
                            "ordinary Box Tool identity changed".into(),
                        ));
                    }
                    match context {
                        Some(context) => {
                            original
                                .tool()
                                .execute_with_context(context, arguments)
                                .await
                        }
                        None => original.tool().execute(arguments).await,
                    }
                })
            })
            .await?
    }
}
