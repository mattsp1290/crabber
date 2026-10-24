use super::{failure, input, manager::Manager};
use crabber::JsonToolExecutor;
use crabber::extension::{ExtensionError, ToolContext};
use serde_json::Value;
use std::sync::Arc;
pub(super) struct ExecuteTool(pub(super) Arc<Manager>);
pub(super) struct ClearTool(pub(super) Arc<Manager>);
#[async_trait::async_trait]
impl JsonToolExecutor for ExecuteTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(failure("context"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        let input = input::execute(&arguments, &self.0.configuration.limits)?;
        serde_json::to_value(self.0.execute(context, input.code, input.timeout).await?)
            .map_err(|_| failure("runner-failed"))
    }
}
#[async_trait::async_trait]
impl JsonToolExecutor for ClearTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(failure("context"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        input::clear(&arguments)?;
        serde_json::to_value(self.0.clear(context).await?).map_err(|_| failure("runner-failed"))
    }
}
