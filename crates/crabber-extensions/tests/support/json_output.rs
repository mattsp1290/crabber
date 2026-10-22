#![allow(dead_code)]
use crabber::{
    core::ToolOutput,
    extension::{ExtensionError, ToolContext, ToolExecutor},
};
use serde_json::Value;
#[async_trait::async_trait]
pub trait JsonOutputExecutor: ToolExecutor {
    async fn execute_json_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        self.execute_with_context(context, arguments)
            .await
            .and_then(json)
    }
}
impl<T: ToolExecutor + ?Sized> JsonOutputExecutor for T {}
fn json(output: ToolOutput) -> Result<Value, ExtensionError> {
    output
        .json_value()
        .map_err(|_| ExtensionError::Plan("expected JSON tool output".into()))
}
