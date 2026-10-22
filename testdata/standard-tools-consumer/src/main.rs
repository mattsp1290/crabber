//! Git-only tools consumer with a custom extension and custom host tool.
mod contexts;
mod matrix;
mod probe;
use async_trait::async_trait;
use crabber::{
    ExtensionError, ToolDefinition,
    core::ToolInfo,
    extension::{Extension, Registrar},
};
use serde_json::{Value, json};
use std::sync::Arc;
struct Marker;
#[async_trait]
impl crabber::JsonToolExecutor for Marker {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!({"marker":true}))
    }
}
#[async_trait]
impl Extension for Marker {
    fn id(&self) -> &str {
        "consumer/marker"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "consumer-marker-v1".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "consumer_marker".into(),
                description: "Synthetic consumer marker".into(),
                parameters: json!({"type":"object","properties":{},"additionalProperties":false}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Marker),
        }));
        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    probe::run(false, Some(Arc::new(Marker))).await?;
    probe::run(true, None).await?;
    contexts::run().await?;
    matrix::run().await?;
    Ok(())
}
