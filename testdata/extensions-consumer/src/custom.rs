use async_trait::async_trait;
use crabber::{
    JsonToolExecutor, PermissionDecision,
    core::ToolInfo,
    extension::{Extension, ExtensionError, Registrar, ToolContext, ToolDefinition},
    runtime::PermissionPolicy,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub const SECRET: &str = "ghp_0123456789abcdef";
pub struct Policy;
impl PermissionPolicy for Policy {
    fn decide(&self, info: &ToolInfo, _: &Value) -> PermissionDecision {
        if info.name == "confirmed_marker" {
            PermissionDecision::Ask
        } else {
            PermissionDecision::Allow
        }
    }
}
pub struct Custom(pub Arc<AtomicUsize>);
struct Marker(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Marker {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Tool("context required".into()))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(
            json!({"directory":context.workspace().directory(),"workspace":context.workspace().workspace_id(),"session":context.session_id,"secret":SECRET}),
        )
    }
}
#[async_trait]
impl Extension for Custom {
    fn id(&self) -> &str {
        "consumer/custom"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "consumer-custom.v1".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        for name in ["consumer_marker", "guarded_marker", "confirmed_marker"] {
            r.tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: name.into(),
                    description: "Synthetic consumer marker".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec!["host.marker".into()],
                },
                executor: Arc::new(Marker(self.0.clone())),
            }));
        }
        Ok(())
    }
}
