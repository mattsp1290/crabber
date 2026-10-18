//! Shared registration supplies a real capability to an executed tool.
use super::{Error, call, done, selection};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, JsonToolExecutor, PermissionDecision, StaticPolicy,
    ToolDefinition,
    core::{InputFingerprint, RunStatus, ToolInfo},
    extension::{
        ComponentIdentity, Extension, HostServices, Registrar, Scope, ToolContext, WorkspaceFs,
    },
    session::Store,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Reader(Arc<AtomicUsize>);
#[async_trait]
impl WorkspaceFs for Reader {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        if path != "fixture.txt" {
            return Err("unexpected relative path".into());
        }
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(b"shared capability result".to_vec())
    }
}
struct Tool;
#[async_trait]
impl JsonToolExecutor for Tool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Rejected(
            "actual invocation context required",
        ))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let service = context
            .host
            .workspace_fs
            .as_ref()
            .ok_or(ExtensionError::Rejected("missing shared service"))?;
        let result = service
            .read("fixture.txt")
            .await
            .map_err(|_| ExtensionError::Rejected("shared service failed"))?;
        assert_eq!(result, b"shared capability result");
        Ok(json!({"shared":"used"}))
    }
}
struct Shared(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Shared {
    fn id(&self) -> &'static str {
        "probe-shared-registration"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed-shared-probe.v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "shared_probe".into(),
                description: "Read an explicit host service".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Tool),
        }));
        Ok(())
    }
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<usize, Error> {
    let reads = Arc::new(AtomicUsize::new(0));
    let installs = Arc::new(AtomicUsize::new(0));
    let provider = crabber::FakeProvider::scripted(vec![call("shared_probe"), done()]);
    let host = Agent::builder()
        .behavior_version("shared-services-probe.v1")
        .store(store)
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(Arc::new(Shared(installs.clone())), Scope::Global)
        .shared_host_services(
            ComponentIdentity {
                id: "probe-services".into(),
                version: "1".into(),
            },
            InputFingerprint::new("e".repeat(64))?,
            HostServices {
                workspace_fs: Some(Arc::new(Reader(reads.clone()))),
                subprocess: None,
            },
        )
        .build()?;
    assert_eq!(
        host.prompt(None, "use explicit shared service")
            .await?
            .done()
            .await?
            .status,
        RunStatus::Completed
    );
    host.close_extensions().await?;
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(installs.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    Ok(1)
}
