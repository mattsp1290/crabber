//! External-consumer probe for native workspace context.
//!
//! Everything here uses the public `crabber` facade only: no private fields,
//! no test-only features and no session-to-workspace side table. The probe
//! learns a session's workspace identity solely from what the runtime hands
//! to its tool and to its `ContextAssemble` handler.

use async_trait::async_trait;
use crabber::{
    AgentBuilder, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection,
    StaticPolicy, StreamDelta, ToolDefinition, ToolExecutor,
    core::{ToolCallId, ToolInfo},
    extension::{
        ContextAssemble, Extension, Point, Registrar, Scope, ToolContext, WorkspaceContext,
    },
    runtime::{InterruptPolicy, PermissionPolicy},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

/// `(workspace ID, directory)`; `None` is the explicit unavailable value.
pub type Identity = (Option<String>, Option<String>);

pub const TOOL: &str = "where_am_i";

/// What the runtime handed to the probe's two extension surfaces.
#[derive(Default)]
pub struct Observations {
    tool: Mutex<Vec<Identity>>,
    assemble: Mutex<Vec<Identity>>,
}
impl Observations {
    /// One entry per tool execution.
    ///
    /// # Panics
    ///
    /// Panics if a recording thread panicked.
    #[must_use]
    pub fn tool(&self) -> Vec<Identity> {
        self.tool.lock().expect("probe poisoned").clone()
    }
    /// One entry per `ContextAssemble` invocation, that is, per model turn.
    ///
    /// # Panics
    ///
    /// Panics if a recording thread panicked.
    #[must_use]
    pub fn assemble(&self) -> Vec<Identity> {
        self.assemble.lock().expect("probe poisoned").clone()
    }
}

struct WhereAmI(Arc<Observations>);
#[async_trait]
impl ToolExecutor for WhereAmI {
    async fn execute(&self, _arguments: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Tool("tool context required".into()))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _arguments: Value,
    ) -> Result<Value, ExtensionError> {
        // Routing data only: an extension must handle absence explicitly and
        // must not fall back to its own defaults or to model arguments.
        let workspace = context.workspace();
        let identity = (
            workspace.workspace_id().map(str::to_owned),
            workspace.directory().map(str::to_owned),
        );
        self.0
            .tool
            .lock()
            .expect("probe poisoned")
            .push(identity.clone());
        Ok(json!({"workspace_id": identity.0, "directory": identity.1}))
    }
}

/// A native extension with one tool and one dynamic prompt contributor.
pub struct ProbeExtension(pub Arc<Observations>);
#[async_trait]
impl Extension for ProbeExtension {
    fn id(&self) -> &'static str {
        "probe/workspace-context"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "probe".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: TOOL.into(),
                description: "Reports the session workspace".into(),
                parameters: json!({"type":"object","properties":{"note":{"type":"string"}}}),
                retry_safe: true,
                required_permissions: Vec::new(),
            },
            executor: Arc::new(WhereAmI(Arc::clone(&self.0))),
        }));
        let observations = Arc::clone(&self.0);
        registrar.on_transform(
            ContextAssemble::ID,
            0,
            "probe-workspace",
            Arc::new(move |mut value| {
                let observations = Arc::clone(&observations);
                Box::pin(async move {
                    let field = |key: &str| value[key].as_str().map(str::to_owned);
                    let identity = (
                        field(WorkspaceContext::WORKSPACE_ID_KEY),
                        field(WorkspaceContext::DIRECTORY_KEY),
                    );
                    if let Some(sections) = value["prompt_sections"].as_array_mut() {
                        sections.push(json!(format!(
                            "Workspace: {}",
                            identity.0.as_deref().unwrap_or("unavailable")
                        )));
                    }
                    observations
                        .assemble
                        .lock()
                        .expect("probe poisoned")
                        .push(identity);
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}

/// Allows every tool but pauses the run before executing it.
pub struct PauseBeforeTools;
impl PermissionPolicy for PauseBeforeTools {
    fn decide(&self, _tool: &ToolInfo, _arguments: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _tool: &ToolInfo, _arguments: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}

/// A model turn that calls the probe tool with arguments naming a workspace
/// the session does not have. The runtime must never surface these values.
#[must_use]
pub fn tool_call() -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: TOOL.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: json!({"note":"hi","workspace_id":"model-ws","directory":"/model/root"})
                .to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

#[must_use]
pub fn text(text: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(text.into()), StreamDelta::Completed]
}

/// Host configuration presenting `workspace_id` and `directory` at admission.
#[must_use]
pub fn config(workspace_id: &str, directory: &str) -> AgentConfig {
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = workspace_id.into();
    config.directory = directory.into();
    config
}

/// A host with the probe extension mounted. The caller selects the store.
#[must_use]
pub fn host(
    observations: &Arc<Observations>,
    scripts: Vec<Vec<StreamDelta>>,
    config: AgentConfig,
    pause: bool,
) -> AgentBuilder {
    let policy: Arc<dyn PermissionPolicy> = if pause {
        Arc::new(PauseBeforeTools)
    } else {
        Arc::new(StaticPolicy::new(PermissionDecision::Allow))
    };
    crabber::Agent::builder()
        .provider(Arc::new(FakeProvider::scripted(scripts)))
        .config(config)
        .policy(policy)
        .extension(
            Arc::new(ProbeExtension(Arc::clone(observations))),
            Scope::Global,
        )
}

/// Builds the expected identity for assertions.
#[must_use]
pub fn identity(workspace_id: Option<&str>, directory: Option<&str>) -> Identity {
    (
        workspace_id.map(str::to_owned),
        directory.map(str::to_owned),
    )
}
