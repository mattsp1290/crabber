//! Credential-free public-facade example using synthetic data in a temporary directory.
use async_trait::async_trait;
use crabber::JsonToolExecutor;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition,
    core::{ToolCallId, ToolInfo},
    extension::Scope,
};
use crabber_extensions::{
    tool_result_redactor::{
        Limits as RedactorLimits, Options as RedactorOptions, ToolResultRedactor,
    },
    workspace_instructions::{
        Limits, Options, ReaderRoot, WorkspaceInstructions,
        host::{AllowListResolver, WorkspaceRootReader},
    },
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

struct FixtureTool;
#[async_trait]
impl JsonToolExecutor for FixtureTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!({"output": "Authorization: Bearer synthetic-fixture"}))
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("AGENTS.md"), "Use the fixture tool once.")?;
    let workspace = Arc::new(WorkspaceRootReader::open(temp.path())?);
    let directory = workspace.directory().to_string_lossy().into_owned();
    let resolver = Arc::new(AllowListResolver::new(vec![(
        "fixture".into(),
        directory.clone(),
        workspace,
    )])?);
    let instructions = WorkspaceInstructions::new(Options {
        order: 100,
        file_names: vec!["AGENTS.md".into()],
        reader_root: ReaderRoot::Workspace,
        boundaries: vec![],
        limits: Limits {
            max_file_names: 1,
            max_chain_depth: 1,
            max_file_bytes: 1024,
            max_section_bytes: 4096,
            max_in_flight: 4,
            max_wait: Duration::from_secs(1),
        },
    })?;
    let redactor = ToolResultRedactor::new(RedactorOptions {
        order: 1_000_000,
        excluded_tools: vec![],
        additional_patterns: vec![],
        limits: RedactorLimits {
            max_field_bytes: 4096,
            max_total_bytes: 16384,
            max_depth: 16,
            max_nodes: 100,
            max_matches_per_field: 16,
            max_patterns: 4,
            max_pattern_bytes: 256,
            max_in_flight: 4,
        },
    })?;
    let call = ToolCallId::new();
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "fixture".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]));
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "fixture".into();
    config.directory = directory;
    let agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .memory()
        .provider(provider.clone())
        .config(config)
        .workspace_reader_resolver(resolver)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "fixture".into(),
                description: "Synthetic output".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(FixtureTool),
        }))
        .shared_extension(Arc::new(instructions), Scope::Global)
        .shared_extension(Arc::new(redactor), Scope::Global)
        .build()?;
    agent
        .prompt(None, "Exercise the fixture")
        .await?
        .done()
        .await?;
    let requests = provider.requests();
    assert!(
        requests[0]
            .system
            .as_deref()
            .unwrap()
            .contains("Use the fixture tool once.")
    );
    let next = format!("{:?}", requests[1]);
    assert!(next.contains("[REDACTED]") && !next.contains("synthetic-fixture"));
    agent.close_extensions().await?;
    println!(
        "Workspace instructions reached the model; protected tool output reached the next request."
    );
    Ok(())
}
