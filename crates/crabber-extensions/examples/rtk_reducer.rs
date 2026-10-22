//! Credential-free host-provisioning example. Verify the release archive before
//! computing RTK_EXAMPLE_SHA256 from its extracted immutable executable.
use crabber::JsonToolExecutor;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision,
    Selection as ModelSelection, StaticPolicy, StreamDelta,
    core::{SessionId, ToolCallId, ToolInfo},
    extension::{Scope, ToolDefinition},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::{
    rtk_reducer::*,
    tool_result_redactor::{
        Limits as RedactorLimits, Options as RedactorOptions, ToolResultRedactor,
    },
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
struct FixtureTool(String);
#[async_trait::async_trait]
impl JsonToolExecutor for FixtureTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!(self.0))
    }
}
async fn persisted(
    store: &MemoryStore,
    session: SessionId,
) -> Result<usize, Box<dyn std::error::Error>> {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session,
            limits: SnapshotLimits {
                messages: 20,
                tool_calls: 10,
                parts: 30,
                text_bytes: 128 * 1024,
                encoded_bytes: 256 * 1024,
            },
            continuation: None,
        })
        .await?
    else {
        return Err("snapshot unavailable".into());
    };
    let [crabber::core::ContentBlock::Text { text }] = page.tool_calls[0]
        .result
        .as_ref()
        .ok_or("missing result")?
        .content
        .as_slice()
    else {
        return Err("unexpected result shape".into());
    };
    Ok(text.len())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (Some(executable), Ok(digest)) = (
        std::env::var_os("RTK_EXAMPLE_EXECUTABLE"),
        std::env::var("RTK_EXAMPLE_SHA256"),
    ) else {
        eprintln!(
            "Set RTK_EXAMPLE_EXECUTABLE=/absolute/path/to/rtk and RTK_EXAMPLE_SHA256=<extracted binary digest> after verifying the v0.51.0 archive checksum."
        );
        return Ok(());
    };
    let root = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    let reducer = Arc::new(RtkReducer::new(Options {
        order: DEFAULT_ORDER,
        executable_path: PathBuf::from(executable),
        executable_sha256: digest,
        temp_root: root.path().into(),
        bindings: vec![Binding {
            tool_name: "fixture_log".into(),
            match_input: None,
            selection: Selection::Text(Filter::Log),
        }],
        limits: Limits {
            max_bindings: 8,
            max_fields_per_binding: 8,
            max_input_matches: 8,
            max_config_bytes: 16 * 1024,
            max_input_bytes: 4 * 1024,
            max_field_bytes: 1024 * 1024,
            max_concurrent: 2,
            max_stdout_bytes: 256 * 1024,
            timeout: Duration::from_secs(10),
            kill_wait: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(30),
        },
    })?);
    let redactor = ToolResultRedactor::new(RedactorOptions {
        order: 1_000_000,
        excluded_tools: vec![],
        additional_patterns: vec![],
        limits: RedactorLimits {
            max_field_bytes: 1024 * 1024,
            max_total_bytes: 2 * 1024 * 1024,
            max_depth: 16,
            max_nodes: 100,
            max_matches_per_field: 64,
            max_patterns: 4,
            max_pattern_bytes: 256,
            max_in_flight: 4,
        },
    })?;
    let log =
        "INFO request completed successfully\n".repeat(199) + "ERROR_SENTINEL synthetic failure\n";
    let original = serde_json::to_string(&log)?.len();
    let id = ToolCallId::new();
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: "fixture_log".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: id },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]));
    let store = Arc::new(MemoryStore::new());
    let mut config = AgentConfig::new(ModelSelection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "fixture".into();
    config.directory = root.path().to_string_lossy().into_owned();
    let agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(provider)
        .config(config)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "fixture_log".into(),
                description: "Synthetic 200-line log".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(FixtureTool(log)),
        }))
        .shared_extension(reducer.clone(), Scope::Global)
        .shared_extension(Arc::new(redactor), Scope::Global)
        .build()?;
    let run = agent.prompt(None, "Reduce the fixture log").await?;
    let session = run.session_id().clone();
    run.done().await?;
    let reduced = persisted(&store, session).await?;
    agent.close_extensions().await?;
    println!(
        "Encoded bytes: {original} -> {reduced}; {:?}",
        reducer.stats()
    );
    Ok(())
}
