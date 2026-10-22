//! Shared integration harness for the standard tools; dev-only.
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    ToolDefinition,
    core::{SessionId, ToolCallId, ToolCallRecord},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
/// Build a direct executor context whose owner lives through cleanup.
pub fn context(
    root: &crabber_tools_core::WorkspaceRoot,
    cancel: CancellationToken,
) -> OwnedContext {
    use crabber::{
        core::{RunId, ToolCallId},
        extension::{HostServices, ToolContext, WorkspaceContext},
    };
    let owner = crabber::extension::ToolResourceOwner::new();
    let context = ToolContext::new(
        SessionId::new(),
        RunId::new(),
        ToolCallId::new(),
        cancel,
        HostServices::default(),
        WorkspaceContext::from_persisted("test", root.path().to_str().unwrap()),
        Arc::new(|_| {}),
    )
    .with_resources(owner.resources());
    OwnedContext { context, owner }
}
/// Finite bounds for scripted tool executions.
pub fn limits() -> crabber_tools_core::Limits {
    crabber_tools_core::Limits {
        max_in_flight: 1,
        max_blocking_wait: Duration::from_secs(2),
    }
}
/// A scripted provider turn containing one tool call.
pub fn turn(name: &str, args: Value) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: args.to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}
/// A scripted provider completion.
pub fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
/// Agent configuration bound to the fixture directory.
pub fn config(directory: &std::path::Path) -> AgentConfig {
    let mut c = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    c.directory = directory.to_string_lossy().into_owned();
    c.workspace_id = "probe".into();
    c
}
/// Read persisted tool calls from the public snapshot API.
pub async fn calls(store: &MemoryStore, id: &SessionId) -> Vec<ToolCallRecord> {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: id.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 8_000_000,
                encoded_bytes: 16_000_000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot page")
    };
    page.tool_calls
}
/// Decode a persisted textual tool envelope.
pub fn value(call: &ToolCallRecord) -> Value {
    let result = call.result.as_ref().unwrap();
    match &result.content[0] {
        crabber::core::ContentBlock::Text { text } => serde_json::from_str(text).unwrap(),
        other => panic!("unexpected content {other:?}"),
    }
}
/// Execute one tool through an agent and return its persisted envelope.
pub async fn execute(root: &std::path::Path, tool: Arc<ToolDefinition>, args: Value) -> Value {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        turn(&tool.info.name, args),
        done(),
    ]));
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(provider)
        .config(config(root))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool)
        .build()
        .unwrap();
    let run = agent.prompt(None, "test").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let calls = calls(&store, &session).await;
    assert_eq!(calls.len(), 1);
    agent.close_extensions().await.unwrap();
    value(&calls[0])
}
/// Check whether a fixture process is still running.
pub fn process_running(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        && stat
            .rsplit_once(") ")
            .is_some_and(|(_, s)| s.starts_with('Z'))
    {
        return false;
    }
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}
/// Wait for a fixture process to exit.
pub async fn assert_gone(pid: u32) {
    for _ in 0..200 {
        if !process_running(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!process_running(pid), "process {pid} survived");
}
/// Wait for a process fixture to publish its readiness file.
pub async fn wait_file(path: &std::path::Path) {
    for _ in 0..200 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("missing pid file");
}
/// Read the process identifier published by a fixture.
pub fn pid(path: &std::path::Path) -> u32 {
    std::fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}
/// Write an executable fixture script.
pub fn script(path: &std::path::Path, text: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}
/// A ripgrep JSON match record for parser fixtures.
pub fn match_line() -> String {
    json!({"type":"match","data":{"path":{"text":"fixture"},"line_number":1,"lines":{"text":"hello\n"},"submatches":[{"match":{"text":"hello"},"start":0,"end":5}]}}).to_string()
}

/// Decode the structured JSON tool output.
pub fn value_of(output: &crabber::core::ToolOutput) -> Value {
    output.json_value().unwrap()
}

/// Construct a fresh admission identity for a scripted prompt.
pub fn admission_options(agent: &Agent, text: &str) -> crabber::core::AdmissionOptions {
    crabber::core::AdmissionOptions {
        key: crabber::core::AdmissionKey::new(crabber::core::ControlId::new().0).unwrap(),
        fingerprint: crabber::core::InputFingerprint::new(crabber_tools_core::config_hash(&text))
            .unwrap(),
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    }
}
/// Build an execution context for a fixture workspace.
pub fn execution(
    session: SessionId,
    workspace: &str,
    directory: &std::path::Path,
) -> crabber::ExecutionContext {
    let directory = if directory.as_os_str().is_empty() {
        std::path::PathBuf::new()
    } else {
        directory.canonicalize().expect("fixture workspace exists")
    };
    crabber::ExecutionContext::new(
        crabber::SessionContext::new(session, workspace, directory.to_str().unwrap()),
        Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
    )
}
/// Start one fresh keyed prompt in a fixture execution context.
pub async fn admit(
    agent: &Agent,
    execution: &crabber::ExecutionContext,
    text: &str,
) -> crabber::RunHandle {
    match agent
        .prompt_keyed_in(execution, text, admission_options(agent, text), None)
        .await
        .unwrap()
    {
        crabber::Admission::Started { handle, .. } => handle,
        crabber::Admission::Replayed(_) => panic!("fresh admission replayed"),
    }
}

/// Assert that an interrupted native tool leaves durable uncertainty.
pub async fn assert_stopped_uncertain(run: crabber::RunHandle, store: &MemoryStore) {
    let id = run.run_id().clone();
    let crabber::RuntimeError::PhaseOutcomeUnknown { phase, reason, .. } =
        run.done().await.unwrap_err()
    else {
        panic!("interrupted native phase must remain uncertain")
    };
    assert_eq!(reason, crabber::session::PhaseError::Uncertain);
    let saved = store.tool_phase(&id, &phase.id).await.unwrap().phase;
    assert_eq!(saved.state, crabber::session::PhaseState::Uncertain);
}

/// Direct-call fixture keeps a close capability for the entire invocation.
pub struct OwnedContext {
    context: crabber::extension::ToolContext,
    owner: crabber::extension::ToolResourceOwner,
}
impl std::ops::Deref for OwnedContext {
    type Target = crabber::extension::ToolContext;
    fn deref(&self) -> &Self::Target {
        &self.context
    }
}
impl OwnedContext {
    /// Explicit lifecycle tests retain and drain the supplied owner themselves.
    pub fn with_resources(
        self,
        resources: crabber::extension::ToolResources,
    ) -> crabber::extension::ToolContext {
        self.context.clone().with_resources(resources)
    }
}
impl Drop for OwnedContext {
    fn drop(&mut self) {
        self.owner.close();
    }
}
/// Ordinary direct-executor tests must observe the actual cleanup join.
#[async_trait::async_trait]
pub trait ExecuteOwned: crabber::ToolExecutor {
    /// Execute with an owned context and assert cleanup drains completely.
    async fn execute_owned_context(
        &self,
        fixture: OwnedContext,
        args: Value,
    ) -> Result<crabber::core::ToolOutput, crabber::ExtensionError> {
        let result = self
            .execute_with_context(fixture.context.clone(), args)
            .await;
        let drain = fixture.owner.drain(Duration::from_secs(2)).await;
        assert!(drain.sealed);
        assert_eq!(drain.pending, 0, "direct executor left pending resources");
        assert!(
            drain.failures.is_empty(),
            "direct executor cleanup failed: {:?}",
            drain.failures
        );
        result
    }
}
impl<T: crabber::ToolExecutor + ?Sized> ExecuteOwned for T {}
