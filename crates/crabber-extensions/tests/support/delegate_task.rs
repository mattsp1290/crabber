#![allow(dead_code)]
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::{RunId, SessionId, ToolCallId},
    extension::{HostServices, Registry, Scope, ToolContext, ToolExecutor, WorkspaceContext},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotPage, SnapshotRequest, Store},
};
use crabber_extensions::delegate_task::{
    DelegateTask, LIST_TOOL, Limits, START_TOOL, STATUS_TOOL, TOOL_NAME, WAIT_TOOL,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub fn context(cancel: CancellationToken) -> ToolContext {
    ToolContext::new(
        SessionId::from("session"),
        RunId::from("run"),
        ToolCallId::from("call"),
        cancel,
        HostServices::default(),
        WorkspaceContext::from_persisted("workspace", "/workspace"),
        Arc::new(|_| {}),
    )
}

pub fn context_for(session: SessionId, workspace: &str) -> ToolContext {
    ToolContext::new(
        session,
        RunId::from("run"),
        ToolCallId::from("call"),
        CancellationToken::new(),
        HostServices::default(),
        WorkspaceContext::from_persisted(workspace, "/workspace"),
        Arc::new(|_| {}),
    )
}

pub fn agent_config() -> AgentConfig {
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "workspace".into();
    config.directory = "/workspace".into();
    config
}

pub fn call_with(arguments: Value) -> Vec<StreamDelta> {
    tool_call(TOOL_NAME, arguments)
}
pub fn tool_call(name: &str, arguments: Value) -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: arguments.to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

pub async fn snapshot_page(store: &MemoryStore, session: &SessionId) -> SnapshotPage {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 16 * 1024 * 1024,
                encoded_bytes: 32 * 1024 * 1024,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot")
    };
    assert!(!format!("{page:?}").contains("HOST-PRIVATE-CANARY"));
    assert!(page.continuation.is_none(), "test snapshot was truncated");
    page
}
pub async fn snapshot_tools(
    store: &MemoryStore,
    session: &SessionId,
) -> Vec<crabber::core::ToolCallRecord> {
    snapshot_page(store, session).await.tool_calls
}

pub fn result_value(call: &crabber::core::ToolCallRecord) -> Value {
    let result = call.result.as_ref().unwrap();
    let [crabber::core::ContentBlock::Text { text }] = result.content.as_slice() else {
        panic!("unexpected result content: {:?}", result.content)
    };
    serde_json::from_str(text).unwrap()
}

pub fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}

pub async fn executor(extension: DelegateTask) -> Arc<dyn ToolExecutor> {
    let registry = Registry::new();
    let handle = registry
        .mount(Arc::new(extension), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let executor = plan.tools[0].executor.clone();
    drop(plan);
    handle.close().await.unwrap();
    executor
}

pub fn agent(
    ext: Arc<DelegateTask>,
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    decision: PermissionDecision,
) -> Agent {
    Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(provider)
        .config(agent_config())
        .policy(Arc::new(StaticPolicy::new(decision)))
        .shared_extension(ext, Scope::Global)
        .build()
        .unwrap()
}

pub async fn mounted(
    ext: Arc<DelegateTask>,
) -> (
    Registry,
    crabber::extension::MountHandle,
    std::collections::HashMap<String, Arc<dyn ToolExecutor>>,
) {
    let registry = Registry::new();
    let handle = registry.mount(ext, Scope::Global).await.unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let tools = plan
        .tools
        .iter()
        .map(|t| (t.info.name.clone(), t.executor.clone()))
        .collect();
    drop(plan);
    (registry, handle, tools)
}
pub fn start_call(arguments: Value) -> Vec<StreamDelta> {
    tool_call(START_TOOL, arguments)
}
pub fn status_call(id: &str) -> Vec<StreamDelta> {
    tool_call(STATUS_TOOL, json!({"task_id":id}))
}
pub fn list_call() -> Vec<StreamDelta> {
    tool_call(LIST_TOOL, json!({}))
}
pub fn wait_call(ids: &[String], timeout: Option<u64>) -> Vec<StreamDelta> {
    let mut args = json!({"task_ids":ids});
    if let Some(timeout) = timeout {
        args["timeout_seconds"] = timeout.into();
    }
    tool_call(WAIT_TOOL, args)
}
pub fn read_task_id(call: &crabber::core::ToolCallRecord) -> String {
    result_value(call)["task_id"].as_str().unwrap().into()
}

pub async fn snapshot_tool(
    store: &MemoryStore,
    session: &SessionId,
) -> crabber::core::ToolCallRecord {
    let mut records = snapshot_tools(store, session).await;
    assert_eq!(records.len(), 1);
    records.remove(0)
}

pub struct DropFlag(pub Arc<AtomicUsize>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
pub const DRIFTS: [fn(&mut Limits); 10] = [
    |l| l.max_task_bytes += 1,
    |l| l.max_profile_bytes += 1,
    |l| l.max_result_bytes += 1,
    |l| l.max_in_flight += 1,
    |l| l.max_wait += Duration::from_secs(1),
    |l| l.shutdown_grace += Duration::from_secs(1),
    |l| l.max_tracked += 1,
    |l| l.max_background_duration += Duration::from_secs(1),
    |l| l.max_wait_ids += 1,
    |l| l.max_wait_reply_bytes += 1,
];
