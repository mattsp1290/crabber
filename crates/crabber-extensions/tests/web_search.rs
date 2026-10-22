use crate::json_output::JsonOutputExecutor as _;
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::{RunId, RunStatus, SessionId, ToolCallId},
    extension::{
        Extension, ExtensionError, HostServices, Registry, Scope, ToolContext, ToolExecutor,
        WorkspaceContext,
    },
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::web_search::{
    Limits, Options, PERMISSION_SEARCH, Searcher, Source, TOOL_NAME, WebSearch,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn limits() -> Limits {
    Limits {
        max_query_bytes: 100,
        max_results: 2,
        max_title_bytes: 16,
        max_url_bytes: 128,
        max_snippet_bytes: 32,
        max_in_flight: 1,
        max_wait: Duration::from_secs(5),
    }
}
fn record() -> Source {
    Source {
        title: "title".into(),
        url: "https://example.test/".into(),
        snippet: "snippet".into(),
    }
}
fn searcher(records: Vec<Source>) -> Searcher {
    Arc::new(move |_| {
        let records = records.clone();
        Box::pin(async move { Ok(records) })
    })
}
fn configured(searcher: Searcher, limits: Limits) -> WebSearch {
    WebSearch::new(Options {
        searcher_identity: "host-v1".into(),
        searcher,
        limits,
    })
    .unwrap()
}
fn extension(records: Vec<Source>) -> WebSearch {
    configured(searcher(records), limits())
}
fn arguments() -> Value {
    json!({"query":"  synthetic bounded search  "})
}
fn context(cancel: CancellationToken) -> ToolContext {
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
fn agent_config() -> AgentConfig {
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "workspace".into();
    config.directory = "/workspace".into();
    config
}
fn call() -> Vec<StreamDelta> {
    call_with(arguments())
}
fn call_with(arguments: Value) -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: TOOL_NAME.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: arguments.to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}
async fn snapshot_tools(
    store: &MemoryStore,
    session: &SessionId,
) -> Vec<crabber::core::ToolCallRecord> {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 100_000,
                encoded_bytes: 1_000_000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot")
    };
    assert!(!format!("{page:?}").contains("HOST-PRIVATE-CANARY"));
    page.tool_calls
}
fn result_value(call: &crabber::core::ToolCallRecord) -> Value {
    let result = call.result.as_ref().unwrap();
    let [crabber::core::ContentBlock::Text { text }] = result.content.as_slice() else {
        panic!("unexpected result content: {:?}", result.content)
    };
    serde_json::from_str(text).unwrap()
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
async fn executor(extension: WebSearch) -> Arc<dyn ToolExecutor> {
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

fn agent(
    ext: Arc<WebSearch>,
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

async fn snapshot_tool(store: &MemoryStore, session: &SessionId) -> crabber::core::ToolCallRecord {
    let mut calls = snapshot_tools(store, session).await;
    assert_eq!(calls.len(), 1);
    calls.remove(0)
}
struct DropFlag(Arc<AtomicUsize>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct PendingHost {
    searcher: Searcher,
    entered: Arc<tokio::sync::Notify>,
    calls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl PendingHost {
    fn new() -> Self {
        let entered = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let searcher: Searcher = {
            let entered = entered.clone();
            let calls = calls.clone();
            let dropped = dropped.clone();
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                let flag = DropFlag(dropped.clone());
                Box::pin(async move {
                    let _flag = flag;
                    std::future::pending().await
                })
            })
        };
        Self {
            searcher,
            entered,
            calls,
            dropped,
        }
    }
}
fn spawn_call(
    tool: Arc<dyn ToolExecutor>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<Result<Value, ExtensionError>> {
    tokio::spawn(async move {
        tool.execute_json_with_context(context(cancel), arguments())
            .await
    })
}
#[path = "web_search/bounds.rs"]
mod bounds;
#[path = "web_search/contract.rs"]
mod contract;
#[path = "web_search/faults.rs"]
mod faults;
#[path = "web_search/lifecycle.rs"]
mod lifecycle;
#[path = "web_search/routing.rs"]
mod routing;
#[path = "web_search/runtime.rs"]
mod runtime;

#[path = "support/json_output.rs"]
mod json_output;
