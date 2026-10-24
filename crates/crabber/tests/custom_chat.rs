#![cfg(feature = "custom-http")]

use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, ExtensionError, JsonToolExecutor, ModelControls,
    PermissionDecision, ReasoningEffort, RunResult, RuntimeError, Selection, StaticPolicy,
    ToolDefinition,
    core::{
        InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, Role, RunId, RunStatus,
        SessionId, ToolInfo,
        event_payload::{MessageEnded, StreamOutcome},
    },
    providers::{HttpAdapter, HttpResolver, Protocol, ProviderErrorKind, VersionedResolver},
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{task::JoinHandle, time::timeout};

#[path = "http_loopback/support.rs"]
mod loopback;
use loopback::{Captured, LIMIT, server};

const UPSTREAM_MESSAGE: &str = "SENTINEL_UPSTREAM_MESSAGE_7F4C";
const PROVIDER_CODE: &str = "SENTINEL_PROVIDER_CODE_8A2D";
const NESTED_METADATA: &str = "SENTINEL_NESTED_METADATA_1B9E";
const UNRELATED_TOP_LEVEL: &str = "SENTINEL_UNRELATED_TOP_LEVEL_6D3A";
const SENTINELS: [&str; 4] = [
    UPSTREAM_MESSAGE,
    PROVIDER_CODE,
    NESTED_METADATA,
    UNRELATED_TOP_LEVEL,
];

struct CountingTool(Arc<AtomicUsize>);

#[async_trait]
impl JsonToolExecutor for CountingTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Value::Null)
    }
}

fn partial_tool_then_error() -> String {
    let start = json!({
        "choices": [{
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": "partial-call",
                    "function": {"name": "count", "arguments": "{"}
                }]
            }
        }]
    });
    format!("data: {start}\n\n{}data: [DONE]\n\n", retryable_response())
}

fn retryable_response() -> String {
    let error = json!({
        "error": {
            "code": 503,
            "message": UPSTREAM_MESSAGE,
            "metadata": {
                "error_type": "provider_overloaded",
                "nested": NESTED_METADATA
            }
        },
        "provider_code": PROVIDER_CODE,
        "unrelated": UNRELATED_TOP_LEVEL
    });
    format!("data: {error}\n\n")
}

fn clean_response(text: &str) -> String {
    let delta = json!({"choices": [{"delta": {"content": text}}]});
    format!("data: {delta}\n\ndata: [DONE]\n\n")
}

fn resolver(endpoint: &str) -> VersionedResolver {
    let adapter = HttpAdapter::custom("fixture", endpoint, Protocol::ChatCompletions)
        .with_api_key("fixture-key");
    VersionedResolver::new(
        Arc::new(HttpResolver::new().with_adapter(adapter)),
        vec![
            ProviderBindingDescriptor::new(
                "fixture",
                "model",
                "loopback-chat",
                "1",
                InputFingerprint::new("c".repeat(64)).unwrap(),
                ProviderBindingLimits::default(),
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn selection() -> Selection {
    Selection {
        provider_id: "fixture".into(),
        model_id: "model".into(),
    }
}

struct ChatJourney {
    agent: Agent,
    store: Arc<MemoryStore>,
    executions: Option<Arc<AtomicUsize>>,
    server: JoinHandle<Vec<Captured>>,
}

struct JourneyResult {
    result: Result<RunResult, RuntimeError>,
    records: Vec<Arc<EventRecord>>,
    store: Arc<MemoryStore>,
    session_id: SessionId,
    run_id: RunId,
    executions: Option<Arc<AtomicUsize>>,
    captured: Vec<Captured>,
}

impl ChatJourney {
    async fn new(
        responses: Vec<String>,
        behavior_version: &str,
        controls: ModelControls,
        counting_tool: bool,
    ) -> Self {
        let (endpoint, server) = server(responses, "/chat/completions").await;
        let store = Arc::new(MemoryStore::new());
        let executions = counting_tool.then(|| Arc::new(AtomicUsize::new(0)));
        let builder = Agent::builder()
            .behavior_version(behavior_version)
            .store(store.clone())
            .provider(Arc::new(resolver(&endpoint)))
            .config(AgentConfig::new(selection()).model_controls(controls));
        let builder = if let Some(executions) = &executions {
            builder
                .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
                .shared_tool(Arc::new(ToolDefinition {
                    info: ToolInfo {
                        name: "count".into(),
                        description: "count executions".into(),
                        parameters: json!({"type": "object"}),
                        retry_safe: false,
                        required_permissions: vec![],
                    },
                    executor: Arc::new(CountingTool(Arc::clone(executions))),
                }))
        } else {
            builder
        };
        Self {
            agent: builder.build().unwrap(),
            store,
            executions,
            server,
        }
    }

    async fn run(self, prompt: &str) -> JourneyResult {
        let mut handle = timeout(LIMIT, self.agent.prompt(None, prompt))
            .await
            .expect("prompt timed out")
            .unwrap();
        let session_id = handle.session_id().clone();
        let run_id = handle.run_id().clone();
        let mut receiver = handle.events();
        let drain = async {
            let mut records = Vec::new();
            while let Some(record) = receiver.recv().await.unwrap() {
                records.push(record);
            }
            assert!(receiver.recv().await.unwrap().is_none());
            records
        };
        let (records, result) = tokio::join!(timeout(LIMIT, drain), timeout(LIMIT, handle.done()));
        let captured = timeout(LIMIT, self.server)
            .await
            .expect("server join timed out")
            .unwrap();
        JourneyResult {
            result: result.expect("run completion timed out"),
            records: records.expect("event drain timed out"),
            store: self.store,
            session_id,
            run_id,
            executions: self.executions,
            captured,
        }
    }
}

fn assert_sentinels_absent(label: &str, artifact: &str) {
    for sentinel in SENTINELS {
        assert!(!artifact.contains(sentinel), "{label} leaked {sentinel}");
    }
}

fn assert_no_tool_effects(records: &[Arc<EventRecord>], executions: &AtomicUsize) {
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(!records.iter().any(|record| matches!(
        record.kind,
        EventKind::ToolCallPending | EventKind::ToolCallSettled
    )));
}

#[tokio::test]
async fn chat_pre_output_error_retries_once_then_commits_one_response() {
    let journey = ChatJourney::new(
        vec![retryable_response(), clean_response("ok-after-retry")],
        "custom-chat-pre-output-retry.v1",
        ModelControls::default(),
        true,
    )
    .await
    .run("retry user message")
    .await;

    assert_eq!(journey.result.unwrap().status, RunStatus::Completed);
    assert_eq!(journey.captured.len(), 2);
    assert_no_tool_effects(&journey.records, journey.executions.as_deref().unwrap());
    assert_eq!(
        journey
            .records
            .iter()
            .filter(|record| record.kind == EventKind::TextDelta)
            .count(),
        1
    );
    assert_eq!(
        journey
            .records
            .iter()
            .filter(|record| record.kind == EventKind::MessageCommitted)
            .count(),
        1
    );
    assert!(!journey.records.iter().any(|record| matches!(
        record.kind,
        EventKind::ToolCallStarted | EventKind::ToolCallArgsDelta
    )));
    let public_events = serde_json::to_string(&journey.records).unwrap();
    assert_eq!(public_events.matches("ok-after-retry").count(), 1);
    assert_sentinels_absent("event", &public_events);

    let messages = journey
        .store
        .list_all_messages(&journey.session_id)
        .await
        .unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .count(),
        1
    );
    let stored = serde_json::to_string(&messages).unwrap();
    assert_eq!(stored.matches("ok-after-retry").count(), 1);
    assert_sentinels_absent("store", &stored);
    let run = journey
        .store
        .get_run(&journey.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert!(run.error.is_none());
}

#[tokio::test]
async fn agent_reasoning_effort_high_reaches_chat_wire_body() {
    let journey = ChatJourney::new(
        vec![clean_response("done")],
        "custom-chat-reasoning-effort.v1",
        ModelControls::default().reasoning_effort(ReasoningEffort::High),
        false,
    )
    .await
    .run("reason carefully")
    .await;

    assert_eq!(journey.result.unwrap().status, RunStatus::Completed);
    assert_eq!(journey.captured.len(), 1);
    assert_eq!(
        journey.captured[0].1.get("reasoning_effort"),
        Some(&json!("high"))
    );
}

#[tokio::test]
async fn chat_stream_error_after_partial_tool_call_fails_without_dispatch() {
    let journey = ChatJourney::new(
        vec![partial_tool_then_error()],
        "custom-chat-stream-error.v1",
        ModelControls::default(),
        true,
    )
    .await
    .run("original user message")
    .await;

    let error = journey.result.unwrap_err();
    let RuntimeError::Provider(provider) = &error else {
        panic!("expected provider error, got {error:?}");
    };
    assert_eq!(provider.kind, ProviderErrorKind::Server);
    assert_eq!(provider.message, "Chat stream error");
    assert!(!provider.retryable);
    assert_sentinels_absent("error", &format!("{error} {error:?}"));
    assert_eq!(journey.captured.len(), 1);
    assert_no_tool_effects(&journey.records, journey.executions.as_deref().unwrap());

    let ended: Vec<MessageEnded> = journey
        .records
        .iter()
        .filter(|record| record.kind == EventKind::MessageStreamEnded)
        .map(|record| serde_json::from_value(record.payload.clone()).unwrap())
        .collect();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].outcome, StreamOutcome::Failed);
    assert_eq!(
        journey
            .records
            .iter()
            .filter(|record| record.kind == EventKind::ToolCallStarted)
            .count(),
        1,
        "partial tool call start was not observed"
    );
    assert_eq!(
        journey
            .records
            .iter()
            .filter(|record| record.kind == EventKind::ToolCallArgsDelta)
            .count(),
        1,
        "partial tool arguments were not observed"
    );
    assert!(!journey.records.iter().any(|record| matches!(
        record.kind,
        EventKind::TurnCompleted | EventKind::MessageCommitted
    )));
    assert!(!journey.records.iter().any(|record| {
        record
            .payload
            .get("status")
            .is_some_and(|value| value == "completed")
            || record
                .payload
                .get("outcome")
                .is_some_and(|value| value == "completed")
    }));
    assert_sentinels_absent("event", &serde_json::to_string(&journey.records).unwrap());

    let messages = journey
        .store
        .list_all_messages(&journey.session_id)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, Role::User);
    let stored = serde_json::to_string(&messages).unwrap();
    assert!(stored.contains("original user message"));
    assert!(!stored.contains("partial-call"));
    assert!(!stored.contains("\"arguments\":\"{\""));
    assert_sentinels_absent("store", &stored);
    let run = journey
        .store
        .get_run(&journey.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let persisted_error = run
        .error
        .expect("failed run must persist a sanitized error");
    assert!(persisted_error.contains("Chat stream error"));
    assert_sentinels_absent("persisted run error", &persisted_error);
}
