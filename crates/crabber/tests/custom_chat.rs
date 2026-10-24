#![cfg(feature = "custom-http")]

use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, JsonToolExecutor, ModelControls, PermissionDecision,
    ReasoningEffort, RuntimeError, Selection, StaticPolicy, ToolDefinition,
    core::{
        EventKind, InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, Role,
        RunStatus, ToolInfo,
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
use tokio::time::timeout;

#[path = "http_loopback/support.rs"]
mod loopback;
use loopback::{LIMIT, counted_server};

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

fn response() -> String {
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
    format!("data: {start}\n\ndata: {error}\n\ndata: [DONE]\n\n")
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
    format!("data: {error}\n\ndata: [DONE]\n\n")
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

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "single end-to-end retry journey")]
async fn chat_pre_output_error_retries_once_then_commits_one_response() {
    let requests = Arc::new(AtomicUsize::new(0));
    let (endpoint, server, requests) = counted_server(
        vec![retryable_response(), clean_response("ok-after-retry")],
        "/chat/completions",
        Arc::clone(&requests),
    )
    .await;
    let store = Arc::new(MemoryStore::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .behavior_version("custom-chat-pre-output-retry.v1")
        .store(store.clone())
        .provider(Arc::new(resolver(&endpoint)))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "count".into(),
                description: "count executions".into(),
                parameters: json!({"type": "object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(CountingTool(Arc::clone(&executions))),
        }))
        .build()
        .unwrap();

    let mut handle = timeout(LIMIT, agent.prompt(None, "retry user message"))
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
        records
    };
    let (records, result) = tokio::join!(timeout(LIMIT, drain), timeout(LIMIT, handle.done()));
    let records = records.expect("event drain timed out");
    assert_eq!(
        result.expect("run completion timed out").unwrap().status,
        RunStatus::Completed
    );

    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == EventKind::TextDelta)
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == EventKind::MessageCommitted)
            .count(),
        1
    );
    assert!(!records.iter().any(|record| matches!(
        record.kind,
        EventKind::ToolCallStarted
            | EventKind::ToolCallArgsDelta
            | EventKind::ToolCallPending
            | EventKind::ToolCallSettled
    )));
    let public_events = serde_json::to_string(&records).unwrap();
    assert_eq!(public_events.matches("ok-after-retry").count(), 1);
    for sentinel in SENTINELS {
        assert!(!public_events.contains(sentinel), "event leaked {sentinel}");
    }

    let messages = store.list_all_messages(&session_id).await.unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .count(),
        1
    );
    let stored = serde_json::to_string(&messages).unwrap();
    assert_eq!(stored.matches("ok-after-retry").count(), 1);
    for sentinel in SENTINELS {
        assert!(!stored.contains(sentinel), "store leaked {sentinel}");
    }
    let run = store.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert!(run.error.is_none());
    let captured = timeout(LIMIT, server)
        .await
        .expect("server join timed out")
        .unwrap();
    assert_eq!(captured.len(), 2);
}

#[tokio::test]
async fn agent_reasoning_effort_high_reaches_chat_wire_body() {
    let requests = Arc::new(AtomicUsize::new(0));
    let (endpoint, server, requests) = counted_server(
        vec![clean_response("done")],
        "/chat/completions",
        Arc::clone(&requests),
    )
    .await;
    let config = AgentConfig::new(selection())
        .model_controls(ModelControls::default().reasoning_effort(ReasoningEffort::High));
    let agent = Agent::builder()
        .behavior_version("custom-chat-reasoning-effort.v1")
        .store(Arc::new(MemoryStore::new()))
        .provider(Arc::new(resolver(&endpoint)))
        .config(config)
        .build()
        .unwrap();

    let result = timeout(LIMIT, async {
        agent.prompt(None, "reason carefully").await?.done().await
    })
    .await
    .expect("agent run timed out")
    .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let captured = timeout(LIMIT, server)
        .await
        .expect("server join timed out")
        .unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].1.get("reasoning_effort"), Some(&json!("high")));
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "single end-to-end regression journey"
)]
async fn chat_stream_error_after_partial_tool_call_fails_without_dispatch() {
    let requests = Arc::new(AtomicUsize::new(0));
    let (endpoint, server, requests) =
        counted_server(vec![response()], "/chat/completions", Arc::clone(&requests)).await;
    let selection = Selection {
        provider_id: "fixture".into(),
        model_id: "model".into(),
    };
    let adapter = HttpAdapter::custom("fixture", endpoint, Protocol::ChatCompletions)
        .with_api_key("fixture-key");
    let resolver = VersionedResolver::new(
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
    .unwrap();
    let store = Arc::new(MemoryStore::new());
    let executions = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .behavior_version("custom-chat-stream-error.v1")
        .store(store.clone())
        .provider(Arc::new(resolver))
        .config(AgentConfig::new(selection))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "count".into(),
                description: "count executions".into(),
                parameters: json!({"type": "object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(CountingTool(Arc::clone(&executions))),
        }))
        .build()
        .unwrap();

    let mut handle = timeout(LIMIT, agent.prompt(None, "original user message"))
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
    let records = records.expect("event drain timed out");
    let error = result.expect("run completion timed out").unwrap_err();

    let RuntimeError::Provider(provider) = &error else {
        panic!("expected provider error, got {error:?}");
    };
    assert_eq!(provider.kind, ProviderErrorKind::Server);
    assert_eq!(provider.message, "Chat stream error");
    assert!(!provider.retryable);
    let diagnostic = format!("{error} {error:?}");
    for sentinel in SENTINELS {
        assert!(!diagnostic.contains(sentinel), "error leaked {sentinel}");
    }

    let ended: Vec<MessageEnded> = records
        .iter()
        .filter(|record| record.kind == EventKind::MessageStreamEnded)
        .map(|record| serde_json::from_value(record.payload.clone()).unwrap())
        .collect();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].outcome, StreamOutcome::Failed);
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == EventKind::ToolCallStarted)
            .count(),
        1,
        "partial tool call start was not observed"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == EventKind::ToolCallArgsDelta)
            .count(),
        1,
        "partial tool arguments were not observed"
    );
    assert!(!records.iter().any(|record| matches!(
        record.kind,
        EventKind::TurnCompleted
            | EventKind::MessageCommitted
            | EventKind::ToolCallPending
            | EventKind::ToolCallSettled
    )));
    assert!(!records.iter().any(|record| {
        record
            .payload
            .get("status")
            .is_some_and(|value| value == "completed")
            || record
                .payload
                .get("outcome")
                .is_some_and(|value| value == "completed")
    }));
    let public_events = serde_json::to_string(&records).unwrap();
    for sentinel in SENTINELS {
        assert!(!public_events.contains(sentinel), "event leaked {sentinel}");
    }

    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let captured = timeout(LIMIT, server)
        .await
        .expect("server join timed out")
        .unwrap();
    assert_eq!(captured.len(), 1);

    let messages = store.list_all_messages(&session_id).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, Role::User);
    let stored = serde_json::to_string(&messages).unwrap();
    assert!(stored.contains("original user message"));
    assert!(!stored.contains("partial-call"));
    assert!(!stored.contains("\"arguments\":\"{\""));
    for sentinel in SENTINELS {
        assert!(!stored.contains(sentinel), "store leaked {sentinel}");
    }
    let run = store.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let persisted_error = run
        .error
        .expect("failed run must persist a sanitized error");
    assert!(persisted_error.contains("Chat stream error"));
    for sentinel in SENTINELS {
        assert!(
            !persisted_error.contains(sentinel),
            "persisted run error leaked {sentinel}"
        );
    }
}
