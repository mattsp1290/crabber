#![cfg(feature = "anthropic")]
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, JsonToolExecutor, Observer, PermissionDecision,
    RuntimeError, Selection, StaticPolicy, ToolDefinition,
    core::{Role, RunStatus, ToolInfo},
    extension::ExtensionError,
    providers::{HttpAdapter, HttpResolver, ProviderErrorKind},
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(10);
type Captured = (BTreeMap<String, String>, Value);

async fn read_request(socket: &mut TcpStream) -> Captured {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0, "request ended before headers");
        raw.extend_from_slice(&buffer[..n]);
        if let Some(end) = raw.windows(4).position(|b| b == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(raw.len() < 65536);
    };
    assert!(raw.starts_with(b"POST /messages HTTP/1.1\r\n"));
    let headers: BTreeMap<_, _> = std::str::from_utf8(&raw[..end])
        .unwrap()
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers["content-length"].parse().unwrap();
    assert!(length < 65536);
    while raw.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0, "request ended before body");
        raw.extend_from_slice(&buffer[..n]);
    }
    (
        headers,
        serde_json::from_slice(&raw[end..end + length]).unwrap(),
    )
}

async fn server(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(LIMIT, async move {
            let mut captured = Vec::new();
            for body in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                captured.push(read_request(&mut socket).await);
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            captured
        })
        .await
        .expect("loopback server timed out")
    });
    (url, task)
}

fn frame(name: &str, data: &Value) -> String {
    format!("event: {name}\ndata: {data}\n\n")
}
fn start() -> String {
    frame(
        "message_start",
        &json!({"type":"message_start","message":{"usage":{"input_tokens":3}}}),
    )
}
fn block_start(index: u32, block: &Value) -> String {
    frame(
        "content_block_start",
        &json!({"type":"content_block_start","index":index,"content_block":block}),
    )
}
fn block_delta(index: u32, delta: &Value) -> String {
    frame(
        "content_block_delta",
        &json!({"type":"content_block_delta","index":index,"delta":delta}),
    )
}
fn block_stop(index: u32) -> String {
    frame(
        "content_block_stop",
        &json!({"type":"content_block_stop","index":index}),
    )
}
fn text_block(index: u32, text: &str) -> String {
    block_start(index, &json!({"type":"text","text":""}))
        + &block_delta(index, &json!({"type":"text_delta","text":text}))
        + &block_stop(index)
}
fn tool_block(index: u32) -> String {
    block_start(
        index,
        &json!({"type":"tool_use","id":"toolu_1","name":"echo","input":{}}),
    ) + &block_delta(
        index,
        &json!({"type":"input_json_delta","partial_json":"{\"text\":\"hello\"}"}),
    ) + &block_stop(index)
}
fn finish(reason: &str, output: u32) -> String {
    frame(
        "message_delta",
        &json!({"type":"message_delta","delta":{"stop_reason":reason},"usage":{"output_tokens":output}}),
    ) + &frame("message_stop", &json!({"type":"message_stop"}))
}
fn overloaded() -> String {
    frame(
        "error",
        &json!({"type":"error","error":{"type":"overloaded_error","message":"PRIVATE_BODY_SENTINEL"}}),
    )
}
fn text() -> String {
    start() + &text_block(0, "OK") + &finish("end_turn", 2)
}
fn tool_call() -> String {
    start() + &tool_block(0) + &finish("tool_use", 5)
}
fn thinking_then_tool() -> String {
    start()
        + &block_start(0, &json!({"type":"thinking","thinking":""}))
        + &block_delta(
            0,
            &json!({"type":"thinking_delta","thinking":"PRIVATE_THOUGHT"}),
        )
        + &block_delta(
            0,
            &json!({"type":"signature_delta","signature":"SIGNATURE_A"}),
        )
        + &block_stop(0)
        + &tool_block(1)
        + &finish("tool_use", 5)
}
fn refusal_after_text() -> String {
    start() + &text_block(0, "PARTIAL_REFUSED_TEXT") + &finish("refusal", 2)
}
fn overloaded_early() -> String {
    start() + &overloaded()
}
fn overloaded_after_text() -> String {
    start() + &text_block(0, "partial") + &overloaded()
}
fn thinking_then_overload() -> String {
    start()
        + &block_start(0, &json!({"type":"thinking","thinking":""}))
        + &block_delta(
            0,
            &json!({"type":"thinking_delta","thinking":"PRIVATE_THOUGHT"}),
        )
        + &overloaded()
}
fn text_then_eof() -> String {
    start() + &text_block(0, "partial")
}
fn overflow_after_text() -> String {
    start()
        + &text_block(0, "partial")
        + &frame(
            "message_delta",
            &json!({"type":"message_delta","delta":{"stop_reason":"model_context_window_exceeded"}}),
        )
        + &frame("message_stop", &json!({"type":"message_stop"}))
}
const TRUNCATED_TOOL: &str =
    include_str!("../../crabber-providers/testdata/anthropic/truncated_tool.sse");

#[derive(Default)]
struct Events(Mutex<Vec<EventRecord>>);
impl Observer for Events {
    fn emit(&self, event: &EventRecord) {
        self.0.lock().unwrap().push(event.clone());
    }
}
impl Events {
    fn all(&self) -> Vec<EventRecord> {
        self.0.lock().unwrap().clone()
    }
    fn serialized(&self) -> String {
        serde_json::to_string(&self.all()).unwrap()
    }
    fn of_kind(&self, kind: &EventKind) -> Vec<EventRecord> {
        self.all()
            .into_iter()
            .filter(|event| &event.kind == kind)
            .collect()
    }
}

struct Echo;
#[async_trait]
impl JsonToolExecutor for Echo {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        Ok(value)
    }
}
fn echo_tool() -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(Echo),
    })
}

fn builder(url: &str, store: Arc<MemoryStore>) -> crabber::AgentBuilder {
    let adapter = HttpAdapter::anthropic()
        .with_base_url(url)
        .with_api_key("key")
        .try_with_user_agent_product("crabber-channels/0.1.0")
        .unwrap();
    Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .provider(Arc::new(HttpResolver::new().with_adapter(adapter)))
        .config(
            AgentConfig::new(Selection {
                provider_id: "anthropic".into(),
                model_id: "claude-fixture".into(),
            })
            .max_output_tokens(1234),
        )
}

/// Runs one prompt to completion; returns the session id and the run outcome.
async fn run(
    agent: &Agent,
) -> (
    crabber::core::SessionId,
    Result<crabber::RunResult, RuntimeError>,
) {
    timeout(LIMIT, async {
        let handle = agent.prompt(None, "hi").await.unwrap();
        let session_id = handle.session_id().clone();
        (session_id, handle.done().await)
    })
    .await
    .unwrap()
}

async fn assistant_messages(
    store: &MemoryStore,
    session: &crabber::core::SessionId,
) -> Vec<crabber::core::Message> {
    store
        .list_messages(session, None)
        .await
        .unwrap()
        .into_iter()
        .filter(|message| message.role == Role::Assistant)
        .collect()
}

fn provider_error(
    result: Result<crabber::RunResult, RuntimeError>,
) -> crabber::providers::ProviderError {
    match result {
        Err(RuntimeError::Provider(error)) => error,
        other => panic!("expected provider error, got {other:?}"),
    }
}

#[tokio::test]
async fn facade_sends_messages_headers_and_tool_free_body() {
    let (url, task) = server(vec![text()]).await;
    let agent = builder(&url, Arc::new(MemoryStore::new())).build().unwrap();
    let (_, result) = run(&agent).await;
    assert_eq!(result.unwrap().status, RunStatus::Completed);
    let captured = task.await.unwrap();
    assert_eq!(captured.len(), 1);
    let (headers, body) = &captured[0];
    assert_eq!(headers["x-api-key"], "key");
    assert_eq!(headers["anthropic-version"], "2023-06-01");
    assert_eq!(headers["user-agent"], "crabber-channels/0.1.0 crabber/0.1");
    assert!(!headers.contains_key("authorization"));
    assert_eq!(body["model"], "claude-fixture");
    assert_eq!(body["stream"], true);
    assert_eq!(body["max_tokens"], 1234);
    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(
        body["messages"][0]["content"][0],
        json!({"type":"text","text":"hi"})
    );
}

#[tokio::test]
async fn facade_round_trips_tool_use_and_tool_result() {
    let (url, task) = server(vec![tool_call(), text()]).await;
    let events = Arc::new(Events::default());
    let agent = builder(&url, Arc::new(MemoryStore::new()))
        .shared_tool(echo_tool())
        .observer(events.clone())
        .build()
        .unwrap();
    let (_, result) = run(&agent).await;
    assert_eq!(result.unwrap().status, RunStatus::Completed);
    let captured = task.await.unwrap();
    assert_eq!(captured.len(), 2);
    let first = &captured[0].1;
    assert_eq!(first["tools"][0]["name"], "echo");
    assert!(first["tools"][0]["input_schema"].is_object());
    assert!(first.get("tool_choice").is_none());
    let second = &captured[1].1;
    let messages = second["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["role"], "user");
    assert_eq!(messages[1]["role"], "assistant");
    assert!(messages[1]["content"].as_array().unwrap().contains(
        &json!({"type":"tool_use","id":"toolu_1","name":"echo","input":{"text":"hello"}})
    ));
    assert_eq!(messages[2]["role"], "user");
    let result_block = &messages[2]["content"].as_array().unwrap()[0];
    assert_eq!(result_block["type"], "tool_result");
    assert_eq!(result_block["tool_use_id"], "toolu_1");
    assert_eq!(result_block["is_error"], false);
    let content = result_block["content"].as_str().expect("string content");
    assert_eq!(
        serde_json::from_str::<Value>(content).unwrap(),
        json!({"text":"hello"})
    );
    for (_, body) in &captured {
        assert_eq!(body["max_tokens"], 1234);
    }
    let settled = events.of_kind(&EventKind::ToolCallSettled);
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].payload["name"], "echo");
    assert_eq!(settled[0].payload["status"], "completed");
}

#[tokio::test]
async fn facade_replays_signed_thinking_before_tool_use() {
    let (url, task) = server(vec![thinking_then_tool(), text()]).await;
    let events = Arc::new(Events::default());
    let agent = builder(&url, Arc::new(MemoryStore::new()))
        .shared_tool(echo_tool())
        .observer(events.clone())
        .build()
        .unwrap();
    let (_, result) = run(&agent).await;
    assert_eq!(result.unwrap().status, RunStatus::Completed);
    let captured = task.await.unwrap();
    assert_eq!(captured.len(), 2);
    let assistant = &captured[1].1["messages"][1]["content"];
    assert_eq!(
        assistant[0],
        json!({"type":"thinking","thinking":"PRIVATE_THOUGHT","signature":"SIGNATURE_A"})
    );
    assert_eq!(assistant[1]["type"], "tool_use");
    let serialized = events.serialized();
    assert!(!serialized.contains("PRIVATE_THOUGHT"));
    assert!(!serialized.contains("SIGNATURE_A"));
}

#[tokio::test]
async fn facade_refusal_fails_run_and_persists_no_assistant_message() {
    let (url, task) = server(vec![refusal_after_text()]).await;
    let events = Arc::new(Events::default());
    let store = Arc::new(MemoryStore::new());
    let agent = builder(&url, store.clone())
        .observer(events.clone())
        .build()
        .unwrap();
    let (session, result) = run(&agent).await;
    let error = provider_error(result);
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    assert!(!error.retryable);
    assert_eq!(error.message, "Messages refusal");
    assert!(!format!("{error:?}").contains("PARTIAL_REFUSED_TEXT"));
    assert_eq!(task.await.unwrap().len(), 1);
    assert_eq!(assistant_messages(&store, &session).await.len(), 0);
    // Live deltas are emitted immediately and are not rolled back.
    let deltas = events.of_kind(&EventKind::TextDelta);
    assert_eq!(deltas.len(), 1);
    assert!(
        deltas[0]
            .payload
            .to_string()
            .contains("PARTIAL_REFUSED_TEXT")
    );
}

#[tokio::test]
async fn facade_truncated_tool_input_fails_run() {
    let (url, task) = server(vec![TRUNCATED_TOOL.to_owned()]).await;
    let events = Arc::new(Events::default());
    let store = Arc::new(MemoryStore::new());
    let agent = builder(&url, store.clone())
        .shared_tool(echo_tool())
        .observer(events.clone())
        .build()
        .unwrap();
    let (session, result) = run(&agent).await;
    let error = provider_error(result);
    assert_eq!(error.message, "Messages output truncated");
    assert_eq!(task.await.unwrap().len(), 1);
    assert_eq!(assistant_messages(&store, &session).await.len(), 0);
    assert_eq!(events.of_kind(&EventKind::ToolCallSettled).len(), 0);
}

#[tokio::test]
async fn facade_early_overload_is_retried_without_duplicate_text() {
    let (url, task) = server(vec![overloaded_early(), overloaded_early(), text()]).await;
    let events = Arc::new(Events::default());
    let store = Arc::new(MemoryStore::new());
    let agent = builder(&url, store.clone())
        .observer(events.clone())
        .build()
        .unwrap();
    let started = Instant::now();
    let (session, result) = run(&agent).await;
    assert_eq!(result.unwrap().status, RunStatus::Completed);
    assert!(started.elapsed() < LIMIT);
    assert_eq!(task.await.unwrap().len(), 3);
    let assistants = assistant_messages(&store, &session).await;
    assert_eq!(assistants.len(), 1);
    let persisted = serde_json::to_string(&assistants[0].parts).unwrap();
    assert!(persisted.contains("\"OK\""), "{persisted}");
    let streamed: String = events
        .of_kind(&EventKind::TextDelta)
        .iter()
        .map(|event| event.payload.to_string())
        .collect();
    assert_eq!(streamed.matches("OK").count(), 1, "{streamed}");
}

#[tokio::test]
async fn facade_overload_after_content_is_not_retried() {
    let (url, task) = server(vec![overloaded_after_text()]).await;
    let store = Arc::new(MemoryStore::new());
    let agent = builder(&url, store.clone()).build().unwrap();
    let (session, result) = run(&agent).await;
    let error = provider_error(result);
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(!error.retryable);
    assert_eq!(error.message, "Messages stream error");
    assert_eq!(task.await.unwrap().len(), 1);
    assert_eq!(assistant_messages(&store, &session).await.len(), 0);
}

#[tokio::test]
async fn facade_private_thinking_before_overload_is_retried() {
    let (url, task) = server(vec![thinking_then_overload(), text()]).await;
    let events = Arc::new(Events::default());
    let agent = builder(&url, Arc::new(MemoryStore::new()))
        .observer(events.clone())
        .build()
        .unwrap();
    let (_, result) = run(&agent).await;
    assert_eq!(result.unwrap().status, RunStatus::Completed);
    assert_eq!(task.await.unwrap().len(), 2);
    assert!(!events.serialized().contains("PRIVATE_THOUGHT"));
}

#[tokio::test]
async fn facade_transport_failure_after_text_is_not_retried() {
    let (url, task) = server(vec![text_then_eof()]).await;
    let store = Arc::new(MemoryStore::new());
    let agent = builder(&url, store.clone()).build().unwrap();
    let (session, result) = run(&agent).await;
    let error = provider_error(result);
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(!error.retryable);
    assert_eq!(task.await.unwrap().len(), 1);
    assert_eq!(assistant_messages(&store, &session).await.len(), 0);
}

#[tokio::test]
async fn facade_overflow_after_text_compacts_and_reruns() {
    // Context overflow is the documented exception to "no duplicate deltas":
    // the runtime compacts and re-runs, so the failed attempt's text stays
    // visible to observers ahead of the re-run's text.
    // Requests: the overflowing attempt, the compaction summary, the re-run.
    let (url, task) = server(vec![overflow_after_text(), text(), text()]).await;
    let events = Arc::new(Events::default());
    let store = Arc::new(MemoryStore::new());
    let agent = builder(&url, store.clone())
        .observer(events.clone())
        .build()
        .unwrap();
    let (session, result) = run(&agent).await;
    assert_eq!(result.unwrap().status, RunStatus::Completed);
    assert_eq!(task.await.unwrap().len(), 3);
    assert_eq!(events.of_kind(&EventKind::ContextEpochStarted).len(), 1);
    let streamed: Vec<_> = events
        .of_kind(&EventKind::TextDelta)
        .iter()
        .map(|event| event.payload["text"].clone())
        .collect();
    assert_eq!(streamed, [json!("partial"), json!("OK")]);
    assert_eq!(assistant_messages(&store, &session).await.len(), 1);
}
