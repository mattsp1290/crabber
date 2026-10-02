use crate::host::{AgentFactory, Host};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{ContentBlock, ToolCallId, ToolInfo},
    extension::{Extension, Point, Registrar, Scope, ToolResultTransform},
    session::{MemoryStore, Store},
};
use crabber_agui::ag_ui_core::{event::Event, types::Message};
use serde_json::{Value, json};
use std::{error::Error, sync::Arc};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub type CheckResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct Echo;
#[async_trait]
impl ToolExecutor for Echo {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        Ok(json!({"echo":input, "private":"SECRET_SENTINEL"}))
    }
}
struct Redact;
#[async_trait]
impl Extension for Redact {
    fn id(&self) -> &'static str {
        "agui-example/redact"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "public-echo".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.on_transform(
            ToolResultTransform::ID,
            0,
            "redact",
            Arc::new(|mut value| {
                Box::pin(async move {
                    if let Some(object) = value["result"].as_object_mut() {
                        object.remove("private");
                    }
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}

#[must_use]
pub fn journey(unicode: bool) -> Vec<Vec<StreamDelta>> {
    let mut tools = Vec::new();
    for id in ["echo-a", "echo-b"] {
        tools.extend([
            StreamDelta::ToolCallStart {
                call_id: ToolCallId::from(id),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: ToolCallId::from(id),
                text: "{\"text\":".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: ToolCallId::from(id),
                text: "\"hello\"}".into(),
            },
            StreamDelta::ToolCallDone {
                call_id: ToolCallId::from(id),
            },
        ]);
    }
    tools.push(StreamDelta::Completed);
    vec![
        tools,
        vec![
            StreamDelta::TextDelta(
                if unicode {
                    "Héllo 🌍 中\n\r\"quoted\""
                } else {
                    "Hello from Crabber"
                }
                .into(),
            ),
            StreamDelta::Completed,
        ],
    ]
}

#[must_use]
pub fn builder(
    store: Arc<MemoryStore>,
    provider: Arc<dyn crabber::providers::Resolver>,
) -> crabber::AgentBuilder {
    Agent::builder().store(Arc::new(crate::thread_store::ThreadStore(store))).provider(provider)
        .config(AgentConfig::new(Selection { provider_id: "fake".into(), model_id: "scripted".into() }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
        .tool(Arc::new(ToolDefinition { info: ToolInfo { name:"echo".into(), description:"Echo public input".into(),
            parameters:json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}), retry_safe:true, required_permissions:vec![] }, executor:Arc::new(Echo) }))
        .extension(Arc::new(Redact), Scope::Global)
}

#[must_use]
pub fn factory(store: Arc<MemoryStore>, scripts: Vec<Vec<StreamDelta>>) -> AgentFactory {
    Arc::new(move || {
        builder(
            store.clone(),
            Arc::new(FakeProvider::scripted(fresh_calls(&scripts))),
        )
        .build()
    })
}

fn fresh_calls(scripts: &[Vec<StreamDelta>]) -> Vec<Vec<StreamDelta>> {
    let mut ids = std::collections::BTreeMap::new();
    scripts
        .iter()
        .map(|script| {
            script
                .iter()
                .map(|delta| {
                    let mut delta = delta.clone();
                    match &mut delta {
                        StreamDelta::ToolCallStart { call_id, .. }
                        | StreamDelta::ToolCallArgsDelta { call_id, .. }
                        | StreamDelta::ToolCallDone { call_id } => {
                            *call_id = ids
                                .entry(call_id.clone())
                                .or_insert_with(ToolCallId::new)
                                .clone();
                        }
                        _ => {}
                    }
                    delta
                })
                .collect()
        })
        .collect()
}

pub struct Server {
    pub url: String,
    pub host: Host,
    stop: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}
impl Server {
    /// # Errors
    /// Returns loopback bind errors.
    pub async fn start(host: Host) -> CheckResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/run", listener.local_addr()?);
        let stop = CancellationToken::new();
        let signal = stop.clone();
        let router = host.router();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(signal.cancelled_owned())
                .await
        });
        Ok(Self {
            url,
            host,
            stop,
            task,
        })
    }
    /// # Errors
    /// Reports workers/listeners that fail to stop in the host cleanup deadline.
    /// # Panics
    /// Panics if the host reports completion with active workers.
    pub async fn stop(self) -> CheckResult {
        let cleanup = self.host.shutdown().await;
        self.stop.cancel();
        tokio::time::timeout(self.host.cleanup, self.task).await???;
        cleanup?;
        assert_eq!(
            self.host.active.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        Ok(())
    }
}

#[must_use]
pub fn input(thread: &str) -> Value {
    json!({"threadId":thread,"runId":"wire-alias","messages":[{"id":"client-message","role":"user","content":"hello"}]})
}

/// Independent byte-first decoder: UTF-8 is decoded only after complete SSE frames.
#[derive(Default)]
pub struct Decoder {
    pending: Vec<u8>,
    pub events: Vec<Event>,
}
impl Decoder {
    /// # Errors
    /// Rejects invalid framing/JSON; incomplete Unicode stays buffered.
    pub fn push(&mut self, bytes: &[u8]) -> CheckResult {
        self.pending.extend_from_slice(bytes);
        while let Some(end) = self.pending.windows(2).position(|b| b == b"\n\n") {
            let frame: Vec<_> = self.pending.drain(..end + 2).collect();
            if !frame.starts_with(b"data: ") {
                return Err("invalid SSE frame".into());
            }
            self.events
                .push(serde_json::from_slice(&frame[6..frame.len() - 2])?);
        }
        Ok(())
    }
    /// # Errors
    /// Rejects a partial final frame or missing/duplicate terminal.
    pub fn finish(self) -> CheckResult<Vec<Event>> {
        if !self.pending.is_empty() {
            return Err("partial SSE frame".into());
        }
        let terminals = self
            .events
            .iter()
            .filter(|e| matches!(e, Event::RunFinished(_) | Event::RunError(_)))
            .count();
        if terminals != 1
            || !self
                .events
                .last()
                .is_some_and(|e| matches!(e, Event::RunFinished(_) | Event::RunError(_)))
        {
            return Err("invalid terminal sequence".into());
        }
        Ok(self.events)
    }
}

/// # Errors
/// Returns HTTP/byte-decoder failures.
/// # Panics
/// Panics on an incorrect content type, leaked sentinel or replay marker.
pub async fn request(server: &Server, thread: &str) -> CheckResult<Vec<Event>> {
    let response = reqwest::Client::new()
        .post(&server.url)
        .json(&input(thread))
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::OK {
        return Err("run HTTP status".into());
    }
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let bytes = response.bytes().await?;
    assert!(!bytes.windows(6).any(|b| b == b"[DONE]"));
    assert!(!bytes.windows(4).any(|b| b == b"id: "));
    assert!(!String::from_utf8_lossy(&bytes).contains("SECRET_SENTINEL"));
    let mut decoder = Decoder::default();
    // Deterministically split every Unicode scalar and every frame delimiter.
    for byte in bytes {
        decoder.push(&[byte])?;
    }
    decoder.finish()
}

/// Execute real pinned-client ASCII and independent Unicode HTTP journeys.
///
/// # Errors
/// Returns any failed lifecycle or reconstruction check.
/// # Panics
/// Panics when the pinned-client or stored-transcript assertions fail.
pub async fn run() -> CheckResult {
    use ag_ui_client::{Agent as _, HttpAgent, RunAgentParams};
    let store = Arc::new(MemoryStore::new());
    let server = Server::start(Host::new(factory(store.clone(), journey(false)))).await?;
    let client = HttpAgent::builder()
        .with_url_str(&server.url)?
        .with_timeout(10)
        .build()?;
    let result = client
        .run_agent(&RunAgentParams::new().user("hello"), ())
        .await?;
    assert!(matches!(
        result.outcome,
        Some(crabber_agui::ag_ui_core::types::RunFinishedOutcome::Success { .. })
    ));
    assert_eq!(result.new_messages.len(), 4);
    let assistant = result
        .new_messages
        .iter()
        .find_map(|m| match m {
            Message::Assistant {
                tool_calls: Some(calls),
                ..
            } => Some(calls),
            _ => None,
        })
        .ok_or("missing calls")?;
    assert_eq!(assistant.len(), 2);
    for call in assistant {
        assert_eq!(call.function.name, "echo");
        assert_eq!(
            serde_json::from_str::<Value>(&call.function.arguments)?,
            json!({"text":"hello"})
        );
    }
    assert_eq!(
        result
            .new_messages
            .iter()
            .filter(|m| matches!(m, Message::Tool { .. }))
            .count(),
        2
    );
    assert!(result.new_messages.iter().any(|m| matches!(m, Message::Assistant { content: Some(content), .. } if content == "Hello from Crabber")));
    server.stop().await?;

    unicode().await?;
    println!("AG-UI: pinned ASCII client and fragmented Unicode HTTP journeys passed");
    Ok(())
}

async fn unicode() -> CheckResult {
    let store = Arc::new(MemoryStore::new());
    let server = Server::start(Host::new(factory(store.clone(), journey(true)))).await?;
    let events = request(&server, "unicode").await?;
    let messages = store
        .list_messages(&crabber::core::SessionId::from("unicode"), None)
        .await?;
    for message in messages
        .iter()
        .filter(|m| m.role == crabber::core::Role::Assistant)
    {
        let projected: String = events
            .iter()
            .filter_map(|e| match e {
                Event::TextMessageContent(e)
                    if e.message_id.to_string() == message.id.to_string() =>
                {
                    Some(e.delta.as_str())
                }
                _ => None,
            })
            .collect();
        let stored: String = message
            .parts
            .iter()
            .filter_map(|p| match &p.content {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(projected, stored);
    }
    for message in messages
        .iter()
        .filter(|m| m.role == crabber::core::Role::Tool)
    {
        let ContentBlock::ToolResult {
            content, is_error, ..
        } = &message.parts[0].content
        else {
            return Err("stored result".into());
        };
        let projected = events
            .iter()
            .find_map(|e| match e {
                Event::ToolCallResult(e) if e.message_id.to_string() == message.id.to_string() => {
                    e.content.as_text()
                }
                _ => None,
            })
            .ok_or("missing result")?;
        assert_eq!(
            serde_json::from_str::<Value>(projected)?,
            crabber_agui::public_tool_content(content, *is_error, false)
        );
    }
    server.stop().await?;
    Ok(())
}
