#![cfg(feature = "anthropic")]
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, JsonToolExecutor, Observer, OperationKind,
    OperationalObservation, PermissionDecision, Selection, StaticPolicy, TerminalReason,
    ToolDefinition,
    core::{
        ContentBlock, MediaKind, MediaSource, Message, MessageId, Part, PartId, PartKind, Role,
        RunId, RunStatus, SessionId, ToolInfo, TurnId,
    },
    extension::ExtensionError,
    providers::{
        HttpAdapter, HttpResolver, ModelRequest, ProviderAdapter, ProviderError, RequestIdentity,
        Resolver, StreamDelta, Streamer,
    },
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const KEY_ENV: &str = "CRABBER_ANTHROPIC_API_KEY";
const MODEL_ENV: &str = "CRABBER_ANTHROPIC_LIVE_MODEL";
const DEFAULT_MODEL: &str = "claude-opus-5-5";
const LIMIT: Duration = Duration::from_secs(180);
/// The 1x1 PNG published as a test image in the Anthropic vision guide.
const ONE_PIXEL_PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";

fn live_key() -> String {
    let key = std::env::var(KEY_ENV).unwrap_or_default();
    assert!(
        !key.is_empty(),
        "set {KEY_ENV} before running the live acceptance tests"
    );
    key
}
fn live_adapter() -> (HttpAdapter, String, String) {
    let key = live_key();
    let model = std::env::var(MODEL_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_MODEL.to_owned());
    let adapter = HttpAdapter::anthropic()
        .with_api_key(key.clone())
        .try_with_user_agent_product("crabber-live-check/0.1")
        .unwrap();
    (adapter, model, key)
}
fn selection(model: &str) -> Selection {
    Selection {
        provider_id: "anthropic".into(),
        model_id: model.into(),
    }
}

#[derive(Default)]
struct Observed {
    reasons: Mutex<Vec<TerminalReason>>,
    events: Mutex<Vec<EventRecord>>,
}
impl Observer for Observed {
    fn emit(&self, event: &EventRecord) {
        self.events.lock().unwrap().push(event.clone());
    }
    fn operational_completed(&self, observation: &OperationalObservation) {
        if matches!(observation.kind, OperationKind::Model { .. }) {
            self.reasons.lock().unwrap().push(observation.reason);
        }
    }
}
impl Observed {
    fn serialized(&self) -> String {
        serde_json::to_string(&*self.events.lock().unwrap()).unwrap()
    }
}

struct CapturingResolver {
    inner: HttpResolver,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl CapturingResolver {
    fn new(adapter: HttpAdapter) -> (Self, Arc<Mutex<Vec<ModelRequest>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: HttpResolver::new().with_adapter(adapter),
                requests: requests.clone(),
            },
            requests,
        )
    }
}
struct CapturingStreamer {
    inner: Arc<dyn Streamer>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
#[async_trait]
impl Streamer for CapturingStreamer {
    async fn stream(
        &self,
        request: ModelRequest,
    ) -> Result<crabber::providers::DeltaStream, ProviderError> {
        self.requests.lock().unwrap().push(request.clone());
        self.inner.stream(request).await
    }
}
#[async_trait]
impl Resolver for CapturingResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        self.inner.binding_descriptor(selection)
    }
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(CapturingStreamer {
            inner: self.inner.resolve(selection).await?,
            requests: self.requests.clone(),
        }))
    }
}

struct Echo;
#[async_trait]
impl JsonToolExecutor for Echo {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        Ok(value)
    }
}

fn agent_builder(
    resolver: Arc<dyn Resolver>,
    observer: Arc<Observed>,
    model: &str,
) -> crabber::AgentBuilder {
    Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .memory()
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .provider(resolver)
        .observer(observer)
        .config(AgentConfig::new(selection(model)).max_output_tokens(8192))
}

async fn drive(agent: &Agent, prompt: &str) -> (bool, crabber::RunResult) {
    tokio::time::timeout(LIMIT, async {
        let mut handle = agent.prompt(None, prompt).await.unwrap();
        let mut events = handle.events();
        let mut saw_text = false;
        while let Some(event) = events.recv().await.unwrap() {
            saw_text |= matches!(event.kind, EventKind::TextDelta);
        }
        (saw_text, handle.done().await.unwrap())
    })
    .await
    .expect("live acceptance timed out")
}

#[tokio::test]
#[ignore = "requires CRABBER_ANTHROPIC_API_KEY and live Anthropic API access"]
async fn live_tool_free_text_completes_through_facade() {
    let (adapter, model, key) = live_adapter();
    let observer = Arc::new(Observed::default());
    let agent = agent_builder(
        Arc::new(HttpResolver::new().with_adapter(adapter)),
        observer.clone(),
        &model,
    )
    .build()
    .unwrap();
    let (saw_text, result) = drive(&agent, "Reply with the single word OK.").await;
    assert!(saw_text, "provider returned no text delta");
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(*observer.reasons.lock().unwrap(), [TerminalReason::Success]);
    assert!(!observer.serialized().contains(&key));
}

#[tokio::test]
#[ignore = "requires CRABBER_ANTHROPIC_API_KEY and live Anthropic API access"]
async fn live_echo_tool_round_trip_settles() {
    let (adapter, model, key) = live_adapter();
    let observer = Arc::new(Observed::default());
    let (resolver, requests) = CapturingResolver::new(adapter);
    let tool = Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: "Echo the text argument".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(Echo),
    });
    let agent = agent_builder(Arc::new(resolver), observer.clone(), &model)
        .shared_tool(tool)
        .build()
        .unwrap();
    let (_, result) = drive(
        &agent,
        "Call the echo tool with text \"hello\", then reply Done.",
    )
    .await;
    assert_eq!(result.status, RunStatus::Completed);
    let settled: Vec<_> = observer
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallSettled)
        .cloned()
        .collect();
    assert!(
        settled
            .iter()
            .any(|event| event.payload["name"] == "echo" && event.payload["status"] == "completed"),
        "no completed echo ToolCallSettled event"
    );
    assert_eq!(
        *observer.reasons.lock().unwrap(),
        [TerminalReason::Success, TerminalReason::Success]
    );
    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 2);
    let thinking_replayed = captured[1].messages.iter().any(|message| {
        message.parts.iter().any(|part| {
            matches!(&part.content, ContentBlock::Reasoning { provider_state: Some(state), .. }
                if state["signature"].as_str().is_some_and(|s| !s.is_empty()))
        })
    });
    println!("thinking_replayed={thinking_replayed}");
    assert!(!observer.serialized().contains(&key));
}

#[tokio::test]
#[ignore = "requires CRABBER_ANTHROPIC_API_KEY and live Anthropic API access"]
async fn live_model_catalog_contains_selected_model() {
    let (adapter, model, key) = live_adapter();
    let models = tokio::time::timeout(LIMIT, adapter.models())
        .await
        .expect("catalog request timed out")
        .unwrap_or_else(|error| panic!("catalog request failed: {:?}", error.kind));
    assert!(!models.is_empty(), "catalog is empty");
    assert!(models.iter().all(|descriptor| !descriptor.id.is_empty()));
    let ids: Vec<_> = models
        .iter()
        .map(|descriptor| descriptor.id.as_str())
        .collect();
    println!("catalog ids: {ids:?}");
    assert!(
        ids.iter()
            .any(|id| *id == model || id.starts_with(&model) || model.starts_with(id)),
        "selected model {model} not found in catalog; set {MODEL_ENV} to one of the printed ids"
    );
    assert!(!format!("{models:?}").contains(&key));
}

#[tokio::test]
#[ignore = "requires CRABBER_ANTHROPIC_API_KEY and live Anthropic API access"]
async fn live_user_image_base64_streams_text() {
    let (adapter, model, key) = live_adapter();
    let streamer = adapter
        .build(&selection(&model))
        .await
        .unwrap_or_else(|error| panic!("adapter build failed: {:?}", error.kind));
    let message_id = MessageId::new();
    let session_id = SessionId::new();
    let part = |ordinal: u32, kind: PartKind, content: ContentBlock| Part {
        id: PartId::new(),
        message_id: message_id.clone(),
        ordinal,
        kind,
        content,
    };
    let request = ModelRequest {
        context: vec![],
        identity: RequestIdentity {
            session_id: session_id.clone(),
            run_id: RunId::new(),
            turn_id: TurnId::new(),
        },
        selection: selection(&model),
        system: None,
        messages: vec![Message {
            id: message_id.clone(),
            session_id,
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![
                part(
                    0,
                    PartKind::Media,
                    ContentBlock::MediaContent {
                        kind: MediaKind::Image,
                        mime_type: "image/png".into(),
                        source: MediaSource::Data {
                            base64: ONE_PIXEL_PNG.into(),
                        },
                    },
                ),
                part(
                    1,
                    PartKind::UserInputText,
                    ContentBlock::Text {
                        text: "Reply with the single word OK.".into(),
                    },
                ),
            ],
            created_at: crabber::core::Clock::now(&crabber::core::SystemClock),
        }],
        tools: vec![],
        temperature: None,
        max_tokens: Some(8192),
        tool_choice: None,
    };
    let deltas: Vec<StreamDelta> = tokio::time::timeout(LIMIT, async {
        streamer.stream(request).await.unwrap().collect().await
    })
    .await
    .expect("live image request timed out");
    let errors: Vec<_> = deltas
        .iter()
        .filter_map(|delta| match delta {
            StreamDelta::Error(error) => Some(format!("{:?}: {}", error.kind, error.message)),
            _ => None,
        })
        .collect();
    assert!(
        errors.is_empty(),
        "stream errored (a `Messages output truncated` error means the token cap went to thinking): {errors:?}"
    );
    assert!(
        deltas
            .iter()
            .any(|delta| matches!(delta, StreamDelta::TextDelta(_))),
        "no text delta"
    );
    assert!(matches!(deltas.last(), Some(StreamDelta::Completed)));
    assert!(!format!("{deltas:?}").contains(&key));
}
