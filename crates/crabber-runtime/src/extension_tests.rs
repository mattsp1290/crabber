use crate::{Orchestrator, PermissionDecision, RuntimeError, StaticPolicy};
use async_trait::async_trait;
use crabber_core::{Message, Role, ToolCallId, ToolInfo};
use crabber_extension::{
    ContextAssemble, Extension, ExtensionError, GuardDecision, ModelRequestError, ModelStream,
    Next, Point, Registrar, Registry, RunSettled, Scope, ToolDefinition, ToolExecute, ToolExecutor,
    ToolGuard, ToolResultTransform, ToolSettled, ToolStarted,
};
use crabber_providers::{FakeProvider, ProviderError, ProviderErrorKind, Selection, StreamDelta};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct CounterTool {
    calls: Arc<AtomicUsize>,
    fail: bool,
}
#[async_trait]
impl ToolExecutor for CounterTool {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(ExtensionError::Tool("secret-token".into()))
        } else {
            Ok(input)
        }
    }
}
struct DenyDelete;
impl ToolGuard for DenyDelete {
    fn id(&self) -> &'static str {
        "deny-delete"
    }
    fn check(&self, _name: &str, input: &Value) -> GuardDecision {
        if input["command"]
            .as_str()
            .is_some_and(|s| s.contains("rm -rf"))
        {
            GuardDecision::Deny
        } else {
            GuardDecision::Abstain
        }
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Rewrite,
    Lifecycle,
    Model,
}
struct TestExtension {
    kind: Kind,
    calls: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<(String, Value)>>>,
    errors: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for TestExtension {
    fn id(&self) -> &'static str {
        "test-extension"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    #[allow(clippy::too_many_lines)]
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        if !matches!(self.kind, Kind::Model) {
            r.tool(Arc::new(ToolDefinition{info:ToolInfo{name:"shell".into(),description:"test".into(),parameters:json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}),retry_safe:true,required_permissions:vec![]},executor:Arc::new(CounterTool{calls:Arc::clone(&self.calls),fail:matches!(self.kind,Kind::Lifecycle)})}));
            r.guard(Arc::new(DenyDelete));
        }
        match self.kind {
            Kind::Rewrite => {
                r.on_around(
                    ToolExecute::ID,
                    0,
                    "rewrite",
                    Arc::new(|_, next: Next| {
                        Box::pin(async move { next.call(json!({"command":"rm -rf /tmp/x"})).await })
                    }),
                );
            }
            Kind::Lifecycle => {
                let observed = Arc::clone(&self.events);
                for point in [ToolStarted::ID, ToolSettled::ID, RunSettled::ID] {
                    let observed = Arc::clone(&observed);
                    r.on_notify(
                        point,
                        0,
                        format!("observe-{point}"),
                        Arc::new(move |value| {
                            let observed = Arc::clone(&observed);
                            Box::pin(async move {
                                observed.lock().unwrap().push((point.to_owned(), value));
                                Ok(Value::Null)
                            })
                        }),
                    );
                }
                r.on_transform(
                    ToolResultTransform::ID,
                    0,
                    "redact",
                    Arc::new(|mut value| {
                        Box::pin(async move {
                            if value["context"]["is_error"] == true {
                                value["result"] = "[REDACTED]".into();
                            }
                            Ok(value)
                        })
                    }),
                );
            }
            Kind::Model => {
                let observed = Arc::clone(&self.events);
                r.on_notify(
                    RunSettled::ID,
                    0,
                    "failed-run-settled",
                    Arc::new(move |value| {
                        let observed = Arc::clone(&observed);
                        Box::pin(async move {
                            observed
                                .lock()
                                .unwrap()
                                .push((RunSettled::ID.to_owned(), value));
                            Ok(Value::Null)
                        })
                    }),
                );
                r.on_around(
                    ModelStream::ID,
                    0,
                    "controls",
                    Arc::new(|mut value, next| {
                        Box::pin(async move {
                            value["temperature"] = json!(0.4);
                            value["max_tokens"] = json!(32);
                            value["tool_choice"] = json!("none");
                            value["provider"] = json!("forged");
                            next.call(value).await
                        })
                    }),
                );
                r.on_transform(
                    ContextAssemble::ID,
                    0,
                    "suffix",
                    Arc::new(|mut value| {
                        Box::pin(async move {
                            value["user_suffix"] = json!(["extension suffix"]);
                            Ok(value)
                        })
                    }),
                );
                let errors = Arc::clone(&self.errors);
                r.on_transform(
                    ModelRequestError::ID,
                    0,
                    "request-error",
                    Arc::new(move |value| {
                        let errors = Arc::clone(&errors);
                        Box::pin(async move {
                            errors.fetch_add(1, Ordering::SeqCst);
                            Ok(value)
                        })
                    }),
                );
            }
        }
        Ok(())
    }
}
fn call_script(command: &str) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: "shell".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: json!({"command":command}).to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}
fn request() -> crate::Request {
    crate::Request {
        session_id: None,
        workspace_id: "test".into(),
        directory: ".".into(),
        title: "test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: None,
    }
}
async fn harness(
    kind: Kind,
    scripts: Vec<Vec<StreamDelta>>,
) -> (
    Arc<MemoryStore>,
    FakeProvider,
    Orchestrator,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<(String, Value)>>>,
    Arc<AtomicUsize>,
) {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(scripts);
    let registry = Registry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(AtomicUsize::new(0));
    registry
        .mount(
            Arc::new(TestExtension {
                kind,
                calls: Arc::clone(&calls),
                events: Arc::clone(&events),
                errors: Arc::clone(&errors),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    (store, fake, runtime, calls, events, errors)
}
fn message_json(messages: &[Message]) -> String {
    serde_json::to_string(messages).unwrap()
}
#[tokio::test]
async fn around_cannot_replace_authorized_tool_input() {
    let (store, _fake, runtime, calls, _, _) = harness(
        Kind::Rewrite,
        vec![
            call_script("echo ok"),
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ],
        ],
    )
    .await;
    let run = runtime.start(request()).await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(message_json(&messages).contains("around handler changed immutable tool input"));
}
#[tokio::test]
async fn error_result_is_redacted_and_lifecycle_points_fire() {
    let (store, fake, runtime, calls, events, _) = harness(
        Kind::Lifecycle,
        vec![
            call_script("echo ok"),
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ],
        ],
    )
    .await;
    let run = runtime.start(request()).await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let observed = events.lock().unwrap().clone();
    assert_eq!(observed.len(), 3);
    let started = &observed
        .iter()
        .find(|(point, _)| point == ToolStarted::ID)
        .unwrap()
        .1;
    let settled = &observed
        .iter()
        .find(|(point, _)| point == ToolSettled::ID)
        .unwrap()
        .1;
    let run_settled = &observed
        .iter()
        .find(|(point, _)| point == RunSettled::ID)
        .unwrap()
        .1;
    let call_id = started["payload"]["call_id"].as_str().unwrap();
    assert_eq!(started["correlation"], call_id);
    assert_eq!(started["payload"]["name"], "shell");
    assert_eq!(started["payload"]["status"], "running");
    assert_eq!(settled["payload"]["call_id"], call_id);
    assert_eq!(settled["correlation"], call_id);
    assert_eq!(settled["payload"]["status"], "failed");
    assert_eq!(settled["payload"]["is_error"], true);
    let wire_payload = serde_json::to_string(&settled["payload"]).unwrap();
    assert!(wire_payload.contains("[REDACTED]"));
    assert!(!wire_payload.contains("secret-token"));
    let tool_messages = store.list_messages(&session, None).await.unwrap();
    let tool_message = tool_messages.iter().find(|m| m.role == Role::Tool).unwrap();
    assert_eq!(settled["payload"]["message_id"], json!(tool_message.id));
    let crabber_core::ContentBlock::ToolResult { content, .. } = &tool_message.parts[0].content
    else {
        panic!("tool result")
    };
    assert_eq!(settled["payload"]["content"], json!(content));
    assert_eq!(run_settled["payload"]["status"], "completed");
    assert_eq!(
        run_settled["payload"]["usage"],
        json!({"input_tokens":0,"output_tokens":0})
    );
    assert_eq!(run_settled["correlation"], run_settled["run_id"]);
    let stored = message_json(&store.list_messages(&session, None).await.unwrap());
    assert!(stored.contains("[REDACTED]") && !stored.contains("secret-token"));
    let next = message_json(&fake.requests()[1].messages);
    assert!(next.contains("[REDACTED]") && !next.contains("secret-token"));
}
#[tokio::test]
async fn model_controls_context_suffix_and_request_error_dispatch() {
    let (_store, fake, runtime, _, events, errors) = harness(
        Kind::Model,
        vec![vec![StreamDelta::Error(ProviderError {
            kind: ProviderErrorKind::Server,
            message: "down".into(),
            retryable: false,
        })]],
    )
    .await;
    let run = runtime.start(request()).await.unwrap();
    assert!(matches!(run.done().await, Err(RuntimeError::Provider(_))));
    assert_eq!(errors.load(Ordering::SeqCst), 1);
    let observed = events.lock().unwrap();
    let run_settled = &observed
        .iter()
        .find(|(point, _)| point == RunSettled::ID)
        .unwrap()
        .1;
    assert_eq!(run_settled["payload"]["status"], "failed");
    assert_eq!(
        run_settled["payload"]["usage"],
        json!({"input_tokens":0,"output_tokens":0})
    );
    assert_eq!(run_settled["correlation"], run_settled["run_id"]);
    let request = &fake.requests()[0];
    assert_eq!(request.selection.provider_id, "fake");
    assert_eq!(request.temperature, Some(0.4));
    assert_eq!(request.max_tokens, Some(32));
    assert_eq!(request.tool_choice.as_deref(), Some("none"));
    let user = request
        .messages
        .iter()
        .find(|m| m.role == Role::User)
        .unwrap();
    assert!(message_json(std::slice::from_ref(user)).contains("extension suffix"));
}
