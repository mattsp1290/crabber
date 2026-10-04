use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{ContentBlock, EventKind, RunStatus, ToolCallId, ToolInfo, ToolResultStatus},
    extension::{
        Extension, InputUnavailable, Point, Registrar, Scope, ToolInput, ToolOutcomeClass,
        ToolPrepare, ToolResultContext, TransformOutput, TransformPhase,
    },
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

// This synthetic secret is created only by the executor, never by model arguments.
const SECRET: &str = "probe-executed-output-secret";
const REDACTED: &str = "[REDACTED]";

#[derive(Debug, Clone)]
struct Seen {
    tool: String,
    input: ToolInput,
    class: ToolOutcomeClass,
    is_error: bool,
    phase: TransformPhase,
    resolved: bool,
    call_id: ToolCallId,
    result: Value,
}
impl Seen {
    fn capture(context: &ToolResultContext, result: &Value) -> Self {
        Self {
            tool: context.tool_name().into(),
            input: context.input().clone(),
            class: context.class(),
            is_error: context.is_error(),
            phase: context.phase(),
            resolved: context.resolved(),
            call_id: context.call_id().clone(),
            result: result.clone(),
        }
    }
}

#[derive(Default)]
struct Observed {
    prepared: Mutex<Vec<Value>>,
    executed: Mutex<Vec<Value>>,
    reduced: Mutex<Vec<Seen>>,
    redacted: Mutex<Vec<Seen>>,
}
struct OutputTool(Arc<Observed>);
#[async_trait]
impl ToolExecutor for OutputTool {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        self.0.executed.lock().unwrap().push(input);
        Ok(json!({"secret": SECRET, "bulky": [1, 2, 3]}))
    }
}
struct Protection(Arc<Observed>);
#[async_trait]
impl Extension for Protection {
    fn id(&self) -> &'static str {
        "probe/protection"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "public-contract-v1".into()
    }

    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let observed = self.0.clone();
        registrar.on_transform(
            ToolPrepare::ID,
            0,
            "normalize",
            Arc::new(move |mut envelope| {
                let observed = observed.clone();
                Box::pin(async move {
                    observed
                        .prepared
                        .lock()
                        .unwrap()
                        .push(envelope["input"].clone());
                    if envelope["input"]["command"] == "reject" {
                        return Err(ExtensionError::Tool("probe preparation rejected".into()));
                    }
                    let command = envelope["input"]["command"]
                        .as_str()
                        .unwrap()
                        .trim()
                        .to_owned();
                    envelope["input"] = json!({"command": command});
                    Ok(envelope)
                })
            }),
        );
        let observed = self.0.clone();
        registrar.on_result_transform(
            100,
            "reduce",
            Arc::new(move |context, result| {
                let observed = observed.clone();
                Box::pin(async move {
                    observed
                        .reduced
                        .lock()
                        .unwrap()
                        .push(Seen::capture(&context, &result));
                    if context.tool_name() == "output"
                        && context.input() == &ToolInput::Normalized(json!({"command": "inspect"}))
                    {
                        return Ok(TransformOutput::marked_error(
                            json!({"secret": result["secret"], "reduced": true}),
                        ));
                    }
                    // False mark_error cannot downgrade a source error.
                    Ok(TransformOutput::new(result))
                })
            }),
        );
        let observed = self.0.clone();
        // Phase ordering must put this after the reducer despite its lower order.
        registrar.on_final_redaction(
            -100,
            "protect",
            Arc::new(move |context, mut result| {
                let observed = observed.clone();
                Box::pin(async move {
                    observed
                        .redacted
                        .lock()
                        .unwrap()
                        .push(Seen::capture(&context, &result));
                    if let Some(secret) = result.get_mut("secret") {
                        *secret = json!(REDACTED);
                    }
                    Ok(TransformOutput::new(result))
                })
            }),
        );
        Ok(())
    }
}

async fn exercise(calls: &[(&str, &str)], error_flags: &[bool]) -> Arc<Observed> {
    let observed = Arc::new(Observed::default());
    let ids: Vec<_> = calls.iter().map(|_| ToolCallId::new()).collect();
    let mut script = Vec::new();
    for ((name, command), id) in calls.iter().zip(&ids) {
        script.extend([
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: (*name).into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: json!({"command": command}).to_string(),
            },
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
        ]);
    }
    script.push(StreamDelta::Completed);
    let provider = Arc::new(FakeProvider::scripted(vec![
        script,
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]));
    let store = Arc::new(MemoryStore::new());
    let mut builder = Agent::builder()
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension(Arc::new(Protection(observed.clone())), Scope::Global);
    for name in ["output", "other-output"] {
        builder = builder.tool(Arc::new(ToolDefinition {
            info: ToolInfo { name: name.into(), description: "Synthetic output probe".into(), parameters: json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}), retry_safe: false, required_permissions: vec![] },
            executor: Arc::new(OutputTool(observed.clone())),
        }));
    }
    let agent = builder.build().unwrap();
    let run = agent
        .prompt(None, "Exercise synthetic tools")
        .await
        .unwrap();
    let session = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    // Inspect the entire request, including system, tools and every message.
    let next_request = format!("{:?}", requests[1]);
    assert!(!next_request.contains(SECRET));
    let messages = store.list_all_messages(&session).await.unwrap();
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session,
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 1000,
                text_bytes: 100_000,
                encoded_bytes: 1_000_000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("complete probe snapshot")
    };
    assert!(page.continuation.is_none());
    assert_eq!(page.tool_calls.len(), calls.len());
    let settled: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallSettled)
        .collect();
    assert_eq!(settled.len(), calls.len());
    for (id, expected_error) in ids.iter().zip(error_flags) {
        let call = page.tool_calls.iter().find(|call| &call.id == id).unwrap();
        let result = call.result.as_ref().unwrap();
        let mut protected = observed
            .redacted
            .lock()
            .unwrap()
            .iter()
            .find(|seen| &seen.call_id == id)
            .unwrap()
            .result
            .clone();
        if let Some(secret) = protected.get_mut("secret") {
            *secret = json!(REDACTED);
        }
        assert_eq!(
            result.content,
            vec![ContentBlock::Text {
                text: serde_json::to_string(&protected).unwrap()
            }]
        );
        assert_eq!(
            result.status,
            if *expected_error {
                ToolResultStatus::Failed
            } else {
                ToolResultStatus::Completed
            }
        );
        let tool_message = messages
            .iter()
            .flat_map(|message| &message.parts)
            .find_map(|part| match &part.content {
                ContentBlock::ToolResult {
                    call_id,
                    content,
                    is_error,
                } if call_id == id => Some((content, is_error)),
                _ => None,
            })
            .unwrap();
        assert_eq!(tool_message.0, &result.content);
        assert_eq!(*tool_message.1, *expected_error);
        let event = settled
            .iter()
            .find(|event| event.payload["call_id"] == json!(id))
            .unwrap();
        assert_eq!(event.payload["content"], json!(result.content));
        assert_eq!(event.payload["is_error"], *expected_error);
        assert!(!event.live_only);
        assert!(event.cursor.is_some());
        let returned = requests[1]
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .find_map(|part| match &part.content {
                ContentBlock::ToolResult {
                    call_id,
                    content,
                    is_error,
                } if call_id == id => Some((content, is_error)),
                _ => None,
            })
            .unwrap();
        assert_eq!(returned, tool_message);
    }
    for persisted in [
        serde_json::to_string(&page).unwrap(),
        serde_json::to_string(&messages).unwrap(),
        serde_json::to_string(&events).unwrap(),
    ] {
        assert!(
            !persisted.contains(SECRET),
            "executed secret escaped durable protection"
        );
    }
    agent.close_extensions().await.unwrap();
    observed
}

#[tokio::test]
async fn exact_normalized_binding_reducer_then_final_redactor_protects_next_request() {
    let observed = exercise(
        &[
            ("output", " inspect "),
            ("output", "different"),
            ("other-output", " inspect "),
        ],
        &[true, false, false],
    )
    .await;
    assert_eq!(
        *observed.prepared.lock().unwrap(),
        [
            json!({"command":" inspect "}),
            json!({"command":"different"}),
            json!({"command":" inspect "})
        ]
    );
    assert_eq!(
        *observed.executed.lock().unwrap(),
        [
            json!({"command":"inspect"}),
            json!({"command":"different"}),
            json!({"command":"inspect"})
        ]
    );
    let reduced = observed.reduced.lock().unwrap();
    let redacted = observed.redacted.lock().unwrap();
    assert_eq!(reduced.len(), 3);
    assert_eq!(redacted.len(), 3);
    assert_eq!(
        reduced
            .iter()
            .map(|seen| (seen.tool.as_str(), seen.input.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                "output",
                ToolInput::Normalized(json!({"command":"inspect"}))
            ),
            (
                "output",
                ToolInput::Normalized(json!({"command":"different"}))
            ),
            (
                "other-output",
                ToolInput::Normalized(json!({"command":"inspect"}))
            )
        ]
    );
    for first in reduced.iter() {
        let last = redacted
            .iter()
            .find(|last| last.call_id == first.call_id)
            .unwrap();
        assert!(first.resolved && last.resolved);
        assert_eq!(first.class, ToolOutcomeClass::Succeeded);
        assert_eq!(last.class, first.class);
        assert_eq!(last.tool, first.tool);
        assert_eq!(last.input, first.input);
        assert_eq!(first.phase, TransformPhase::Ordinary);
        assert_eq!(last.phase, TransformPhase::FinalRedaction);
        assert!(!first.is_error);
        assert_eq!(first.result, json!({"secret":SECRET,"bulky":[1,2,3]}));
        let matches = first.tool == "output"
            && first.input == ToolInput::Normalized(json!({"command":"inspect"}));
        assert_eq!(last.is_error, matches);
        assert_eq!(
            last.result,
            if matches {
                json!({"secret":SECRET,"reduced":true})
            } else {
                first.result.clone()
            }
        );
    }
    assert_eq!(
        reduced
            .iter()
            .filter(|seen| seen.tool == "output"
                && seen.input == ToolInput::Normalized(json!({"command":"inspect"})))
            .count(),
        1
    );
}

#[tokio::test]
async fn prepare_failure_reports_unavailable_input_and_cannot_downgrade_error() {
    let observed = exercise(&[("output", "reject")], &[true]).await;
    assert_eq!(
        *observed.prepared.lock().unwrap(),
        [json!({"command":"reject"})]
    );
    assert!(observed.executed.lock().unwrap().is_empty());
    for (seen, phase) in [
        (&observed.reduced, TransformPhase::Ordinary),
        (&observed.redacted, TransformPhase::FinalRedaction),
    ] {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].tool, "output");
        assert!(seen[0].resolved);
        assert_eq!(
            seen[0].input,
            ToolInput::Unavailable {
                reason: InputUnavailable::PrepareFailed
            }
        );
        assert_eq!(seen[0].class, ToolOutcomeClass::PrepareFailed);
        assert!(seen[0].is_error);
        assert_eq!(seen[0].phase, phase);
    }
}
