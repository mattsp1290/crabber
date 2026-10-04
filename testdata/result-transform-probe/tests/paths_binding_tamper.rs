//! Public-facade proofs for the error outcome paths, parallel call
//! association and tamper resistance of the result-transform contract.

use async_trait::async_trait;
use crabber::{
    Agent, AgentBuilder, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection,
    StaticPolicy, StreamDelta, ToolDefinition, ToolExecutor,
    core::{
        ContentBlock, EventKind, Message, RunId, RunStatus, SessionId, ToolCallId, ToolCallRecord,
        ToolInfo, ToolResultStatus,
    },
    extension::{
        Extension, Point, Registrar, Scope, ToolInput, ToolOutcomeClass, ToolPrepare,
        ToolResultContext, ToolResultTransform, TransformOutput, TransformPhase,
        result_transform_failed_message,
    },
    runtime::ExecutionMode,
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

// This synthetic secret is created only by the executor, never by model arguments.
const SECRET: &str = "probe-executed-output-secret";
// One outer bound per test: a parked handler must fail, never hang the suite.
const BOUND: Duration = Duration::from_secs(10);
const INTERMEDIATE: &str = "probe-intermediate-marker";
const AUTHORED: &str = "probe-authored-marker";

type Install = Arc<dyn Fn(&mut Registrar) + Send + Sync>;
struct Mounted(&'static str, Install);
#[async_trait]
impl Extension for Mounted {
    fn id(&self) -> &'static str {
        self.0
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.0.into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        (self.1)(registrar);
        Ok(())
    }
}

type Observation = (ToolResultContext, Value);
#[derive(Default)]
struct Recorder {
    ordinary: Mutex<Vec<Observation>>,
    finals: Mutex<Vec<Observation>>,
    executed: Mutex<Vec<(String, Value)>>,
}
impl Recorder {
    /// Pass-through handlers in both phases that only record what they saw,
    /// plus a `ToolPrepare` rewrite so normalized input differs from the
    /// provider's raw arguments.
    fn extension(self: &Arc<Self>) -> Arc<Mounted> {
        let recorder = self.clone();
        Arc::new(Mounted(
            "probe/recorder",
            Arc::new(move |registrar| {
                registrar.on_transform(
                    ToolPrepare::ID,
                    0,
                    "trim-command",
                    Arc::new(|mut envelope| {
                        Box::pin(async move {
                            if let Some(command) = envelope["input"]["command"].as_str() {
                                envelope["input"]["command"] = json!(command.trim());
                            }
                            Ok(envelope)
                        })
                    }),
                );
                let seen = recorder.clone();
                registrar.on_result_transform(
                    0,
                    "record",
                    Arc::new(move |context, result| {
                        seen.ordinary
                            .lock()
                            .unwrap()
                            .push((context, result.clone()));
                        Box::pin(async move { Ok(TransformOutput::new(result)) })
                    }),
                );
                let seen = recorder.clone();
                registrar.on_final_redaction(
                    0,
                    "record-final",
                    Arc::new(move |context, result| {
                        seen.finals.lock().unwrap().push((context, result.clone()));
                        Box::pin(async move { Ok(TransformOutput::new(result)) })
                    }),
                );
            }),
        ))
    }
}

struct NamedTool {
    name: &'static str,
    fail: bool,
    recorder: Arc<Recorder>,
}
#[async_trait]
impl ToolExecutor for NamedTool {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        self.recorder
            .executed
            .lock()
            .unwrap()
            .push((self.name.into(), input));
        if self.fail {
            return Err(ExtensionError::Tool("probe execution failed".into()));
        }
        Ok(json!({"secret": SECRET}))
    }
}
fn named_tool(name: &'static str, fail: bool, recorder: &Arc<Recorder>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "Synthetic path probe".into(),
            parameters: json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(NamedTool {
            name,
            fail,
            recorder: recorder.clone(),
        }),
    })
}

/// One model turn requesting every call, then a closing text turn.
fn scripted(calls: &[(ToolCallId, &str, Value)]) -> Arc<FakeProvider> {
    let mut turn = Vec::new();
    for (id, name, arguments) in calls {
        turn.extend([
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: (*name).into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: arguments.to_string(),
            },
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
        ]);
    }
    turn.push(StreamDelta::Completed);
    Arc::new(FakeProvider::scripted(vec![
        turn,
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]))
}

fn builder(
    store: &Arc<MemoryStore>,
    provider: &Arc<FakeProvider>,
    policy: StaticPolicy,
) -> AgentBuilder {
    Agent::builder()
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(policy))
}

fn tool_result_part(messages: &[Message], id: &ToolCallId) -> (Vec<ContentBlock>, bool) {
    messages
        .iter()
        .flat_map(|message| &message.parts)
        .find_map(|part| match &part.content {
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } if call_id == id => Some((content.clone(), *is_error)),
            _ => None,
        })
        .expect("tool result part for the call")
}

/// Everything durable or provider-visible after a completed two-request run.
struct Durable {
    calls: Vec<ToolCallRecord>,
    /// Serialized snapshot, messages, events and the full next provider request.
    artifacts: Vec<String>,
}
impl Durable {
    async fn collect(store: &MemoryStore, provider: &FakeProvider, session: &SessionId) -> Self {
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        let messages = store.list_all_messages(session).await.unwrap();
        let events = store.list_events(session, None, 1000).await.unwrap();
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: session.clone(),
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
        // The result, the message, the event and the next request hold one value.
        for call in &page.tool_calls {
            let result = call.result.as_ref().expect("settled call");
            let is_error = result.status != ToolResultStatus::Completed;
            let message = tool_result_part(&messages, &call.id);
            assert_eq!(message, (result.content.clone(), is_error));
            assert_eq!(tool_result_part(&requests[1].messages, &call.id), message);
            let event = events
                .iter()
                .find(|event| {
                    event.kind == EventKind::ToolCallSettled
                        && event.payload["call_id"] == json!(call.id)
                })
                .expect("durable settlement event");
            assert_eq!(event.payload["content"], json!(result.content));
            assert_eq!(event.payload["is_error"], is_error);
            assert!(!event.live_only);
            assert!(event.cursor.is_some());
        }
        Self {
            artifacts: vec![
                serde_json::to_string(&page).unwrap(),
                serde_json::to_string(&messages).unwrap(),
                serde_json::to_string(&events).unwrap(),
                format!("{:?}", requests[1]),
            ],
            calls: page.tool_calls,
        }
    }
    fn call(&self, id: &ToolCallId) -> &ToolCallRecord {
        self.calls.iter().find(|call| &call.id == id).unwrap()
    }
    fn assert_absent(&self, marker: &str) {
        for artifact in &self.artifacts {
            assert!(!artifact.contains(marker), "{marker} escaped: {artifact}");
        }
    }
}

fn text(value: &Value) -> Vec<ContentBlock> {
    vec![ContentBlock::Text {
        text: serde_json::to_string(value).unwrap(),
    }]
}

struct PathRun {
    recorder: Arc<Recorder>,
    durable: Durable,
    call: ToolCallId,
    session: SessionId,
    run: RunId,
}
/// Runs one scripted call against three registered tools: `output` succeeds,
/// `failing` returns an executor error and `denied` is denied by policy.
async fn run_path(tool: &'static str, arguments: Value) -> PathRun {
    let recorder = Arc::new(Recorder::default());
    let call = ToolCallId::new();
    let provider = scripted(&[(call.clone(), tool, arguments)]);
    let store = Arc::new(MemoryStore::new());
    let policy =
        StaticPolicy::new(PermissionDecision::Allow).with_rule("denied", PermissionDecision::Deny);
    let agent = builder(&store, &provider, policy)
        .tool(named_tool("output", false, &recorder))
        .tool(named_tool("failing", true, &recorder))
        .tool(named_tool("denied", false, &recorder))
        .extension(recorder.extension(), Scope::Global)
        .build()
        .unwrap();
    let handle = agent.prompt(None, "Exercise one path").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let durable = Durable::collect(&store, &provider, &session).await;
    agent.close_extensions().await.unwrap();
    PathRun {
        recorder,
        durable,
        call,
        session,
        run,
    }
}

/// Asserts both phases saw the same authoritative context and the unchanged
/// seed, and that the durable record is the failed form of that seed.
fn assert_error_path(
    path: &PathRun,
    tool: &str,
    resolved: bool,
    class: ToolOutcomeClass,
    input: &ToolInput,
) -> Value {
    let ordinary = path.recorder.ordinary.lock().unwrap();
    let finals = path.recorder.finals.lock().unwrap();
    assert_eq!((ordinary.len(), finals.len()), (1, 1));
    for ((context, _), phase) in [
        (&ordinary[0], TransformPhase::Ordinary),
        (&finals[0], TransformPhase::FinalRedaction),
    ] {
        assert_eq!(context.tool_name(), tool);
        assert_eq!(context.resolved(), resolved);
        assert_eq!(context.input(), input);
        assert_eq!(context.class(), class);
        assert!(context.is_error());
        assert_eq!(context.phase(), phase);
        assert_eq!(context.call_id(), &path.call);
        assert_eq!(context.session_id(), &path.session);
        assert_eq!(context.run_id(), &path.run);
    }
    let seed = ordinary[0].1.clone();
    assert_eq!(finals[0].1, seed);
    let record = path.durable.call(&path.call);
    assert_eq!(record.name, tool);
    assert_eq!(record.run_id, path.run);
    // A resolved call's record holds the normalized input the handlers saw.
    // An unknown tool's record uses its own stored form for raw arguments.
    if let ToolInput::Normalized(arguments) = input {
        assert_eq!(&record.arguments, arguments);
    }
    let result = record.result.as_ref().unwrap();
    // Pass-through handlers returned `mark_error: false`; the class still wins.
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.content, text(&seed));
    seed
}

#[tokio::test]
async fn execution_error_keeps_class_and_normalized_input() {
    timeout(BOUND, async {
        // The provider sends untrimmed text; `ToolPrepare` normalizes it.
        let raw = json!({"command": " inspect "});
        let arguments = json!({"command": "inspect"});
        let path = run_path("failing", raw).await;
        let seed = assert_error_path(
            &path,
            "failing",
            true,
            ToolOutcomeClass::ExecutionFailed,
            &ToolInput::Normalized(arguments.clone()),
        );
        assert!(seed.as_str().unwrap().contains("probe execution failed"));
        assert_eq!(
            *path.recorder.executed.lock().unwrap(),
            [("failing".to_owned(), arguments)]
        );
    })
    .await
    .expect("execution-error path bound");
}

#[tokio::test]
async fn permission_denial_keeps_class_and_never_executes() {
    timeout(BOUND, async {
        // The provider sends untrimmed text; `ToolPrepare` normalizes it.
        let raw = json!({"command": " inspect "});
        let arguments = json!({"command": "inspect"});
        let path = run_path("denied", raw).await;
        let seed = assert_error_path(
            &path,
            "denied",
            true,
            ToolOutcomeClass::PermissionDenied,
            &ToolInput::Normalized(arguments),
        );
        assert_eq!(seed, json!("permission denied"));
        assert!(path.recorder.executed.lock().unwrap().is_empty());
        path.durable.assert_absent(SECRET);
    })
    .await
    .expect("permission-denial path bound");
}

#[tokio::test]
async fn unknown_tool_reports_unresolved_name_and_raw_input() {
    timeout(BOUND, async {
        // Unknown tools are never prepared: the raw text stays untrimmed.
        let arguments = json!({"command": " inspect "});
        let path = run_path("missing", arguments.clone()).await;
        let seed = assert_error_path(
            &path,
            "missing",
            false,
            ToolOutcomeClass::UnknownTool,
            &ToolInput::Raw(arguments),
        );
        assert_eq!(seed, json!("unknown tool: missing"));
        assert!(path.recorder.executed.lock().unwrap().is_empty());
    })
    .await
    .expect("unknown-tool path bound");
}

#[derive(Default)]
struct Gate {
    waiting: Mutex<HashMap<String, oneshot::Sender<()>>>,
    contexts: Mutex<Vec<ToolResultContext>>,
}
fn own_result(context: &ToolResultContext) -> Value {
    let ToolInput::Normalized(input) = context.input() else {
        panic!("resolved probe tools have normalized input")
    };
    json!({"call": context.call_id().to_string(), "tool": context.tool_name(), "input": input})
}

#[tokio::test]
async fn parallel_calls_keep_their_own_tool_input_and_ids_under_reverse_completion() {
    timeout(BOUND, async {
        let recorder = Arc::new(Recorder::default());
        let gate = Arc::new(Gate::default());
        let (entered_tx, mut entered) = mpsc::unbounded_channel();
        let (completed_tx, mut completed) = mpsc::unbounded_channel();
        let parked = gate.clone();
        let parking = Arc::new(Mounted(
            "probe/parking",
            Arc::new(move |registrar| {
                let gate = parked.clone();
                let entered = entered_tx.clone();
                let completed = completed_tx.clone();
                registrar.on_result_transform(
                    0,
                    "park",
                    Arc::new(move |context, _| {
                        let gate = gate.clone();
                        let entered = entered.clone();
                        let completed = completed.clone();
                        Box::pin(async move {
                            let (release, released) = oneshot::channel();
                            let id = context.call_id().clone();
                            gate.waiting.lock().unwrap().insert(id.to_string(), release);
                            gate.contexts.lock().unwrap().push(context.clone());
                            let _ = entered.send(id.clone());
                            released
                                .await
                                .map_err(|_| ExtensionError::Tool("probe gate dropped".into()))?;
                            let _ = completed.send(id);
                            Ok(TransformOutput::new(own_result(&context)))
                        })
                    }),
                );
            }),
        ));
        let calls = [
            (ToolCallId::new(), "output", json!({"command": "inspect"})),
            (ToolCallId::new(), "output", json!({"command": "different"})),
            (
                ToolCallId::new(),
                "other-output",
                json!({"command": "inspect"}),
            ),
        ];
        let provider = scripted(&calls);
        let store = Arc::new(MemoryStore::new());
        let agent = builder(
            &store,
            &provider,
            StaticPolicy::new(PermissionDecision::Allow),
        )
        .execution_mode(ExecutionMode::Parallel { max: calls.len() })
        .tool(named_tool("output", false, &recorder))
        .tool(named_tool("other-output", false, &recorder))
        .extension(parking, Scope::Global)
        .build()
        .unwrap();
        let handle = agent.prompt(None, "Exercise parallel calls").await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();

        // All three handlers are in flight before any is released.
        let mut in_flight = Vec::new();
        for _ in &calls {
            in_flight.push(entered.recv().await.unwrap());
        }
        assert_eq!(gate.waiting.lock().unwrap().len(), calls.len());
        for (id, _, _) in &calls {
            assert!(in_flight.contains(id));
        }
        // Completion order is the reverse of request order.
        for (id, _, _) in calls.iter().rev() {
            let release = gate
                .waiting
                .lock()
                .unwrap()
                .remove(&id.to_string())
                .unwrap();
            release.send(()).unwrap();
            assert_eq!(&completed.recv().await.unwrap(), id);
        }
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);

        let durable = Durable::collect(&store, &provider, &session).await;
        assert_eq!(durable.calls.len(), calls.len());
        {
            let contexts = gate.contexts.lock().unwrap();
            assert_eq!(contexts.len(), calls.len());
            for (id, name, arguments) in &calls {
                let record = durable.call(id);
                let matching: Vec<_> = contexts
                    .iter()
                    .filter(|context| context.call_id() == id)
                    .collect();
                assert_eq!(matching.len(), 1);
                let context = matching[0];
                assert_eq!(
                    (record.name.as_str(), &record.arguments),
                    (*name, arguments)
                );
                assert_eq!(context.tool_name(), record.name);
                assert_eq!(
                    context.input(),
                    &ToolInput::Normalized(record.arguments.clone())
                );
                assert_eq!(context.run_id(), &record.run_id);
                assert_eq!(context.run_id(), &run);
                assert_eq!(context.session_id(), &session);
                assert!(context.resolved());
                assert_eq!(context.class(), ToolOutcomeClass::Succeeded);
                // Each call persisted the value produced for itself, not a sibling's.
                let result = record.result.as_ref().unwrap();
                assert_eq!(result.status, ToolResultStatus::Completed);
                assert_eq!(result.content, text(&own_result(context)));
            }
            let bound = ToolInput::Normalized(json!({"command": "inspect"}));
            assert_eq!(
                contexts
                    .iter()
                    .filter(|context| context.tool_name() == "output" && context.input() == &bound)
                    .count(),
                1
            );
        }
        agent.close_extensions().await.unwrap();
    })
    .await
    .expect("parallel association bound");
}

#[derive(Clone)]
enum Tamper {
    Context(&'static str, Value),
    ExtraKey,
    /// Control: an honest envelope whose authored result must persist.
    Honest,
}

#[tokio::test]
async fn tampered_envelope_fails_closed_with_fixed_text() {
    const HANDLER: &str = "tamper";
    let cases = [
        Tamper::Context("tool_name", json!("other-output")),
        Tamper::Context("resolved", json!(false)),
        Tamper::Context("input", json!({"kind": "raw", "value": AUTHORED})),
        Tamper::Context("call_id", json!(ToolCallId::new().to_string())),
        Tamper::Context("session_id", json!(SessionId::new().to_string())),
        Tamper::Context("run_id", json!(RunId::new().to_string())),
        Tamper::Context("class", json!("permission_denied")),
        Tamper::Context("is_error", json!(true)),
        Tamper::Context("phase", json!("final_redaction")),
        Tamper::ExtraKey,
        Tamper::Honest,
    ];
    timeout(BOUND, async {
        for case in cases {
            let recorder = Arc::new(Recorder::default());
            let later = Arc::new(AtomicUsize::new(0));
            let finals = Arc::new(AtomicUsize::new(0));
            let received = Arc::new(Mutex::new(Vec::new()));
            let seen_by_tamper = received.clone();
            let label = match &case {
                Tamper::Context(field, _) => format!("context.{field}"),
                Tamper::ExtraKey => "extra top-level key".into(),
                Tamper::Honest => "honest control".into(),
            };
            let (tamper, later_ran, finals_ran) = (case.clone(), later.clone(), finals.clone());
            let chain = Arc::new(Mounted(
                "probe/tamper-chain",
                Arc::new(move |registrar| {
                    registrar.on_transform(
                        ToolResultTransform::ID,
                        1,
                        "accept-intermediate",
                        Arc::new(|mut envelope| {
                            Box::pin(async move {
                                envelope["result"] = json!(INTERMEDIATE);
                                Ok(envelope)
                            })
                        }),
                    );
                    let tamper = tamper.clone();
                    let received = seen_by_tamper.clone();
                    registrar.on_transform(
                        ToolResultTransform::ID,
                        2,
                        HANDLER,
                        Arc::new(move |mut envelope| {
                            let tamper = tamper.clone();
                            let received = received.clone();
                            Box::pin(async move {
                                received.lock().unwrap().push(envelope["result"].clone());
                                envelope["result"] = json!(AUTHORED);
                                match tamper {
                                    Tamper::Context(field, value) => {
                                        envelope["context"][field] = value;
                                    }
                                    Tamper::ExtraKey => envelope["is_error"] = json!(false),
                                    Tamper::Honest => {}
                                }
                                Ok(envelope)
                            })
                        }),
                    );
                    let ran = later_ran.clone();
                    registrar.on_result_transform(
                        3,
                        "later",
                        Arc::new(move |_, result| {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Box::pin(async move { Ok(TransformOutput::new(result)) })
                        }),
                    );
                    let ran = finals_ran.clone();
                    registrar.on_final_redaction(
                        0,
                        "final",
                        Arc::new(move |_, result| {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Box::pin(async move { Ok(TransformOutput::new(result)) })
                        }),
                    );
                }),
            ));
            let call = ToolCallId::new();
            let provider = scripted(&[(call.clone(), "output", json!({"command": "inspect"}))]);
            let store = Arc::new(MemoryStore::new());
            let agent = builder(
                &store,
                &provider,
                StaticPolicy::new(PermissionDecision::Allow),
            )
            .tool(named_tool("output", false, &recorder))
            .extension(chain, Scope::Global)
            .build()
            .unwrap();
            let handle = agent.prompt(None, "Exercise tampering").await.unwrap();
            let session = handle.session_id().clone();
            let run = handle.run_id().clone();
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
            let durable = Durable::collect(&store, &provider, &session).await;
            agent.close_extensions().await.unwrap();

            // Identity is durable and never taken from a callback return.
            let record = durable.call(&call);
            assert_eq!(record.name, "output");
            assert_eq!(record.run_id, run);
            assert_eq!(recorder.executed.lock().unwrap().len(), 1);
            let result = record.result.as_ref().unwrap();
            durable.assert_absent(SECRET);
            durable.assert_absent(INTERMEDIATE);
            // The intermediate value was accepted before the tamper handler ran,
            // so its absence below is a real protection, not an unreached value.
            assert_eq!(*received.lock().unwrap(), [json!(INTERMEDIATE)], "{label}");
            let ran = (later.load(Ordering::SeqCst), finals.load(Ordering::SeqCst));
            if matches!(case, Tamper::Honest) {
                assert_eq!(result.status, ToolResultStatus::Completed, "{label}");
                assert_eq!(result.content, text(&json!(AUTHORED)), "{label}");
                assert_eq!(ran, (1, 1), "{label}");
            } else {
                assert_eq!(result.status, ToolResultStatus::Failed, "{label}");
                assert_eq!(
                    result.content,
                    text(&json!(result_transform_failed_message(HANDLER))),
                    "{label}"
                );
                // The chain stops: no later handler and no final redactor runs.
                assert_eq!(ran, (0, 0), "{label}");
                durable.assert_absent(AUTHORED);
            }
        }
    })
    .await
    .expect("tamper cases bound");
}
