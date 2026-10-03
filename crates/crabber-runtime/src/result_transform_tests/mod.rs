//! Shared harness for the tool-result-transform tests, plus characterization of today's contract.
//!
//! Each later slice fills exactly one submodule so the slices never share a file. The harness
//! records the raw `Value` every handler receives, so switching it to the typed context API is a
//! change in one place.
// Helpers below are used by the later slices named on each submodule; until they land, some have
// no caller.
#![allow(dead_code)]

mod binding;
mod cancel;
mod compose;
mod paths;
mod recovery;
mod tamper;

use crate::{
    ApprovalRequester, Orchestrator, PermissionDecision, PermissionPolicy, Request, RunHandle,
};
use async_trait::async_trait;
use crabber_core::{
    ContentBlock, EventKind, EventRecord, Message, Role, RunId, SessionId, ToolCallId,
    ToolCallRecord, ToolCallStatus, ToolInfo,
};
use crabber_extension::{
    Extension, ExtensionError, Point, Registrar, Registry, Scope, ToolDefinition, ToolExecutor,
    ToolPrepare, ToolResultTransform,
};
use crabber_providers::{FakeProvider, ModelRequest, Selection, StreamDelta};
use crabber_session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// Tool whose executor echoes its input.
pub(super) const ECHO: &str = "echo";
/// Second succeeding tool, so one turn can call two different tools.
pub(super) const SHOUT: &str = "shout";
/// Tool whose executor always returns `Err`.
pub(super) const FAIL: &str = "fail";
/// Tool the harness policy denies before it executes.
pub(super) const FORBIDDEN: &str = "forbidden";
/// Tool name the plan does not contain.
pub(super) const MISSING: &str = "missing";
/// `text` value a `ToolPrepare` handler rejects.
pub(super) const PREPARE_REJECTED: &str = "reject-in-prepare";
/// Message the failing executor reports.
pub(super) const EXECUTOR_ERROR: &str = "executor exploded";

struct TextTool {
    name: &'static str,
}
#[async_trait]
impl ToolExecutor for TextTool {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        match self.name {
            FAIL => Err(ExtensionError::Tool(EXECUTOR_ERROR.into())),
            SHOUT => {
                Ok(json!({"shouted": input["text"].as_str().unwrap_or_default().to_uppercase()}))
            }
            _ => Ok(input),
        }
    }
}

/// Handler that parks until released; later cancellation slices drive it.
#[derive(Default)]
pub(super) struct Gate {
    /// Notified once a handler has reached the gate.
    pub(super) entered: Notify,
    /// Notify to let the parked handler continue.
    pub(super) release: Notify,
}

/// Everything the recording extension saw, as the raw payloads handlers receive.
#[derive(Default)]
pub(super) struct Probe {
    /// Payload of every `ToolResultTransform` invocation, in order.
    pub(super) results: Mutex<Vec<Value>>,
    /// Payload of every `ToolPrepare` invocation, in order.
    pub(super) prepares: Mutex<Vec<Value>>,
    /// When set, the result handler parks here after recording.
    pub(super) gate: Mutex<Option<Arc<Gate>>>,
}
impl Probe {
    pub(super) fn results(&self) -> Vec<Value> {
        self.results.lock().unwrap().clone()
    }
    pub(super) fn prepares(&self) -> Vec<Value> {
        self.prepares.lock().unwrap().clone()
    }
}

/// Records what each result and prepare handler receives and leaves the payload unchanged,
/// except that it rejects a `ToolPrepare` whose input text is [`PREPARE_REJECTED`].
struct RecordingExtension {
    probe: Arc<Probe>,
}
#[async_trait]
impl Extension for RecordingExtension {
    fn id(&self) -> &'static str {
        "result-transform-recorder"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        for name in [ECHO, SHOUT, FAIL, FORBIDDEN] {
            r.tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: name.into(),
                    description: "test".into(),
                    parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
                    retry_safe: true,
                    required_permissions: vec![],
                },
                executor: Arc::new(TextTool { name }),
            }));
        }
        let probe = Arc::clone(&self.probe);
        r.on_transform(
            ToolPrepare::ID,
            0,
            "record-prepare",
            Arc::new(move |value| {
                let probe = Arc::clone(&probe);
                Box::pin(async move {
                    probe.prepares.lock().unwrap().push(value.clone());
                    if value["input"]["text"] == PREPARE_REJECTED {
                        return Err(ExtensionError::Tool("prepare handler rejected".into()));
                    }
                    Ok(value)
                })
            }),
        );
        let probe = Arc::clone(&self.probe);
        r.on_transform(
            ToolResultTransform::ID,
            0,
            "record-result",
            Arc::new(move |value| {
                let probe = Arc::clone(&probe);
                Box::pin(async move {
                    probe.results.lock().unwrap().push(value.clone());
                    let gate = probe.gate.lock().unwrap().clone();
                    if let Some(gate) = gate {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                    }
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}

/// Denies [`FORBIDDEN`] and allows every other tool.
struct DenyForbidden;
impl PermissionPolicy for DenyForbidden {
    fn decide(&self, tool: &ToolInfo, _arguments: &Value) -> PermissionDecision {
        if tool.name == FORBIDDEN {
            PermissionDecision::Deny
        } else {
            PermissionDecision::Allow
        }
    }
}

/// Asks for [`FORBIDDEN`] and refuses; another route to the denial path.
struct AskAndRefuse;
impl PermissionPolicy for AskAndRefuse {
    fn decide(&self, tool: &ToolInfo, _arguments: &Value) -> PermissionDecision {
        if tool.name == FORBIDDEN {
            PermissionDecision::Ask
        } else {
            PermissionDecision::Allow
        }
    }
}
struct Refuse;
#[async_trait]
impl ApprovalRequester for Refuse {
    async fn approve(&self, _tool: &ToolInfo, _arguments: &Value) -> bool {
        false
    }
}

/// How the harness decides permission for [`FORBIDDEN`].
#[derive(Clone, Copy)]
pub(super) enum Denial {
    /// The policy returns `Deny`.
    Policy,
    /// The policy returns `Ask` and the approver refuses.
    RefusedApproval,
}

/// One tool call a scripted provider turn makes.
pub(super) struct ScriptedCall {
    pub(super) id: ToolCallId,
    pub(super) name: &'static str,
    /// Argument JSON exactly as the provider streams it.
    pub(super) arguments: String,
}
impl ScriptedCall {
    pub(super) fn new(name: &'static str, arguments: &Value) -> Self {
        Self {
            id: ToolCallId::new(),
            name,
            arguments: arguments.to_string(),
        }
    }
    /// Call with a `{"text": text}` input, which every harness tool accepts.
    pub(super) fn text(name: &'static str, text: &str) -> Self {
        Self::new(name, &json!({"text": text}))
    }
}

/// Provider turn that makes every given call in order.
pub(super) fn calls_turn(calls: &[&ScriptedCall]) -> Vec<StreamDelta> {
    let mut deltas = Vec::new();
    for call in calls {
        deltas.push(StreamDelta::ToolCallStart {
            call_id: call.id.clone(),
            name: call.name.into(),
        });
        deltas.push(StreamDelta::ToolCallArgsDelta {
            call_id: call.id.clone(),
            text: call.arguments.clone(),
        });
        deltas.push(StreamDelta::ToolCallDone {
            call_id: call.id.clone(),
        });
    }
    deltas.push(StreamDelta::Completed);
    deltas
}

/// Provider turn that only answers with text and ends the run.
pub(super) fn final_turn() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}

/// Script of one tool-calling turn followed by the closing text turn.
pub(super) fn one_turn(calls: &[&ScriptedCall]) -> Vec<Vec<StreamDelta>> {
    vec![calls_turn(calls), final_turn()]
}

fn request() -> Request {
    Request {
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
    }
}

/// A runtime wired to the recording extension, a scripted provider and an in-memory store.
pub(super) struct Harness {
    pub(super) store: Arc<MemoryStore>,
    pub(super) fake: FakeProvider,
    pub(super) runtime: Orchestrator,
    pub(super) probe: Arc<Probe>,
}
impl Harness {
    /// Every tool allowed except [`FORBIDDEN`], which the policy denies.
    pub(super) async fn new(scripts: Vec<Vec<StreamDelta>>) -> Self {
        Self::with_denial(scripts, Denial::Policy).await
    }
    pub(super) async fn with_denial(scripts: Vec<Vec<StreamDelta>>, denial: Denial) -> Self {
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(scripts);
        let probe = Arc::new(Probe::default());
        let registry = Registry::new();
        registry
            .mount(
                Arc::new(RecordingExtension {
                    probe: Arc::clone(&probe),
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        let builder = Orchestrator::builder()
            .store(Arc::clone(&store) as Arc<dyn Store>)
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(registry));
        let builder = match denial {
            Denial::Policy => builder.policy(Arc::new(DenyForbidden)),
            Denial::RefusedApproval => builder
                .policy(Arc::new(AskAndRefuse))
                .approver(Arc::new(Refuse)),
        };
        let runtime = builder.build().unwrap();
        Self {
            store,
            fake,
            runtime,
            probe,
        }
    }
    /// Install a gate that parks every result handler after it records its payload.
    pub(super) fn block_results(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.probe.gate.lock().unwrap() = Some(Arc::clone(&gate));
        gate
    }
    /// Start a run without waiting for it (for cancellation tests).
    pub(super) async fn start(&self) -> RunHandle {
        self.runtime.start(request()).await.unwrap()
    }
    /// Run to completion and return handles for reading everything back.
    pub(super) async fn run(&self) -> Finished<'_> {
        let handle = self.start().await;
        let session_id = handle.session_id().clone();
        let run_id = handle.run_id().clone();
        handle.done().await.unwrap();
        Finished {
            harness: self,
            session_id,
            run_id,
        }
    }
}

/// A finished run, with read-backs keyed by call ID.
pub(super) struct Finished<'a> {
    pub(super) harness: &'a Harness,
    pub(super) session_id: SessionId,
    pub(super) run_id: RunId,
}
impl Finished<'_> {
    /// The durable call record, including its persisted `ToolResult`.
    pub(super) async fn record(&self, call: &ToolCallId) -> ToolCallRecord {
        let outcome = self
            .harness
            .store
            .snapshot(SnapshotRequest {
                session_id: self.session_id.clone(),
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 1000,
                    text_bytes: 1 << 20,
                    encoded_bytes: 1 << 22,
                },
                continuation: None,
            })
            .await
            .unwrap();
        let SnapshotOutcome::Page(page) = outcome else {
            panic!("snapshot did not fit one page")
        };
        page.tool_calls
            .into_iter()
            .find(|record| &record.id == call)
            .expect("call record persisted")
    }
    /// The persisted tool message for the call: its text and `is_error`.
    pub(super) async fn tool_message(&self, call: &ToolCallId) -> (Message, String, bool) {
        let messages = self
            .harness
            .store
            .list_messages(&self.session_id, None)
            .await
            .unwrap();
        messages
            .into_iter()
            .filter(|message| message.role == Role::Tool)
            .find_map(|message| {
                let found = message.parts.iter().find_map(|part| match &part.content {
                    ContentBlock::ToolResult {
                        call_id,
                        content,
                        is_error,
                    } if call_id == call => Some((tool_text(content), *is_error)),
                    _ => None,
                })?;
                Some((message, found.0, found.1))
            })
            .expect("tool message persisted")
    }
    /// The durable `ToolCallSettled` event for the call.
    pub(super) async fn settled_event(&self, call: &ToolCallId) -> EventRecord {
        self.harness
            .store
            .list_events(&self.session_id, None, 1000)
            .await
            .unwrap()
            .into_iter()
            .find(|event| {
                event.kind == EventKind::ToolCallSettled
                    && event.correlation.as_deref() == Some(call.to_string().as_str())
            })
            .expect("settled event persisted")
    }
    /// The model request made after the given zero-based turn (what the model sees next).
    pub(super) fn next_request(&self, after_turn: usize) -> ModelRequest {
        self.harness.fake.requests()[after_turn + 1].clone()
    }
    /// Text and `is_error` of the call's result as the next provider request carries it.
    pub(super) fn next_request_result(
        &self,
        after_turn: usize,
        call: &ToolCallId,
    ) -> (String, bool) {
        self.next_request(after_turn)
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .find_map(|part| match &part.content {
                ContentBlock::ToolResult {
                    call_id,
                    content,
                    is_error,
                } if call_id == call => Some((tool_text(content), *is_error)),
                _ => None,
            })
            .expect("tool result in next request")
    }
}

fn tool_text(content: &[ContentBlock]) -> String {
    match content {
        [ContentBlock::Text { text }] => text.clone(),
        other => panic!("expected one text block, got {other:?}"),
    }
}

/// Persisted text of a result record, for comparison with the tool message.
pub(super) fn record_text(record: &ToolCallRecord) -> String {
    tool_text(&record.result.as_ref().expect("settled result").content)
}

// Characterization of today's contract. The result transform receives only
// `{"result": <seed>, "is_error": <bool>}`: no tool name, input, or IDs. These tests pin that
// and the persisted outcome for each of the five paths. They are superseded by crabber-orgu,
// which replaces the payload with the typed context.

/// Assert one settled call end to end: record, tool message, event and next provider request
/// all agree, and the settled content is the JSON-encoded `text`.
async fn assert_settled(
    done: &Finished<'_>,
    call: &ToolCallId,
    status: ToolCallStatus,
    text: &str,
    is_error: bool,
) {
    let record = done.record(call).await;
    assert_eq!(record.status, status);
    assert_eq!(record_text(&record), text);
    let (_, message_text, message_error) = done.tool_message(call).await;
    assert_eq!((message_text.as_str(), message_error), (text, is_error));
    let event = done.settled_event(call).await;
    assert_eq!(event.payload["is_error"], is_error);
    assert_eq!(event.payload["content"][0]["text"], text);
    let (next_text, next_error) = done.next_request_result(0, call);
    assert_eq!((next_text.as_str(), next_error), (text, is_error));
}

#[tokio::test]
async fn characterize_success_path() {
    let call = ScriptedCall::text(ECHO, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(
        harness.probe.results(),
        [json!({"result": {"text": "hi"}, "is_error": false})]
    );
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Completed,
        r#"{"text":"hi"}"#,
        false,
    )
    .await;
    assert_eq!(done.record(&call.id).await.arguments, json!({"text": "hi"}));
}

#[tokio::test]
async fn characterize_execution_error_path() {
    let call = ScriptedCall::text(FAIL, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(
        harness.probe.results(),
        [json!({"result": "tool execution failed: executor exploded", "is_error": true})]
    );
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Failed,
        r#""tool execution failed: executor exploded""#,
        true,
    )
    .await;
}

#[tokio::test]
async fn characterize_permission_denial_path() {
    for denial in [Denial::Policy, Denial::RefusedApproval] {
        let call = ScriptedCall::text(FORBIDDEN, "hi");
        let harness = Harness::with_denial(one_turn(&[&call]), denial).await;
        let done = harness.run().await;
        assert_eq!(
            harness.probe.results(),
            [json!({"result": "permission denied", "is_error": true})]
        );
        assert_settled(
            &done,
            &call.id,
            ToolCallStatus::Failed,
            r#""permission denied""#,
            true,
        )
        .await;
    }
}

#[tokio::test]
async fn characterize_unknown_tool_path() {
    let call = ScriptedCall::text(MISSING, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(
        harness.probe.results(),
        [json!({"result": "unknown tool: missing", "is_error": true})]
    );
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Failed,
        r#""unknown tool: missing""#,
        true,
    )
    .await;
    // The raw provider arguments are dropped; only the sentinel survives durably.
    assert_eq!(
        done.record(&call.id).await.arguments,
        json!({"$crabber_prepare_error": "unknown tool: missing"})
    );
}

#[tokio::test]
async fn characterize_preparation_error_path() {
    // A `ToolPrepare` handler failure.
    let handler = ScriptedCall::text(ECHO, PREPARE_REJECTED);
    let harness = Harness::new(one_turn(&[&handler])).await;
    let done = harness.run().await;
    assert_eq!(harness.probe.prepares().len(), 1);
    assert_eq!(
        harness.probe.results(),
        [json!({"result": "tool execution failed: prepare handler rejected", "is_error": true})]
    );
    assert_settled(
        &done,
        &handler.id,
        ToolCallStatus::Failed,
        r#""tool execution failed: prepare handler rejected""#,
        true,
    )
    .await;

    // Schema validation failure: `text` must be a string, so no handler runs at all.
    let invalid = ScriptedCall::new(ECHO, &json!({"text": 5}));
    let harness = Harness::new(one_turn(&[&invalid])).await;
    let done = harness.run().await;
    assert_eq!(harness.probe.prepares().len(), 0);
    let results = harness.probe.results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["is_error"], true);
    let message = results[0]["result"].as_str().unwrap().to_owned();
    assert_eq!(
        done.record(&invalid.id).await.arguments,
        json!({"$crabber_prepare_error": message})
    );
    assert_settled(
        &done,
        &invalid.id,
        ToolCallStatus::Failed,
        &serde_json::to_string(&message).unwrap(),
        true,
    )
    .await;
}

#[tokio::test]
async fn characterize_two_calls_one_turn_each_see_only_result_and_flag() {
    let first = ScriptedCall::text(ECHO, "a");
    let second = ScriptedCall::text(ECHO, "b");
    let other = ScriptedCall::text(SHOUT, "c");
    let harness = Harness::new(one_turn(&[&first, &second, &other])).await;
    harness.run().await;
    // Sequential execution settles in call order; the payloads carry nothing to tell the two
    // `echo` calls apart except the tool output itself.
    assert_eq!(
        harness.probe.results(),
        [
            json!({"result": {"text": "a"}, "is_error": false}),
            json!({"result": {"text": "b"}, "is_error": false}),
            json!({"result": {"shouted": "C"}, "is_error": false}),
        ]
    );
}

#[tokio::test]
async fn harness_gate_parks_the_result_handler_until_released() {
    let call = ScriptedCall::text(ECHO, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let gate = harness.block_results();
    let handle = harness.start().await;
    gate.entered.notified().await;
    assert_eq!(harness.probe.results().len(), 1);
    assert_eq!(harness.fake.requests().len(), 1);
    gate.release.notify_one();
    handle.done().await.unwrap();
    assert_eq!(harness.fake.requests().len(), 2);
}
