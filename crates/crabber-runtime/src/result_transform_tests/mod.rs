//! Shared harness and runtime integration checks for the tool-result-transform contract.
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
mod context;
mod interrupted_runtime;
mod paths;
mod recovery;
mod tamper;

use crate::{
    ApprovalRequester, ExecutionMode, Orchestrator, PermissionDecision, PermissionPolicy, Request,
    RunHandle, ToolPipeline,
};
use async_trait::async_trait;
use crabber_core::{
    ContentBlock, EventKind, EventRecord, Message, Role, RunId, SessionId, ToolCallId,
    ToolCallRecord, ToolCallStatus, ToolInfo,
};
use crabber_extension::{
    Extension, ExtensionError, Point, Registrar, Registry, RunPlanProvider, Scope, ToolDefinition,
    ToolExecutor, ToolGuard, ToolPrepare, ToolResultContext, ToolResultTransform,
};
use crabber_providers::{FakeProvider, ModelRequest, Resolver, Selection, StreamDelta};
use crabber_session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, oneshot};

/// Tool whose executor echoes its input.
pub(super) const ECHO: &str = "echo";
/// Second succeeding tool, so one turn can call two different tools.
pub(super) const SHOUT: &str = "shout";
/// Tool whose executor always returns `Err`.
pub(super) const FAIL: &str = "fail";
/// Tool the harness policy denies before it executes.
pub(super) const FORBIDDEN: &str = "forbidden";
/// Tool whose schema accepts any object, so a sentinel-shaped argument object passes validation.
pub(super) const OPEN: &str = "open";
/// Tool whose executor fails with the text `permission denied`.
pub(super) const SAYS_DENIED: &str = "says-denied";
/// Tool name the plan does not contain.
pub(super) const MISSING: &str = "missing";
/// `text` value a `ToolPrepare` handler rejects.
pub(super) const PREPARE_REJECTED: &str = "reject-in-prepare";
/// Message the failing executor reports.
pub(super) const EXECUTOR_ERROR: &str = "executor exploded";

struct TextTool {
    name: &'static str,
    probe: Arc<Probe>,
}
#[async_trait]
impl ToolExecutor for TextTool {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        self.probe
            .executed
            .lock()
            .unwrap()
            .push((self.name.to_owned(), input.clone()));
        match self.name {
            FAIL => Err(ExtensionError::Tool(EXECUTOR_ERROR.into())),
            SAYS_DENIED => Err(ExtensionError::Tool("permission denied".into())),
            SHOUT => {
                Ok(json!({"shouted": input["text"].as_str().unwrap_or_default().to_uppercase()}))
            }
            _ => Ok(input),
        }
    }
}

/// Parks handlers of any kind until the test releases them.
///
/// Not tied to the recording handler: a test builds a `Gate`, and any handler (an ordinary
/// result handler, a `ToolPipeline` stage, a final redactor) calls [`Gate::pass`] with a key
/// describing what it is processing. Only keys the gate's selector accepts park, so one call can
/// be held while others go through, and a specific parked call can be released on its own.
///
/// JSON handlers can select by the authoritative context or the current result. Arrivals are
/// counted by a semaphore and handed out in
/// order, so several handlers parking at once never coalesce, and release is by per-entry
/// channel, so no wakeup is lost. Release only affects entries that have already parked, so
/// await [`Gate::entered`] first.
pub(super) struct Gate {
    select: Box<dyn Fn(&Value) -> bool + Send + Sync>,
    arrivals: Semaphore,
    state: Mutex<GateState>,
}
#[derive(Default)]
struct GateState {
    /// Keys that have parked and not yet been handed out by `entered`.
    unseen: VecDeque<Value>,
    /// Parked entries waiting for release.
    parked: Vec<(Value, oneshot::Sender<()>)>,
}
impl Gate {
    /// Park every key the predicate accepts.
    pub(super) fn when(select: impl Fn(&Value) -> bool + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            select: Box::new(select),
            arrivals: Semaphore::new(0),
            state: Mutex::default(),
        })
    }
    /// Park every key.
    pub(super) fn all() -> Arc<Self> {
        Self::when(|_| true)
    }
    /// Called by the handler under test: returns at once for an unselected key, otherwise parks
    /// until released.
    pub(super) async fn pass(&self, key: &Value) {
        if !(self.select)(key) {
            return;
        }
        let (release, parked) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap();
            state.unseen.push_back(key.clone());
            state.parked.push((key.clone(), release));
        }
        self.arrivals.add_permits(1);
        let _ = parked.await;
    }
    /// Wait for the next handler to park (one per call, in arrival order) and return its key.
    pub(super) async fn entered(&self) -> Value {
        self.arrivals.acquire().await.unwrap().forget();
        self.state.lock().unwrap().unseen.pop_front().unwrap()
    }
    /// Number of handlers currently parked.
    pub(super) fn parked(&self) -> usize {
        self.state.lock().unwrap().parked.len()
    }
    /// Release every parked entry whose key the predicate accepts; returns how many.
    pub(super) fn release_where(&self, accept: impl Fn(&Value) -> bool) -> usize {
        let mut state = self.state.lock().unwrap();
        let (release, keep) = std::mem::take(&mut state.parked)
            .into_iter()
            .partition::<Vec<_>, _>(|(key, _)| accept(key));
        state.parked = keep;
        let released = release.len();
        for (_, sender) in release {
            let _ = sender.send(());
        }
        released
    }
    /// Release everything parked.
    pub(super) fn release_all(&self) -> usize {
        self.release_where(|_| true)
    }
}

/// Everything the recording extension saw, as the raw payloads handlers receive.
#[derive(Default)]
pub(super) struct Probe {
    /// Payload of every `ToolResultTransform` invocation, in order.
    pub(super) results: Mutex<Vec<Value>>,
    /// Payload of every `ToolPrepare` invocation, in order.
    pub(super) prepares: Mutex<Vec<Value>>,
    /// Every executor invocation as `(tool name, input)`, in order.
    pub(super) executed: Mutex<Vec<(String, Value)>>,
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
    pub(super) fn executed(&self) -> Vec<(String, Value)> {
        self.executed.lock().unwrap().clone()
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
        for name in [ECHO, SHOUT, FAIL, FORBIDDEN, OPEN, SAYS_DENIED] {
            r.tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: name.into(),
                    description: "test".into(),
                    parameters: if name == OPEN {
                        json!({"type":"object"})
                    } else {
                        json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]})
                    },
                    retry_safe: true,
                    required_permissions: vec![],
                },
                executor: Arc::new(TextTool {
                    name,
                    probe: Arc::clone(&self.probe),
                }),
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
                        gate.pass(&value).await;
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

/// Extension built from a closure, so a slice can mount extra handlers, guards or restrictions
/// from its own file without a new type.
pub(super) struct ClosureExtension {
    id: &'static str,
    install: Box<dyn Fn(&mut Registrar) + Send + Sync>,
}
impl ClosureExtension {
    pub(super) fn new(
        id: &'static str,
        install: impl Fn(&mut Registrar) + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            install: Box::new(install),
        })
    }
    /// Extension that installs a guard (`Registrar::guard`, consulted in `permit_and_execute`).
    pub(super) fn guard(id: &'static str, guard: Arc<dyn ToolGuard>) -> Arc<Self> {
        Self::new(id, move |r| r.guard(Arc::clone(&guard)))
    }
    /// Extension that restricts the run to the named tools (`Registrar::restrict_tools`); every
    /// other tool is denied.
    pub(super) fn restrict_to(id: &'static str, names: &[&str]) -> Arc<Self> {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        Self::new(id, move |r| r.restrict_tools(names.clone()))
    }
}
#[async_trait]
impl Extension for ClosureExtension {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        (self.install)(r);
        Ok(())
    }
}

/// Configures a [`Harness`]; everything a later slice needs is reachable from here.
pub(super) struct HarnessBuilder {
    scripts: Vec<Vec<StreamDelta>>,
    extensions: Vec<(Arc<dyn Extension>, Scope)>,
    mode: Option<ExecutionMode>,
    pipeline: Option<Arc<dyn ToolPipeline>>,
    policy: Arc<dyn PermissionPolicy>,
    approver: Option<Arc<dyn ApprovalRequester>>,
}
impl HarnessBuilder {
    pub(super) fn new(scripts: Vec<Vec<StreamDelta>>) -> Self {
        Self {
            scripts,
            extensions: Vec::new(),
            mode: None,
            pipeline: None,
            policy: Arc::new(DenyForbidden),
            approver: None,
        }
    }
    /// Mount another extension as its own mount, after the recorder.
    pub(super) fn mount(mut self, extension: Arc<dyn Extension>, scope: Scope) -> Self {
        self.extensions.push((extension, scope));
        self
    }
    pub(super) fn execution_mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = Some(mode);
        self
    }
    /// Host `ToolPipeline` (runs `prepare` before and `transform_result` after execution).
    pub(super) fn tool_pipeline(mut self, pipeline: Arc<dyn ToolPipeline>) -> Self {
        self.pipeline = Some(pipeline);
        self
    }
    pub(super) fn policy(mut self, policy: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = policy;
        self
    }
    pub(super) fn approver(mut self, approver: Arc<dyn ApprovalRequester>) -> Self {
        self.approver = Some(approver);
        self
    }
    pub(super) async fn build(self) -> Harness {
        let store = Arc::new(MemoryStore::new());
        let turns = self.scripts.len();
        let fake = FakeProvider::scripted(self.scripts);
        let probe = Arc::new(Probe::default());
        let registry = Arc::new(Registry::new());
        registry
            .mount(
                Arc::new(RecordingExtension {
                    probe: Arc::clone(&probe),
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        for (extension, scope) in self.extensions {
            registry.mount(extension, scope).await.unwrap();
        }
        let harness = Harness {
            store,
            fake,
            runtime: None,
            registry,
            probe,
            turns,
            mode: self.mode,
            pipeline: self.pipeline,
            policy: self.policy,
            approver: self.approver,
            contexts: Arc::default(),
        };
        let runtime = harness.fresh_runtime();
        Harness {
            runtime: Some(runtime),
            ..harness
        }
    }
}

/// A runtime wired to the recording extension, a scripted provider and an in-memory store.
pub(super) struct Harness {
    pub(super) store: Arc<MemoryStore>,
    pub(super) fake: FakeProvider,
    runtime: Option<Orchestrator>,
    pub(super) registry: Arc<Registry>,
    pub(super) probe: Arc<Probe>,
    turns: usize,
    mode: Option<ExecutionMode>,
    pipeline: Option<Arc<dyn ToolPipeline>>,
    policy: Arc<dyn PermissionPolicy>,
    approver: Option<Arc<dyn ApprovalRequester>>,
    contexts: Arc<Mutex<Vec<ToolResultContext>>>,
}
impl Harness {
    pub(super) fn builder(scripts: Vec<Vec<StreamDelta>>) -> HarnessBuilder {
        HarnessBuilder::new(scripts)
    }
    /// Every tool allowed except [`FORBIDDEN`], which the policy denies.
    pub(super) async fn new(scripts: Vec<Vec<StreamDelta>>) -> Self {
        HarnessBuilder::new(scripts).build().await
    }
    pub(super) async fn with_denial(scripts: Vec<Vec<StreamDelta>>, denial: Denial) -> Self {
        let builder = HarnessBuilder::new(scripts);
        match denial {
            Denial::Policy => builder,
            Denial::RefusedApproval => builder
                .policy(Arc::new(AskAndRefuse))
                .approver(Arc::new(Refuse)),
        }
        .build()
        .await
    }
    /// The runtime the harness was built with.
    pub(super) fn runtime(&self) -> &Orchestrator {
        self.runtime.as_ref().expect("built")
    }
    /// A new `Orchestrator` over the same store, registry and provider, for resume/recover tests.
    pub(super) fn fresh_runtime(&self) -> Orchestrator {
        self.fresh_runtime_with(Arc::new(self.fake.clone()))
    }
    /// As [`Harness::fresh_runtime`], with a different model resolver (e.g. new scripted turns).
    pub(super) fn fresh_runtime_with(&self, resolver: Arc<dyn Resolver>) -> Orchestrator {
        let mut builder = Orchestrator::builder()
            .store(Arc::clone(&self.store) as Arc<dyn Store>)
            .resolver(resolver)
            .plan_provider(Arc::clone(&self.registry) as Arc<dyn RunPlanProvider>)
            .policy(Arc::clone(&self.policy))
            .context_observer({
                let contexts = Arc::clone(&self.contexts);
                Arc::new(move |context: &ToolResultContext| {
                    contexts.lock().unwrap().push(context.clone());
                })
            });
        if let Some(approver) = &self.approver {
            builder = builder.approver(Arc::clone(approver));
        }
        if let Some(pipeline) = &self.pipeline {
            builder = builder.tool_pipeline(Arc::clone(pipeline));
        }
        if let Some(mode) = self.mode {
            builder = builder.execution_mode(mode);
        }
        builder.build().unwrap()
    }
    /// Every authoritative result context any runtime of this harness has built, in order.
    pub(super) fn contexts(&self) -> Vec<ToolResultContext> {
        self.contexts.lock().unwrap().clone()
    }
    /// Install a gate that parks every result handler after it records its payload.
    pub(super) fn block_results(&self) -> Arc<Gate> {
        self.block_results_with(Gate::all())
    }
    /// Install the given gate on the recording result handler.
    pub(super) fn block_results_with(&self, gate: Arc<Gate>) -> Arc<Gate> {
        *self.probe.gate.lock().unwrap() = Some(Arc::clone(&gate));
        gate
    }
    /// Start a run without waiting for it (for cancellation tests).
    pub(super) async fn start(&self) -> RunHandle {
        self.runtime().start(request()).await.unwrap()
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
    /// The model request made after the given zero-based turn. Assumes one provider request per
    /// scripted turn (no retries or compaction) and fails loudly if that does not hold; prefer
    /// [`Finished::next_request_for`] when a call ID is available.
    pub(super) fn next_request(&self, after_turn: usize) -> ModelRequest {
        let requests = self.harness.fake.requests();
        assert_eq!(
            requests.len(),
            self.harness.turns,
            "expected exactly one provider request per scripted turn"
        );
        requests[after_turn + 1].clone()
    }
    /// The first model request that carries a result for the call: what the model sees next,
    /// independent of how many requests preceded it.
    pub(super) fn next_request_for(&self, call: &ToolCallId) -> ModelRequest {
        self.harness
            .fake
            .requests()
            .into_iter()
            .find(|request| request_result(request, call).is_some())
            .expect("a provider request carries the call's result")
    }
    /// Text and `is_error` of the call's result as the next provider request carries it.
    pub(super) fn next_request_result(&self, call: &ToolCallId) -> (String, bool) {
        request_result(&self.next_request_for(call), call).expect("tool result in request")
    }
}

fn request_result(request: &ModelRequest, call: &ToolCallId) -> Option<(String, bool)> {
    request
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

// The old characterize_* tests required a result/is_error-only payload and are
// retired by crabber-orgu (D9). Their outcome and persistence coverage below now
// checks the authoritative envelope; context.rs covers durable input and IDs.

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
    let (next_text, next_error) = done.next_request_result(call);
    assert_eq!((next_text.as_str(), next_error), (text, is_error));
}

fn observed_results(harness: &Harness) -> Vec<Value> {
    harness
        .probe
        .results()
        .into_iter()
        .map(|envelope| {
            assert_eq!(envelope.as_object().unwrap().len(), 3);
            assert_eq!(envelope["mark_error"], false);
            assert!(envelope["context"]["call_id"].is_string());
            json!({"result": envelope["result"], "is_error": envelope["context"]["is_error"]})
        })
        .collect()
}

#[tokio::test]
async fn success_path_uses_authoritative_envelope() {
    let call = ScriptedCall::text(ECHO, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(
        observed_results(&harness),
        [json!({"result": {"text": "hi"}, "is_error": false})]
    );
    assert_eq!(
        harness.probe.executed(),
        [(ECHO.to_owned(), json!({"text": "hi"}))]
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
async fn execution_error_path_uses_authoritative_envelope() {
    let call = ScriptedCall::text(FAIL, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(
        observed_results(&harness),
        [json!({"result": "tool execution failed: executor exploded", "is_error": true})]
    );
    assert_eq!(
        harness.probe.executed(),
        [(FAIL.to_owned(), json!({"text": "hi"}))]
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
async fn permission_denial_path_uses_authoritative_envelope() {
    for denial in [Denial::Policy, Denial::RefusedApproval] {
        let call = ScriptedCall::text(FORBIDDEN, "hi");
        let harness = Harness::with_denial(one_turn(&[&call]), denial).await;
        let done = harness.run().await;
        assert_eq!(
            observed_results(&harness),
            [json!({"result": "permission denied", "is_error": true})]
        );
        assert_eq!(harness.probe.executed(), []);
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
async fn unknown_tool_path_uses_authoritative_envelope() {
    let call = ScriptedCall::text(MISSING, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(
        observed_results(&harness),
        [json!({"result": "unknown tool: missing", "is_error": true})]
    );
    assert_eq!(harness.probe.executed(), []);
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Failed,
        r#""unknown tool: missing""#,
        true,
    )
    .await;
    // crabber-zv2d: the record now keeps the raw provider arguments under the unknown-tool key
    // (Recorded answer 1), so the class comes from the record instead of the error text. The
    // persisted result is unchanged.
    assert_eq!(
        done.record(&call.id).await.arguments,
        json!({"$crabber_unknown_tool": {"raw": {"text": "hi"}}})
    );
}

#[tokio::test]
async fn preparation_error_path_uses_authoritative_envelope() {
    // A `ToolPrepare` handler failure.
    let handler = ScriptedCall::text(ECHO, PREPARE_REJECTED);
    let harness = Harness::new(one_turn(&[&handler])).await;
    let done = harness.run().await;
    assert_eq!(harness.probe.prepares().len(), 1);
    assert_eq!(harness.probe.executed(), []);
    assert_eq!(
        observed_results(&harness),
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
    assert_eq!(harness.probe.executed(), []);
    let results = harness.probe.results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["context"]["is_error"], true);
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
async fn two_calls_one_turn_keep_distinct_authoritative_ids() {
    let first = ScriptedCall::text(ECHO, "a");
    let second = ScriptedCall::text(ECHO, "b");
    let other = ScriptedCall::text(SHOUT, "c");
    let harness = Harness::new(one_turn(&[&first, &second, &other])).await;
    harness.run().await;
    // Sequential execution settles in call order, with a distinct durable ID per call.
    let envelopes = harness.probe.results();
    for (envelope, call) in envelopes.iter().zip([&first, &second, &other]) {
        assert_eq!(envelope["context"]["call_id"], call.id.to_string());
        assert_eq!(envelope["context"]["tool_name"], call.name);
    }
    assert_eq!(
        observed_results(&harness),
        [
            json!({"result": {"text": "a"}, "is_error": false}),
            json!({"result": {"text": "b"}, "is_error": false}),
            json!({"result": {"shouted": "C"}, "is_error": false}),
        ]
    );
    assert_eq!(
        harness.probe.executed(),
        [
            (ECHO.to_owned(), json!({"text": "a"})),
            (ECHO.to_owned(), json!({"text": "b"})),
            (SHOUT.to_owned(), json!({"text": "c"})),
        ]
    );
}

#[tokio::test]
async fn harness_gate_parks_the_result_handler_until_released() {
    let call = ScriptedCall::text(ECHO, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let gate = harness.block_results();
    let handle = harness.start().await;
    let key = gate.entered().await;
    assert_eq!(key["result"], json!({"text": "hi"}));
    assert_eq!(key["context"]["call_id"], call.id.to_string());
    assert_eq!(gate.parked(), 1);
    assert_eq!(harness.probe.results().len(), 1);
    assert_eq!(harness.fake.requests().len(), 1);
    assert_eq!(gate.release_all(), 1);
    handle.done().await.unwrap();
    assert_eq!(harness.fake.requests().len(), 2);
}

#[tokio::test]
async fn harness_gate_parks_only_the_selected_call_and_releases_it_alone() {
    let held = ScriptedCall::text(ECHO, "held");
    let free = ScriptedCall::text(SHOUT, "free");
    // Select this call by its current output while the other call completes freely.
    let gate = Gate::when(|value| value["result"]["text"] == "held");
    let harness = Harness::builder(one_turn(&[&held, &free]))
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .build()
        .await;
    harness.block_results_with(Arc::clone(&gate));
    let handle = harness.start().await;
    let key = gate.entered().await;
    assert_eq!(key["result"]["text"], "held");
    assert_eq!(gate.parked(), 1);
    assert_eq!(gate.release_where(|key| key["result"]["text"] == "held"), 1);
    handle.done().await.unwrap();
    // The unselected call went through the handler without ever parking.
    assert_eq!(gate.parked(), 0);
    assert_eq!(harness.probe.results().len(), 2);
}

struct DenyEcho;
impl ToolGuard for DenyEcho {
    fn id(&self) -> &'static str {
        "deny-echo"
    }
    fn check(&self, name: &str, _input: &Value) -> crabber_extension::GuardDecision {
        if name == ECHO {
            crabber_extension::GuardDecision::Deny
        } else {
            crabber_extension::GuardDecision::Abstain
        }
    }
}

#[tokio::test]
async fn harness_builder_mounts_extra_extensions_and_runs_in_parallel() {
    let a = ScriptedCall::text(ECHO, "a");
    let b = ScriptedCall::text(SHOUT, "b");
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let observer = Arc::clone(&seen);
    let second = ClosureExtension::new("second-recorder", move |r| {
        let observer = Arc::clone(&observer);
        r.on_transform(
            ToolResultTransform::ID,
            1,
            "second-record-result",
            Arc::new(move |value| {
                let observer = Arc::clone(&observer);
                Box::pin(async move {
                    observer.lock().unwrap().push(value.clone());
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&a, &b]))
        .mount(second, Scope::Global)
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .build()
        .await;
    let done = harness.run().await;
    // Both mounts' handlers saw both results (parallel completion order is not fixed).
    assert_eq!(harness.probe.results().len(), 2);
    assert_eq!(seen.lock().unwrap().len(), 2);
    done.record(&a.id).await;
    done.record(&b.id).await;
    // A fresh runtime shares the store and registry.
    let _fresh = harness.fresh_runtime();
}

#[tokio::test]
async fn harness_builder_installs_guard_and_restriction_denials() {
    let guarded = ScriptedCall::text(ECHO, "a");
    let restricted = ScriptedCall::text(SHOUT, "b");
    let harness = Harness::builder(one_turn(&[&guarded, &restricted]))
        .mount(
            ClosureExtension::guard("deny-echo-guard", Arc::new(DenyEcho)),
            Scope::Global,
        )
        .mount(
            ClosureExtension::restrict_to("only-echo", &[ECHO]),
            Scope::Global,
        )
        .build()
        .await;
    harness.run().await;
    assert_eq!(harness.probe.executed(), []);
    assert_eq!(
        observed_results(&harness),
        [
            json!({"result": "permission denied", "is_error": true}),
            json!({"result": "permission denied", "is_error": true}),
        ]
    );
}

#[tokio::test]
async fn typed_result_chain_preserves_structured_output_and_error_classes() {
    for name in [ECHO, FAIL, FORBIDDEN, MISSING] {
        let call = ScriptedCall::text(name, "secret-input");
        let id = call.id.clone();
        let transform = ClosureExtension::new("typed-result", move |r| {
            let id = id.clone();
            r.on_result_transform(
                1,
                "structured",
                Arc::new(move |context, _| {
                    assert_eq!(context.call_id(), &id);
                    Box::pin(async move {
                        Ok(crabber_extension::TransformOutput::marked_error(
                            json!({"safe": true}),
                        ))
                    })
                }),
            );
        });
        let harness = Harness::builder(one_turn(&[&call]))
            .mount(transform, Scope::Global)
            .build()
            .await;
        let done = harness.run().await;
        assert_settled(
            &done,
            &call.id,
            ToolCallStatus::Failed,
            r#"{"safe":true}"#,
            true,
        )
        .await;
    }
}

#[tokio::test]
async fn result_chain_failures_persist_only_fixed_handler_message() {
    for mode in ["error", "panic", "context", "non-envelope"] {
        let call = ScriptedCall::text(ECHO, "SECRET-ORIGINAL");
        let transform = ClosureExtension::new("failing-result", move |r| {
            r.on_transform(
                ToolResultTransform::ID,
                1,
                "sanitize-failure",
                Arc::new(move |mut value| {
                    Box::pin(async move {
                        match mode {
                            "error" => return Err(ExtensionError::Tool("SECRET-ERROR".into())),
                            "panic" => panic!("SECRET-PANIC"),
                            "context" => value["context"]["tool_name"] = json!("SECRET-CONTEXT"),
                            _ => return Ok(json!("SECRET-MALFORMED")),
                        }
                        value["result"] = json!("SECRET-INTERMEDIATE");
                        Ok(value)
                    })
                }),
            );
            r.on_final_redaction(
                2,
                "must-not-run",
                Arc::new(|_, _| panic!("final redactor ran after a D2 failure")),
            );
        });
        let harness = Harness::builder(one_turn(&[&call]))
            .mount(transform, Scope::Global)
            .build()
            .await;
        let done = harness.run().await;
        let text = serde_json::to_string(&crabber_extension::result_transform_failed_message(
            "sanitize-failure",
        ))
        .unwrap();
        assert_settled(&done, &call.id, ToolCallStatus::Failed, &text, true).await;
        assert!(
            !serde_json::to_string(&done.settled_event(&call.id).await)
                .unwrap()
                .contains("SECRET")
        );
    }
}
