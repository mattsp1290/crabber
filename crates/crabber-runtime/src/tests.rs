use crate::{
    ExecutionMode, InterruptPolicy, ModelStream, Observer, Orchestrator, PermissionDecision,
    PermissionPolicy, Request, RuntimeError, StaticPolicy, ToolPipeline,
};
use async_trait::async_trait;
use crabber_core::{
    ContentBlock, ContextEpoch, EpochId, EventCursor, EventKind, EventRecord, ManualClock, Message,
    Part, Role, Run, RunFence, RunId, RunStatus, Session, SessionId, ToolCallId, ToolCallRecord,
    ToolResult, Usage,
};
use crabber_extension::{
    ExtensionError, JsonToolExecutor, RunPlanProvider, StaticPlanProvider, ToolDefinition,
};
use crabber_providers::{
    DeltaStream, FakeProvider, ModelRequest, ProviderError, Resolver, Selection, StreamDelta,
    Streamer,
};
use crabber_session::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, MemoryStore, Store, StoreError,
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify, oneshot};

#[path = "tests/fault_store.rs"]
mod fault_store;
use fault_store::{
    DelayedTerminalExecution, DelayedTerminalStore, InitialTaskStage, SettlementGate,
};
#[path = "tests/compaction.rs"]
mod compaction;
#[path = "tests/heartbeat.rs"]
mod heartbeat;
#[path = "tests/operational_observation.rs"]
mod operational_observation;
#[path = "tests/recovery_observation.rs"]
mod recovery_observation;
#[path = "tests/resume_lifecycle.rs"]
mod resume_lifecycle;
#[path = "tests/tracing_spans.rs"]
mod tracing_spans;
use operational_observation::{
    OperationalAppendGate, OperationalCapture, OperationalClock, operational_stream,
};

mod approval_order;
mod approval_recovery;
#[path = "tests/callback_ownership.rs"]
mod callback_ownership;
#[path = "tests/continuation_source.rs"]
mod continuation_source;
#[path = "tests/continuations.rs"]
mod continuations;
#[path = "tests/frontend_replay.rs"]
mod frontend_replay;
mod native_phase;
#[path = "tests/owned_tool_invocations.rs"]
mod owned_tool_invocations;
#[path = "tests/preparation_resources.rs"]
mod preparation_resources;
#[path = "tests/prompt_products.rs"]
mod prompt_products;
#[path = "tests/provider_products.rs"]
mod provider_products;
#[path = "tests/public_state.rs"]
mod public_state;
#[path = "tests/reasoning_provenance.rs"]
mod reasoning_provenance;
#[path = "tests/suspension_observation.rs"]
mod suspension_observation;
#[path = "tests/tool_suspension_faults.rs"]
mod tool_suspension_faults;
mod turn_budget;
mod unstarted_batch;

#[path = "tests/native_attempt.rs"]
pub(crate) mod native_attempt;

#[path = "tests/owned_execution.rs"]
mod owned_execution;
#[path = "tests/terminal_outcome.rs"]
mod terminal_outcome;

#[derive(Default)]
struct RecordingObserver(Mutex<Vec<EventKind>>);

impl Observer for RecordingObserver {
    fn emit(&self, event: &crabber_core::EventRecord) {
        self.0
            .lock()
            .expect("observer poisoned")
            .push(event.kind.clone());
    }
}

struct PausePolicy;
impl PermissionPolicy for PausePolicy {
    fn decide(&self, _tool: &crabber_core::ToolInfo, _arguments: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(
        &self,
        _tool: &crabber_core::ToolInfo,
        _arguments: &Value,
    ) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}

struct CountingPipeline(Arc<AtomicUsize>);
#[async_trait]
impl ToolPipeline for CountingPipeline {
    async fn prepare(
        &self,
        _tool: &crabber_core::ToolInfo,
        arguments: Value,
    ) -> Result<Value, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        _context: &crabber_extension::ToolResultContext,
        _tool: &crabber_core::ToolInfo,
        result: crabber_core::ToolOutput,
    ) -> Result<crabber_extension::TransformOutput, String> {
        let result = result
            .json_value()
            .map_err(|_| "JSON fixture result required".to_owned())?;
        Ok(crabber_extension::TransformOutput::new(
            crabber_core::ToolOutput::from_json(&(result)).unwrap(),
        ))
    }
}

struct PartialModelStream(Arc<Notify>);
#[async_trait]
impl ModelStream for PartialModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let _ = next.stream(request).await?;
        let release = Arc::clone(&self.0);
        Ok(Box::pin(
            futures::stream::once(async { StreamDelta::TextDelta("partial".into()) }).chain(
                futures::stream::once(async move {
                    release.notified().await;
                    StreamDelta::Completed
                }),
            ),
        ))
    }
}

struct DeltaObserver(Mutex<Option<oneshot::Sender<()>>>);
impl Observer for DeltaObserver {
    fn emit(&self, event: &EventRecord) {
        if event.kind == EventKind::TextDelta
            && let Some(sender) = self.0.lock().unwrap().take()
        {
            let _ = sender.send(());
        }
    }
}

struct BarrierTool(Arc<Barrier>);
#[async_trait]
impl JsonToolExecutor for BarrierTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0.wait().await;
        Ok(arguments)
    }
}

#[tokio::test]
async fn parallel_tools_execute_together_and_settle_in_assistant_order() {
    let store = Arc::new(MemoryStore::new());
    let ids: Vec<_> = (0..3).map(|_| ToolCallId::new()).collect();
    let mut script = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        script.push(StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: "echo".into(),
        });
        script.push(StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: format!(r#"{{"text":"{index}"}}"#),
        });
        script.push(StreamDelta::ToolCallDone {
            call_id: id.clone(),
        });
    }
    script.push(StreamDelta::Completed);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(vec![
            script,
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(BarrierTool(Arc::new(Barrier::new(3)))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 3 })
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let settled: Vec<_> = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallSettled)
        .map(|event| event.correlation.unwrap())
        .collect();
    assert_eq!(
        settled,
        ids.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
}

async fn crashed_run_with_calls(
    running: bool,
) -> (Orchestrator, Arc<MemoryStore>, Arc<AtomicUsize>, RunId) {
    crashed_run_with_observer(running, Arc::new(crate::NoopObserver)).await
}

async fn crashed_run_with_observer(
    running: bool,
    observer: Arc<dyn Observer>,
) -> (Orchestrator, Arc<MemoryStore>, Arc<AtomicUsize>, RunId) {
    crashed_run_admitted_under(running, observer, None).await
}

/// `admitted_fingerprint` replaces the plan fingerprint stored with the run.
#[allow(clippy::too_many_lines)]
async fn crashed_run_admitted_under(
    running: bool,
    observer: Arc<dyn Observer>,
    admitted_fingerprint: Option<fn(&crabber_extension::RunPlan) -> String>,
) -> (Orchestrator, Arc<MemoryStore>, Arc<AtomicUsize>, RunId) {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let executed = Arc::new(AtomicUsize::new(0));
    let plan_provider = Arc::new(StaticPlanProvider::new(
        vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
        Vec::new(),
    ));
    let session = SessionId::new();
    let plan = plan_provider.acquire_plan(&session).await.unwrap();
    let stale = admitted_fingerprint.map(|stale| stale(&plan));
    plan.release();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(Vec::new())))
        .observer(observer)
        .plan_provider(plan_provider)
        .clock(clock.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let mut original = request();
    original.session_id = Some(session);
    let (plan, mut prepared, _) = runtime.prepare_admission(&original).await.unwrap();
    plan.release();
    prepared.request.session_id = None;
    if let Some(stale) = stale {
        prepared.request.plan_fingerprint.clone_from(&stale);
        prepared.binding.plan_fingerprint = crabber_core::InputFingerprint::new(stale).unwrap();
        prepared.binding.input_fingerprint = prepared.binding.computed_input_fingerprint().unwrap();
    }
    let admitted = store.admit_bound_run(prepared).await.unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    for safe in [true, false] {
        let id = ToolCallId::new();
        let event = EventRecord {
            cursor: None,
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            turn_id: None,
            kind: EventKind::ToolCallPending,
            payload: Value::Null,
            correlation: None,
            live_only: false,
            created_at: now,
        };
        execution
            .create_tool_call(
                ToolCallRecord {
                    id: id.clone(),
                    run_id: admitted.run.id.clone(),
                    name: "echo".into(),
                    arguments: json!({"text":"ok"}),
                    status: crabber_core::ToolCallStatus::Pending,
                    retry_safe: safe,
                    result: None,
                },
                event,
            )
            .await
            .unwrap();
        if running && safe {
            let event = EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallRunning,
                payload: Value::Null,
                correlation: None,
                live_only: false,
                created_at: now,
            };
            execution.claim_tool_call(&id, event).await.unwrap();
        }
    }
    clock.set(now + time::Duration::seconds(31));
    (runtime, store, executed, admitted.run.id)
}

/// The digest `compute_fingerprint` produced before the result-transform
/// contract component (D9): SHA-256 of the sorted component identities.
fn pre_contract_fingerprint(plan: &crabber_extension::RunPlan) -> String {
    use sha2::{Digest, Sha256};
    let mut components = plan.components.clone();
    components.sort_by(|a, b| (&a.id, &a.version).cmp(&(&b.id, &b.version)));
    crabber_extension::PlanFingerprint(
        Sha256::digest(serde_json::to_vec(&components).unwrap()).into(),
    )
    .to_string()
}

#[tokio::test]
async fn resume_refuses_run_admitted_under_pre_contract_fingerprint() {
    let (runtime, store, executed, run_id) = crashed_run_admitted_under(
        false,
        Arc::new(crate::NoopObserver),
        Some(pre_contract_fingerprint),
    )
    .await;
    let stored = store
        .get_run(&run_id)
        .await
        .unwrap()
        .unwrap()
        .plan_fingerprint;
    assert_eq!(stored.len(), 64);
    let error = runtime.resume(&run_id).await.unwrap_err();
    assert!(matches!(error, RuntimeError::PlanChanged), "{error:?}");
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn legacy_retry_safe_pending_calls_do_not_grant_native_dispatch() {
    let (runtime, store, executed, run_id) = crashed_run_with_calls(false).await;
    let before = store.list_unfinished_tool_calls(&run_id).await.unwrap();
    assert!(matches!(
        runtime.resume(&run_id).await,
        Err(RuntimeError::Attempt(
            crabber_session::AttemptError::InvalidTransition
        ))
    ));
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.list_unfinished_tool_calls(&run_id).await.unwrap(),
        before
    );
}

#[tokio::test]
async fn legacy_running_call_cannot_be_declared_interrupted_by_recovery() {
    let (runtime, store, executed, run_id) = crashed_run_with_calls(true).await;
    let before = store.list_unfinished_tool_calls(&run_id).await.unwrap();
    let report = runtime.recover().await.unwrap();
    assert_eq!(report.recovered, Vec::new());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(
        report.skipped[0].reason,
        crate::RecoverySkipReason::Attempt(crabber_session::AttemptError::InvalidTransition)
    );
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.list_unfinished_tool_calls(&run_id).await.unwrap(),
        before
    );
}

struct EchoTool(Arc<AtomicUsize>);

#[async_trait]
impl JsonToolExecutor for EchoTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}

struct GatedTool {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

struct CancellableTool {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl JsonToolExecutor for CancellableTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("tool gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}

struct BlockingSettledObserver {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl Observer for BlockingSettledObserver {
    fn emit(&self, event: &crabber_core::EventRecord) {
        if event.kind == EventKind::RunSettled {
            if let Some(sender) = self.entered.lock().expect("observer gate poisoned").take() {
                let _ = sender.send(());
            }
            let (lock, ready) = &*self.released;
            let mut released = lock.lock().expect("observer gate poisoned");
            while !*released {
                released = ready.wait(released).expect("observer gate poisoned");
            }
        }
    }
}

struct GatedModelStream {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl ModelStream for GatedModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let entered = self.entered.lock().expect("model gate poisoned").take();
        if let Some(sender) = entered {
            let _ = sender.send(());
            self.release.notified().await;
        }
        next.stream(request).await
    }
}

#[async_trait]
impl JsonToolExecutor for GatedTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        Ok(arguments)
    }
}

fn tool(executor: Arc<dyn crabber_extension::ToolExecutor>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: crabber_core::ToolInfo {
            name: "echo".into(),
            description: "Echo input".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: Vec::new(),
        },
        executor,
    })
}

fn request() -> Request {
    Request {
        selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: None,
        workspace_id: "test".into(),
        directory: "/tmp".into(),
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

fn orchestrator<S: Store + 'static>(
    store: Arc<S>,
    fake: FakeProvider,
    executor: Arc<dyn crabber_extension::ToolExecutor>,
    observer: Arc<RecordingObserver>,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(executor)],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(observer)
        .build()
        .unwrap()
}

fn call_script(call_id: ToolCallId, arguments: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("before".into()),
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "echo".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: arguments.into(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

fn text_script(text: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(text.into()), StreamDelta::Completed]
}

fn contains_text(messages: &[Message], text: &str) -> bool {
    messages
        .iter()
        .flat_map(|message| &message.parts)
        .any(|part| matches!(&part.content, ContentBlock::Text { text: actual } if actual == text))
}

#[tokio::test]
async fn invalid_tool_arguments_settle_failed_and_continue() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), "{}"),
        text_script("recovered"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::clone(&executed))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 2);
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(contains_text(&messages, "recovered"));
    assert!(
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .any(|part| matches!(
                &part.content,
                ContentBlock::ToolResult { is_error: true, .. }
            ))
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&fake.requests()[0].identity.run_id)
            .await
            .unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
    );
}

#[tokio::test]
async fn ask_policy_waits_for_a_durable_host_decision() {
    let store = Arc::new(MemoryStore::new());
    let executed = Arc::new(AtomicUsize::new(0));
    let fake = FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"ok"}"#)]);
    let runtime = Orchestrator::builder()
        .behavior_version("ask-test.v1")
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(executed.clone())))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    assert_eq!(
        store
            .approval_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .interactions
            .len(),
        1
    );
    assert!(
        store
            .list_unfinished_tool_calls(&run)
            .await
            .unwrap()
            .iter()
            .all(|call| call.result.is_none())
    );
}

#[tokio::test]
async fn enum_and_nested_schema_failures_never_execute_tool() {
    for arguments in [
        r#"{"choice":"forbidden","nested":{"flag":true}}"#,
        r#"{"choice":"allowed","nested":{}}"#,
    ] {
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), arguments),
            text_script("continued"),
        ]);
        let executed = Arc::new(AtomicUsize::new(0));
        let mut definition = tool(Arc::new(EchoTool(Arc::clone(&executed))));
        Arc::get_mut(&mut definition).unwrap().info.parameters = json!({
            "type":"object", "required":["choice","nested"],
            "properties":{
                "choice":{"enum":["allowed"]},
                "nested":{"type":"object","required":["flag"],"properties":{"flag":{"type":"boolean"}}}
            }
        });
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(Arc::clone(&store) as Arc<dyn Store>)
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![definition],
                Vec::new(),
            )))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        handle.done().await.unwrap();
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(fake.requests().len(), 2);
        let messages = store.list_messages(&session, None).await.unwrap();
        assert!(
            messages
                .iter()
                .flat_map(|message| &message.parts)
                .any(|part| matches!(
                    &part.content,
                    ContentBlock::ToolResult { is_error: true, .. }
                ))
        );
    }
}

#[tokio::test]
async fn completed_commit_survives_heartbeat_channel_closing_before_settle_returns() {
    let inner = Arc::new(MemoryStore::new());
    let (entered_tx, entered_rx) = oneshot::channel();
    let gate = Arc::new(SettlementGate {
        entered: Mutex::new(Some(entered_tx)),
        release: Notify::new(),
        terminal_reads: AtomicUsize::new(0),
        ..SettlementGate::default()
    });
    let store = Arc::new(DelayedTerminalStore {
        inner: inner.clone(),
        gate: Arc::clone(&gate),
    });
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .heartbeat_interval(std::time::Duration::from_millis(5))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inner.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while gate.terminal_reads.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn admission_recovery_verifies_turn_limit_before_terminal_replay() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![text_script("done")]));
    let plan = Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new()));
    let make = |max_turns| {
        Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store.clone())
            .resolver(provider.clone())
            .plan_provider(plan.clone())
            .max_turns(max_turns)
            .build()
            .unwrap()
    };
    let options = crabber_core::AdmissionOptions {
        key: crabber_core::AdmissionKey::new("turn-limit").unwrap(),
        fingerprint: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
    };
    let mut input = request();
    input.session_id = Some(SessionId::new());
    let crate::Admission::Started { handle, receipt } = make(64)
        .start_keyed(input.clone(), options.clone())
        .await
        .unwrap()
    else {
        panic!()
    };
    handle.done().await.unwrap();
    let before = store.get_run(&receipt.run_id).await.unwrap();
    assert!(matches!(
        make(2).recover_admission(input, options).await,
        Err(RuntimeError::Store(StoreError::AdmissionConflict))
    ));
    assert_eq!(store.get_run(&receipt.run_id).await.unwrap(), before);
    assert_eq!(provider.requests().len(), 1);
}

struct PipelineCleanup(Arc<Notify>);
#[async_trait]
impl ToolPipeline for PipelineCleanup {
    async fn prepare(
        &self,
        _tool: &crabber_core::ToolInfo,
        arguments: Value,
    ) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        context: &crabber_extension::ToolResultContext,
        _tool: &crabber_core::ToolInfo,
        result: crabber_core::ToolOutput,
    ) -> Result<crabber_extension::TransformOutput, String> {
        let result = result
            .json_value()
            .map_err(|_| "JSON fixture result required".to_owned())?;
        let closing = context.cleanup().closing();
        let reaped = self.0.clone();
        context.cleanup().spawn(async move {
            closing.await;
            reaped.notify_one();
        });
        Ok(crabber_extension::TransformOutput::new(
            crabber_core::ToolOutput::from_json(&(result)).unwrap(),
        ))
    }
}

#[tokio::test]
async fn pipeline_cleanup_is_joined_before_native_phase_settlement() {
    let store = Arc::new(MemoryStore::new());
    let completed = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool_pipeline(Arc::new(PipelineCleanup(completed.clone())))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .unwrap();
    let report = runtime
        .drain_execution_tasks(std::time::Duration::from_secs(1))
        .await;
    assert_eq!(report.resources.len(), 1);
    assert_eq!(report.resources[0].resources.pending, 0);
    // The actual phase callback observer and pipeline cleanup are both joined.
    assert_eq!(report.resources[0].resources.joined, 2);
    assert!(report.resources[0].resources.sealed);
    assert_eq!(
        report.resources[0].resources.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    let phase = report.resources[0].phase.as_ref().unwrap();
    let stored = store.tool_phase(&run, &phase.id).await.unwrap().phase;
    assert_eq!(stored.state, crabber_session::PhaseState::Completed);
    assert_eq!(
        stored.quiescence,
        crabber_session::PhaseQuiescence::Complete
    );
}

#[tokio::test]
async fn zero_output_cap_is_rejected_before_admission() {
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![])))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let mut input = request();
    let session = SessionId::new();
    input.session_id = Some(session.clone());
    input.max_output_tokens = Some(0);
    assert!(matches!(
        runtime.start(input).await,
        Err(RuntimeError::InvalidConfiguration(_))
    ));
    assert_eq!(store.get_session(&session).await.unwrap(), None);
    assert_eq!(
        store.list_unfinished_runs().await.unwrap(),
        Vec::<Run>::new()
    );
}

#[tokio::test(start_paused = true)]
async fn interruption_during_provider_backoff_is_prompt_and_never_retries() {
    struct BackoffSignal(Mutex<Option<oneshot::Sender<()>>>);
    impl Observer for BackoffSignal {
        fn emit(&self, _: &EventRecord) {}
        fn operational_completed(&self, observation: &crate::OperationalObservation) {
            if observation.reason == crate::TerminalReason::ProviderError
                && matches!(
                    observation.kind,
                    crate::OperationKind::Model {
                        purpose: crate::ModelPurpose::Turn,
                        ..
                    }
                )
                && let Some(sender) = self.0.lock().unwrap().take()
            {
                let _ = sender.send(());
            }
        }
    }
    let (sender, first_error) = oneshot::channel();
    let fake = FakeProvider::scripted(vec![
        vec![StreamDelta::Error(ProviderError {
            kind: crabber_providers::ProviderErrorKind::Server,
            message: "temporary".into(),
            retryable: true,
        })],
        text_script("must not request"),
    ]);
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_version("retry-cancellation-test.v1")
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .observer(Arc::new(BackoffSignal(Mutex::new(Some(sender)))))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    first_error.await.unwrap();
    handle.interrupt();
    let result = tokio::time::timeout(std::time::Duration::from_millis(25), handle.done())
        .await
        .expect("cancellation must not wait for the 100ms retry backoff")
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(fake.requests().len(), 1);
}

#[path = "tests/terminal_control.rs"]
mod terminal_control;

#[path = "tests/grant_ack_faults.rs"]
mod grant_ack_faults;
