//! Workspace context path matrix: every entry path exposes the persisted
//! session identity to tools and `ContextAssemble`, and drift fails closed.

use super::{PausePolicy, text_script};
use crate::{
    Admission, Orchestrator, PermissionDecision, PermissionPolicy, Request, RuntimeError,
    StaticPolicy,
};
use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, Clock, EpochId, EventCursor, EventKind,
    EventRecord, InputFingerprint, ManualClock, Message, Run, RunFence, RunId, RunStatus, Session,
    SessionId, ToolCallId, ToolCallRecord, ToolCallStatus, ToolInfo,
};
use crabber_extension::{
    ContextAssemble, Extension, ExtensionError, JsonToolExecutor, Point, Registrar, Registry,
    RunPlan, RunPlanProvider, Scope, ToolContext, ToolDefinition, WorkspaceContext,
};
use crabber_providers::{FakeProvider, Selection, StreamDelta};
use crabber_session::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, MemoryStore, Store, StoreError,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use time::OffsetDateTime;

type Identity = (Option<String>, Option<String>);
const NONE: [Identity; 0] = [];

fn identity(workspace_id: &str, directory: &str) -> Identity {
    let available = |value: &str| (!value.is_empty()).then(|| value.to_owned());
    (available(workspace_id), available(directory))
}

/// What the tool, the forging handler and the handler after it observed.
#[derive(Default)]
struct Seen {
    tool: Mutex<Vec<Identity>>,
    first_handler: Mutex<Vec<Value>>,
    later_handler: Mutex<Vec<Value>>,
}
impl Seen {
    fn tools(&self) -> Vec<Identity> {
        self.tool.lock().unwrap().clone()
    }
    fn handler_calls(&self) -> usize {
        self.first_handler.lock().unwrap().len()
    }
    /// Every handler invocation, before and after the forging handler, saw `expected`.
    fn assert_handlers_saw(&self, expected: &Identity) {
        let first = self.first_handler.lock().unwrap();
        let later = self.later_handler.lock().unwrap();
        assert!(!first.is_empty(), "ContextAssemble did not run");
        assert_eq!(first.len(), later.len());
        for value in first.iter().chain(later.iter()) {
            assert_eq!(
                value[WorkspaceContext::WORKSPACE_ID_KEY],
                json!(expected.0),
                "{value}"
            );
            assert_eq!(
                value[WorkspaceContext::DIRECTORY_KEY],
                json!(expected.1),
                "{value}"
            );
        }
    }
}

struct ProbeTool(Arc<Seen>);
#[async_trait]
impl JsonToolExecutor for ProbeTool {
    async fn execute(&self, _arguments: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Tool("context required".into()))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        assert!(!context.cancel.is_cancelled());
        let workspace = context.workspace();
        self.0.tool.lock().unwrap().push((
            workspace.workspace_id().map(str::to_owned),
            workspace.directory().map(str::to_owned),
        ));
        Ok(arguments)
    }
}

struct ProbeExtension(Arc<Seen>);
#[async_trait]
impl Extension for ProbeExtension {
    fn id(&self) -> &'static str {
        "workspace-probe"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "probe".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "Echo input".into(),
                parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
                retry_safe: true,
                required_permissions: Vec::new(),
            },
            executor: Arc::new(ProbeTool(Arc::clone(&self.0))),
        }));
        let seen = Arc::clone(&self.0);
        registrar.on_transform(
            ContextAssemble::ID,
            0,
            "forge",
            Arc::new(move |mut value| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    seen.first_handler.lock().unwrap().push(value.clone());
                    value[WorkspaceContext::WORKSPACE_ID_KEY] = json!("forged");
                    value
                        .as_object_mut()
                        .unwrap()
                        .remove(WorkspaceContext::DIRECTORY_KEY);
                    Ok(value)
                })
            }),
        );
        let seen = Arc::clone(&self.0);
        registrar.on_transform(
            ContextAssemble::ID,
            1,
            "observe",
            Arc::new(move |value| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    seen.later_handler.lock().unwrap().push(value.clone());
                    Ok(value)
                })
            }),
        );
        Ok(())
    }
}

struct CountingPlans {
    inner: Registry,
    acquired: AtomicUsize,
}
#[async_trait]
impl RunPlanProvider for CountingPlans {
    fn registration_fingerprint(
        &self,
        context: &crabber_core::SessionContext,
    ) -> Result<crabber_core::InputFingerprint, ExtensionError> {
        self.inner.registration_fingerprint(context)
    }

    async fn acquire_plan(&self, session: &SessionId) -> Result<RunPlan, ExtensionError> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        self.inner.acquire_plan(session).await
    }
    async fn acquire_plan_in(
        &self,
        context: &crabber_core::SessionContext,
    ) -> Result<RunPlan, ExtensionError> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        self.inner.acquire_plan_in(context).await
    }
}

#[path = "workspace_context/drift_store.rs"]
mod drift_store;
use drift_store::DriftStore;

struct Harness {
    clock: Arc<ManualClock>,
    memory: Arc<MemoryStore>,
    store: Arc<DriftStore>,
    fake: FakeProvider,
    plans: Arc<CountingPlans>,
    seen: Arc<Seen>,
    runtime: Orchestrator,
}

/// A fresh host: its own registry, extension instance and orchestrator over `store`.
async fn host(
    clock: &Arc<ManualClock>,
    store: &Arc<DriftStore>,
    fake: &FakeProvider,
    policy: Arc<dyn PermissionPolicy>,
) -> (Orchestrator, Arc<Seen>, Arc<CountingPlans>) {
    let seen = Arc::new(Seen::default());
    let registry = Registry::new();
    registry
        .mount(Arc::new(ProbeExtension(Arc::clone(&seen))), Scope::Global)
        .await
        .unwrap();
    let plans = Arc::new(CountingPlans {
        inner: registry,
        acquired: AtomicUsize::new(0),
    });
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(store) as Arc<dyn Store>)
        .clock(clock.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(plans.clone())
        .policy(policy)
        .build()
        .unwrap();
    (runtime, seen, plans)
}

async fn harness(scripts: Vec<Vec<StreamDelta>>, policy: Arc<dyn PermissionPolicy>) -> Harness {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let memory = Arc::new(MemoryStore::with_clock(clock.clone()));
    let store = DriftStore::new(Arc::clone(&memory));
    let fake = FakeProvider::scripted(scripts);
    let (runtime, seen, plans) = host(&clock, &store, &fake, policy).await;
    Harness {
        clock,
        memory,
        store,
        fake,
        plans,
        seen,
        runtime,
    }
}

fn allow() -> Arc<dyn PermissionPolicy> {
    Arc::new(StaticPolicy::new(PermissionDecision::Allow))
}

fn request(session: Option<&SessionId>, workspace_id: &str, directory: &str) -> Request {
    Request {
        selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: session.cloned(),
        workspace_id: workspace_id.into(),
        directory: directory.into(),
        title: "test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: None,
        model_controls: crabber_core::ModelControls::default(),
    }
}

/// A tool call whose model-supplied arguments carry a conflicting identity.
fn call_script() -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "echo".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: json!({"text":"ok","workspace_id":"model","directory":"/model"}).to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

fn options(key: &str) -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new(key).unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}

fn mismatch<T: std::fmt::Debug>(result: &Result<T, RuntimeError>) {
    assert!(
        matches!(
            result,
            Err(RuntimeError::Store(StoreError::SessionIdentityMismatch))
        ),
        "{result:?}"
    );
}

fn expire(clock: &ManualClock) {
    clock.set(clock.now() + time::Duration::seconds(31));
}

#[tokio::test]
async fn row1_fresh_unkeyed_admission_exposes_persisted_identity() {
    let h = harness(vec![call_script(), text_script("done")], allow()).await;
    let handle = h.runtime.start(request(None, "ws-a", "/a")).await.unwrap();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let expected = identity("ws-a", "/a");
    assert_eq!(h.seen.tools(), std::slice::from_ref(&expected));
    h.seen.assert_handlers_saw(&expected);
    assert_eq!(h.seen.handler_calls(), 2);
}

#[tokio::test]
async fn row2_fresh_keyed_admission_exposes_persisted_identity() {
    let h = harness(vec![call_script(), text_script("done")], allow()).await;
    let session = SessionId::new();
    let Admission::Started { handle, .. } = h
        .runtime
        .start_keyed(request(Some(&session), "ws-k", "/k"), options("row2"))
        .await
        .unwrap()
    else {
        panic!("first keyed admission starts")
    };
    handle.done().await.unwrap();
    let expected = identity("ws-k", "/k");
    assert_eq!(h.seen.tools(), std::slice::from_ref(&expected));
    h.seen.assert_handlers_saw(&expected);
}

#[tokio::test]
async fn row3_same_session_later_run_sees_identical_identity_and_rejects_drift() {
    let h = harness(
        vec![
            call_script(),
            text_script("one"),
            call_script(),
            text_script("two"),
        ],
        allow(),
    )
    .await;
    let handle = h.runtime.start(request(None, "ws-a", "/a")).await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    for (workspace_id, directory) in [("ws-a", "/b"), ("ws-b", "/a"), ("", "/a"), ("ws-a", "")] {
        mismatch(
            &h.runtime
                .start(request(Some(&session), workspace_id, directory))
                .await
                .map(|_| ()),
        );
        mismatch(
            &h.runtime
                .start_keyed(
                    request(Some(&session), workspace_id, directory),
                    options("row3"),
                )
                .await,
        );
    }
    assert_eq!(
        h.memory
            .list_unfinished_runs()
            .await
            .unwrap()
            .into_iter()
            .filter(|run| run.session_id == session)
            .count(),
        0
    );
    assert_eq!(h.seen.tools().len(), 1);
    assert_eq!(h.seen.handler_calls(), 2);
    assert_eq!(h.fake.requests().len(), 2);

    let handle = h
        .runtime
        .start(request(Some(&session), "ws-a", "/a"))
        .await
        .unwrap();
    handle.done().await.unwrap();
    let expected = identity("ws-a", "/a");
    assert_eq!(h.seen.tools(), [expected.clone(), expected.clone()]);
    h.seen.assert_handlers_saw(&expected);
}

#[tokio::test]
async fn row4_paused_pending_execution_resumed_in_process() {
    let h = harness(
        vec![call_script(), text_script("done")],
        Arc::new(PausePolicy),
    )
    .await;
    let handle = h.runtime.start(request(None, "ws-p", "/p")).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(h.seen.tools(), NONE);
    assert_eq!(
        h.runtime.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    let expected = identity("ws-p", "/p");
    assert_eq!(h.seen.tools(), std::slice::from_ref(&expected));
    h.seen.assert_handlers_saw(&expected);
}

#[tokio::test]
async fn row5_paused_pending_execution_resumed_by_fresh_host() {
    let h = harness(
        vec![call_script(), text_script("done")],
        Arc::new(PausePolicy),
    )
    .await;
    let handle = h.runtime.start(request(None, "ws-f", "/f")).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let (fresh, seen, _) = host(&h.clock, &h.store, &h.fake, allow()).await;
    assert_eq!(
        fresh.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    let expected = identity("ws-f", "/f");
    assert_eq!(seen.tools(), std::slice::from_ref(&expected));
    seen.assert_handlers_saw(&expected);
    assert_eq!(h.seen.tools(), NONE);
}

#[tokio::test]
async fn row6_generic_recovery_of_paused_run_with_checkpoint_request() {
    let h = harness(
        vec![call_script(), text_script("done")],
        Arc::new(PausePolicy),
    )
    .await;
    let handle = h.runtime.start(request(None, "ws-r", "/r")).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let run = h.memory.get_run(&run_id).await.unwrap().unwrap();
    assert!(run.checkpoint.unwrap().get("request").is_some());
    expire(&h.clock);
    let recovered = h.runtime.recover().await.unwrap().recovered;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, RunStatus::Completed);
    let expected = identity("ws-r", "/r");
    assert_eq!(h.seen.tools(), std::slice::from_ref(&expected));
    h.seen.assert_handlers_saw(&expected);
}

/// Admits a run directly in the store and leaves it `Running` with one
/// retry-safe pending call and no checkpoint, as a crashed host would.
async fn crashed_running_run(h: &Harness, workspace_id: &str, directory: &str) -> RunId {
    let now = h.clock.now();
    let (plan, prepared, _) = h
        .runtime
        .prepare_admission(&request(None, workspace_id, directory))
        .await
        .unwrap();
    plan.release();
    let admitted = h.memory.admit_bound_run(prepared).await.unwrap();
    let execution = h.memory.execution(admitted.fence).await.unwrap();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: ToolCallId::new(),
                run_id: admitted.run.id.clone(),
                name: "echo".into(),
                arguments: json!({"text":"ok","workspace_id":"model","directory":"/model"}),
                status: ToolCallStatus::Pending,
                retry_safe: true,
                result: None,
            },
            EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: Value::Null,
                correlation: None,
                live_only: false,
                created_at: now,
            },
        )
        .await
        .unwrap();
    admitted.run.id
}

#[tokio::test]
async fn row7_legacy_running_run_cannot_authorize_execution_from_context_alone() {
    let h = harness(Vec::new(), allow()).await;
    let run_id = crashed_running_run(&h, "ws-c", "/c").await;
    let before = h.memory.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(before.status, RunStatus::Running);
    assert!(before.checkpoint.is_none());
    expire(&h.clock);
    let report = h.runtime.recover().await.unwrap();
    assert_eq!(report.recovered, Vec::new());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(
        report.skipped[0].reason,
        crate::RecoverySkipReason::Attempt(crabber_session::AttemptError::InvalidTransition)
    );
    assert_eq!(h.seen.tools(), Vec::new());
    let after = h.memory.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(
        (
            after.status,
            after.checkpoint,
            after.config_hash,
            after.plan_fingerprint
        ),
        (
            before.status,
            before.checkpoint,
            before.config_hash,
            before.plan_fingerprint
        )
    );
    assert_eq!(
        h.memory.latest_execution_attempt(&run_id).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn row8_keyed_admission_recovery_uses_persisted_identity_and_rejects_drift() {
    let h = harness(vec![call_script(), text_script("done")], allow()).await;
    let session = SessionId::new();
    let input = request(Some(&session), "ws-u", "/u");
    h.store.lose_admission.store(true, Ordering::SeqCst);
    assert!(
        h.runtime
            .start_keyed(input.clone(), options("row8"))
            .await
            .is_err()
    );
    h.store.lose_admission.store(false, Ordering::SeqCst);
    let acquired = h.plans.acquired.load(Ordering::SeqCst);
    assert!(matches!(
        h.runtime
            .recover_admission(input.clone(), options("row8"))
            .await,
        Err(RuntimeError::AdmissionExecution(
            crabber_session::AdmissionExecutionError::LiveLease
        ))
    ));
    assert_eq!(h.plans.acquired.load(Ordering::SeqCst), acquired);
    expire(&h.clock);
    let before = h.memory.list_unfinished_runs().await.unwrap();
    assert_eq!(before.len(), 1);

    // Drift is rejected before the receipt lookup: no claim, no execution.
    for (workspace_id, directory) in [("ws-u", "/other"), ("other", "/u"), ("", "")] {
        mismatch(
            &h.runtime
                .recover_admission(
                    request(Some(&session), workspace_id, directory),
                    options("row8"),
                )
                .await,
        );
    }
    assert_eq!(h.memory.list_unfinished_runs().await.unwrap(), before);
    assert_eq!(h.seen.tools(), NONE);
    assert_eq!(h.seen.handler_calls(), 0);

    let Admission::Started { handle, .. } = h
        .runtime
        .recover_admission(input.clone(), options("row8"))
        .await
        .unwrap()
    else {
        panic!("unstarted admission is recovered")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let expected = identity("ws-u", "/u");
    assert_eq!(h.seen.tools(), std::slice::from_ref(&expected));
    h.seen.assert_handlers_saw(&expected);

    // A terminal run replays for the matching request only.
    assert!(matches!(
        h.runtime
            .recover_admission(input, options("row8"))
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    mismatch(
        &h.runtime
            .recover_admission(request(Some(&session), "ws-u", "/other"), options("row8"))
            .await,
    );
}

#[tokio::test]
async fn stored_record_conflict_rejects_resume_and_changes_nothing() {
    let h = harness(
        vec![call_script(), text_script("done")],
        Arc::new(PausePolicy),
    )
    .await;
    let handle = h.runtime.start(request(None, "ws-d", "/d")).await.unwrap();
    let run_id = handle.run_id().clone();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let handlers = h.seen.handler_calls();
    let plans = h.plans.acquired.load(Ordering::SeqCst);
    let before = h.memory.get_run(&run_id).await.unwrap().unwrap();
    expire(&h.clock);

    for (workspace_id, directory) in [("ws-d", "/replacement"), ("other", "/d"), ("", "/d")] {
        h.store.alter(&session, workspace_id, directory);
        // Rejected on every attempt: the run does not heal itself.
        for _ in 0..2 {
            mismatch(&h.runtime.resume(&run_id).await);
            mismatch(&h.runtime.resume_with_context(&run_id, None).await);
        }
        // The sweep skips it instead of failing.
        assert_eq!(h.runtime.recover().await.unwrap().recovered.len(), 0);
    }
    assert_eq!(h.plans.acquired.load(Ordering::SeqCst), plans);
    assert_eq!(h.memory.get_run(&run_id).await.unwrap().unwrap(), before);
    assert_eq!(h.seen.tools(), NONE);
    assert_eq!(h.seen.handler_calls(), handlers);
    assert_eq!(h.fake.requests().len(), 1);

    h.store.restore(&session);
    assert_eq!(
        h.runtime.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(h.seen.tools(), [identity("ws-d", "/d")]);
}

#[tokio::test]
async fn recover_sweep_skips_and_reports_bad_runs_and_recovers_the_rest() {
    let names = ["first", "drifted", "invalid", "missing", "corrupt", "last"];
    let mut scripts: Vec<_> = names.iter().map(|_| call_script()).collect();
    scripts.extend([text_script("one"), text_script("two")]);
    let h = harness(scripts, Arc::new(PausePolicy)).await;
    let mut runs = Vec::new();
    for name in names {
        let handle = h
            .runtime
            .start(request(None, name, &format!("/{name}")))
            .await
            .unwrap();
        runs.push((handle.session_id().clone(), handle.run_id().clone()));
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    }
    expire(&h.clock);
    let invalid = StoreError::Validation("stored record is invalid".into());
    h.store.alter(&runs[1].0, "drifted", "/replacement");
    h.store
        .broken
        .lock()
        .unwrap()
        .insert(runs[2].0.clone(), Some(invalid.clone()));
    h.store
        .broken
        .lock()
        .unwrap()
        .insert(runs[3].0.clone(), None);
    h.store.corrupt.lock().unwrap().insert(runs[4].1.clone());

    // The corrupt checkpoint is reported as invalid, not as host drift.
    let corrupt = StoreError::Validation("checkpoint request is invalid".into());
    assert!(matches!(
        h.runtime.resume(&runs[4].1).await,
        Err(RuntimeError::Store(ref error)) if *error == corrupt
    ));

    let report = h.runtime.recover().await.unwrap();
    let mut recovered: Vec<_> = report.recovered.iter().map(|r| r.run_id.clone()).collect();
    recovered.sort();
    let mut expected = vec![runs[0].1.clone(), runs[5].1.clone()];
    expected.sort();
    assert_eq!(recovered, expected);
    let mut skipped: Vec<_> = report
        .skipped
        .iter()
        .map(|skipped| (skipped.run_id.clone(), skipped.reason.clone()))
        .collect();
    skipped.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = vec![
        (
            runs[1].1.clone(),
            crate::RecoverySkipReason::Store(StoreError::SessionIdentityMismatch),
        ),
        (runs[2].1.clone(), crate::RecoverySkipReason::Store(invalid)),
        (
            runs[3].1.clone(),
            crate::RecoverySkipReason::Store(StoreError::NotFound),
        ),
        (runs[4].1.clone(), crate::RecoverySkipReason::Store(corrupt)),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(skipped, expected);
    for (_, run_id) in &runs[1..5] {
        assert_eq!(
            h.memory.get_run(run_id).await.unwrap().unwrap().status,
            RunStatus::Paused
        );
    }
    let mut tools = h.seen.tools();
    tools.sort();
    assert_eq!(
        tools,
        [identity("first", "/first"), identity("last", "/last")]
    );
}

#[tokio::test]
async fn parallel_tool_calls_share_the_persisted_identity() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let store = DriftStore::new(Arc::new(MemoryStore::with_clock(clock.clone())));
    let seen = Arc::new(Seen::default());
    let registry = Registry::new();
    registry
        .mount(Arc::new(ProbeExtension(Arc::clone(&seen))), Scope::Global)
        .await
        .unwrap();
    let mut both = call_script();
    both.pop();
    both.extend(call_script());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store as Arc<dyn Store>)
        .clock(clock)
        .resolver(Arc::new(FakeProvider::scripted(vec![
            both,
            text_script("done"),
        ])))
        .plan_provider(Arc::new(registry))
        .policy(allow())
        .execution_mode(crate::ExecutionMode::Parallel { max: 2 })
        .build()
        .unwrap();
    let handle = runtime
        .start(request(None, "ws-par", "/par"))
        .await
        .unwrap();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(seen.tools(), vec![identity("ws-par", "/par"); 2]);
}

#[tokio::test]
async fn empty_persisted_fields_are_unavailable_never_defaulted() {
    for (workspace_id, directory) in [("", ""), ("ws", ""), ("", "/root")] {
        let h = harness(
            vec![
                call_script(),
                text_script("one"),
                call_script(),
                text_script("two"),
            ],
            allow(),
        )
        .await;
        let handle = h
            .runtime
            .start(request(None, workspace_id, directory))
            .await
            .unwrap();
        let session = handle.session_id().clone();
        handle.done().await.unwrap();
        // A non-empty value presented against a persisted empty field: rejected.
        mismatch(
            &h.runtime
                .start(request(Some(&session), "filled", "/filled"))
                .await
                .map(|_| ()),
        );
        if !(workspace_id.is_empty() && directory.is_empty()) {
            // The empty field matches on both sides but the other differs: rejected.
            let other_id = if workspace_id.is_empty() { "" } else { "other" };
            let other_directory = if directory.is_empty() { "" } else { "/other" };
            mismatch(
                &h.runtime
                    .start(request(Some(&session), other_id, other_directory))
                    .await
                    .map(|_| ()),
            );
            // An empty value presented against a persisted non-empty field: rejected.
            mismatch(
                &h.runtime
                    .start(request(Some(&session), "", ""))
                    .await
                    .map(|_| ()),
            );
        }
        // Empty on both sides proceeds, with the field still unavailable.
        let handle = h
            .runtime
            .start(request(Some(&session), workspace_id, directory))
            .await
            .unwrap();
        handle.done().await.unwrap();
        let expected = identity(workspace_id, directory);
        assert_eq!(h.seen.tools(), [expected.clone(), expected.clone()]);
        h.seen.assert_handlers_saw(&expected);
    }
}

#[tokio::test]
async fn permission_denial_never_reaches_the_executor() {
    let h = harness(
        vec![call_script(), text_script("done")],
        Arc::new(StaticPolicy::new(PermissionDecision::Deny)),
    )
    .await;
    let handle = h.runtime.start(request(None, "ws-a", "/a")).await.unwrap();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(h.seen.tools(), NONE);
    h.seen.assert_handlers_saw(&identity("ws-a", "/a"));
}
