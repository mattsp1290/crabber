use crabber::{
    Agent, AgentConfig, ContinueTool, ExecutionContext, FakeProvider, PermissionDecision,
    ResumableTool, Selection, StaticPolicy, StreamDelta, ToolContinuation,
    core::{
        AcceptedToolResponse, AuthorityRequirement, ControlId, InputFingerprint, InteractionState,
        RunId, RunStatus, SessionContext, SessionId, ToolCallId, ToolCallRecord, ToolCheckpoint,
        ToolPhaseOutcome,
    },
    extension::{
        Extension, ExtensionError, HostServices, Registry, Scope, ToolContext, ToolDefinition,
        ToolExecutor, WorkspaceContext,
    },
    session::{
        ApprovalResolverClaim, MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest,
        Store, ToolContinuationError, ToolContinuationSnapshot, ToolInteractionAnswer,
        ToolInteractionReply,
    },
};
use crabber_extensions::ask_user::{AskUser, Limits, Options, TOOL_NAME};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[path = "support/agent.rs"]
mod agent_support;
#[path = "ask_user/contract.rs"]
mod contract;
#[path = "ask_user/faults.rs"]
mod faults;
#[path = "ask_user/lifecycle.rs"]
mod lifecycle;
#[path = "ask_user/routing.rs"]
mod routing;
#[path = "ask_user/runtime.rs"]
mod runtime;

fn options() -> Options {
    Options {
        authority: AuthorityRequirement::new("operator", "1").unwrap(),
        limits: Limits::default(),
    }
}
fn extension() -> AskUser {
    AskUser::new(options()).unwrap()
}
fn arguments() -> Value {
    json!({"question":" Pick one ","options":[{"label":" first ","description":"detail"},{"label":"second"}]})
}
fn context() -> ToolContext {
    ToolContext::new(
        "session".into(),
        "run".into(),
        "call".into(),
        CancellationToken::new(),
        HostServices::default(),
        WorkspaceContext::from_persisted("workspace", "/workspace"),
        Arc::new(|_| {}),
    )
}
async fn definition(extension: AskUser) -> Arc<ToolDefinition> {
    let registry = Registry::new();
    let handle = registry
        .mount(Arc::new(extension), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let tool = plan.tools[0].clone();
    drop(plan);
    handle.close().await.unwrap();
    tool
}
struct Counted {
    executor: Arc<dyn ToolExecutor>,
    starts: AtomicUsize,
    resumes: AtomicUsize,
    together: Option<Arc<tokio::sync::Barrier>>,
}
#[async_trait::async_trait]
impl ResumableTool for Counted {
    fn identity(&self) -> crabber::core::ResumableToolIdentity {
        self.executor.resumable().unwrap().identity()
    }
    async fn start(&self, c: ToolContext, a: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if let Some(barrier) = &self.together {
            barrier.wait().await;
        }
        self.executor.resumable().unwrap().start(c, a).await
    }
    async fn resume(
        &self,
        c: ToolContext,
        a: Value,
        p: ToolCheckpoint,
        r: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.resumes.fetch_add(1, Ordering::SeqCst);
        self.executor.resumable().unwrap().resume(c, a, p, r).await
    }
}
async fn counted() -> (Arc<ToolDefinition>, Arc<Counted>) {
    counted_with(extension(), None).await
}
async fn counted_with(
    extension: AskUser,
    together: Option<Arc<tokio::sync::Barrier>>,
) -> (Arc<ToolDefinition>, Arc<Counted>) {
    let tool = definition(extension).await;
    let counted = Arc::new(Counted {
        executor: tool.executor.clone(),
        starts: AtomicUsize::new(0),
        resumes: AtomicUsize::new(0),
        together,
    });
    (
        Arc::new(ToolDefinition::resumable(tool.info.clone(), counted.clone()).unwrap()),
        counted,
    )
}
fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
fn execution() -> ExecutionContext {
    ExecutionContext::new(
        SessionContext::new("session".into(), "workspace", "/workspace"),
        selection(),
    )
}
fn call() -> Vec<StreamDelta> {
    calls(1)
}
fn calls(count: usize) -> Vec<StreamDelta> {
    let mut deltas = Vec::new();
    for _ in 0..count {
        let call_id = ToolCallId::new();
        deltas.extend([
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: TOOL_NAME.into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: arguments().to_string(),
            },
            StreamDelta::ToolCallDone { call_id },
        ]);
    }
    deltas.push(StreamDelta::Completed);
    deltas
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("second is my answer".into()),
        StreamDelta::Completed,
    ]
}
fn builder(
    store: Arc<MemoryStore>,
    provider: FakeProvider,
    tool: Arc<ToolDefinition>,
) -> crabber::AgentBuilder {
    agent_support::builder()
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool)
}
async fn pause(agent: &Agent) -> (RunId, ToolContinuationSnapshot) {
    let handle = agent_support::first_prompt(agent, &execution(), "ask")
        .await
        .unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let snapshot = agent
        .tool_continuation_snapshot(&SessionId::from("session"), &run)
        .await
        .unwrap()
        .unwrap();
    (run, snapshot)
}
fn answer(
    snapshot: &ToolContinuationSnapshot,
    index: usize,
    reply: ToolInteractionReply,
) -> ToolInteractionAnswer {
    ToolInteractionAnswer {
        id: ControlId::new(),
        expected_revision: snapshot.revision,
        session_id: snapshot.session_id.clone(),
        run_id: snapshot.run_id.clone(),
        continuation: snapshot.continuation.clone(),
        interaction_id: snapshot.interactions[index].id.clone(),
        resolver: ApprovalResolverClaim {
            resolver_id: "verified-host".into(),
            authority: snapshot.interactions[index].description.authority().clone(),
            authority_claim: "authenticated operator".into(),
        },
        reply,
    }
}
fn continuation(
    snapshot: &ToolContinuationSnapshot,
    revision: crabber::core::SessionRevision,
) -> ContinueTool {
    ContinueTool {
        id: ControlId::new(),
        session_id: snapshot.session_id.clone(),
        run_id: snapshot.run_id.clone(),
        continuation: snapshot.continuation.clone(),
        expected_revision: revision,
    }
}
async fn complete(agent: &Agent, request: ContinueTool) {
    let ToolContinuation::Started { handle, .. } = agent.continue_tool(request).await.unwrap()
    else {
        panic!("fresh continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
}
async fn records(store: &MemoryStore) -> Vec<ToolCallRecord> {
    let mut continuation = None;
    let mut calls = Vec::new();
    loop {
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: "session".into(),
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 100_000,
                    encoded_bytes: 1_000_000,
                },
                continuation: continuation.take(),
            })
            .await
            .unwrap()
        else {
            panic!("snapshot");
        };
        calls.extend(page.tool_calls);
        continuation = page.continuation;
        if continuation.is_none() {
            return calls;
        }
    }
}

fn value(call: &ToolCallRecord) -> Value {
    let [crabber::core::ContentBlock::Text { text }] = &call.result.as_ref().unwrap().content[..]
    else {
        panic!("JSON result")
    };
    serde_json::from_str(text).unwrap()
}
fn tool_error(error: impl Into<crabber::RuntimeError>, expected: ToolContinuationError) {
    let error = error.into();
    assert!(
        matches!(error, crabber::RuntimeError::ToolContinuation(actual) if actual == expected),
        "{error:?}"
    );
}
