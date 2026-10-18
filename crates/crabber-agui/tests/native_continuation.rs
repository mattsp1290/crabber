//! Actual native waiting/resume compounds through SSE and the pinned client.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crabber::{
    Agent, AgentConfig, Continuation, ContinueApproval, ContinueFrontend, ContinueTool,
    FakeProvider, JsonToolExecutor, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    ToolContinuation, ToolDefinition,
};
use crabber_agui::Completion;
use crabber_core::{
    AdmissionKey, AdmissionOptions, ContentBlock, ControlId, FrontendResultStatus, FrontendTool,
    FrontendToolResult, InputEnvelope, InputFingerprint, RunStatus, SessionContext, SessionId,
    SessionRevision, ToolCallId, ToolInfo, ToolOutput,
};
use crabber_session::{
    ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval, FrontendResolverClaim,
    MemoryStore, Store,
};
use serde_json::{Value, json};

#[path = "native_continuation/projection.rs"]
mod projection;
mod support;

const PRIVATE: &str = "PRIVATE_NATIVE_CONTINUATION_RESOLVER_SENTINEL";

struct Echo(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl JsonToolExecutor for Echo {
    async fn execute(&self, arguments: Value) -> Result<Value, crabber::ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}
fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
fn provider(call: &ToolCallId) -> FakeProvider {
    FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "continuation_echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: json!({"text":"native result"}).to_string(),
            },
            StreamDelta::ToolCallDone {
                call_id: call.clone(),
            },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("finished".into()),
            StreamDelta::Completed,
        ],
    ])
}
fn human_agent(
    store: Arc<MemoryStore>,
    provider: FakeProvider,
    effects: Arc<AtomicUsize>,
) -> Agent {
    Agent::builder()
        .behavior_version("native-human-projection.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "continuation_echo".into(), description: "Human approved echo".into(),
                parameters: json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
                retry_safe: false, required_permissions: vec![],
            },
            executor: Arc::new(Echo(effects)),
        }))
        .build().unwrap()
}

#[tokio::test]
async fn native_human_two_exchanges_preserve_question_parent_and_private_claim() {
    let store = Arc::new(MemoryStore::new());
    let call = ToolCallId::new();
    let provider = provider(&call);
    let effects = Arc::new(AtomicUsize::new(0));
    let agent = human_agent(store.clone(), provider.clone(), effects.clone());
    let handle = agent.prompt(None, "request").await.unwrap();
    let (session, run) = (handle.session_id().clone(), handle.run_id().clone());
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let first = projection::exchange(&store, &session, None, Completion::Paused).await;
    let snapshot = agent
        .approval_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("native Ask")
    };
    assert_eq!(first.waiting.interrupts.len(), 1);
    assert_eq!(first.waiting.interrupts[0].interaction_id, *id);
    assert_eq!(first.waiting.backend_pending, vec![call.clone()]);
    assert_eq!(first.waiting.frontend_pending, Vec::<ToolCallId>::new());
    let terminal = serde_json::to_value(first.output.last().unwrap()).unwrap();
    assert_eq!(terminal["outcome"]["interrupts"][0]["id"], json!(id));
    assert_eq!(
        terminal["outcome"]["interrupts"][0]["message"],
        json!(description.prompt())
    );
    let sdk = support::consume(&first.output).await;
    let reduced = serde_json::to_value(&sdk.outcome).unwrap();
    assert_eq!(reduced["interrupts"][0]["id"], json!(id));
    let answered = agent
        .answer_approval(ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "authenticated-test-host".into(),
                authority: description.authority().clone(),
                authority_claim: PRIVATE.into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let Continuation::Started { handle, .. } = agent
        .continue_approval(ContinueApproval {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run,
            continuation: snapshot.continuation,
            expected_revision: answered.revision,
        })
        .await
        .unwrap()
    else {
        panic!("fresh native approval continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    projection::continued(&store, &session, first, sdk.messages, &call, PRIVATE).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    agent.close_extensions().await.unwrap();
}

fn frontend_agent(store: Arc<MemoryStore>, provider: FakeProvider) -> Agent {
    Agent::builder()
        .behavior_version("native-frontend-projection.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection()))
        .build()
        .unwrap()
}
fn frontend_input() -> InputEnvelope {
    let mut wire =
        serde_json::to_value(InputEnvelope::text("request", SessionRevision::INITIAL).unwrap())
            .unwrap();
    wire["frontend_tools"] = json!([FrontendTool {
        name: "continuation_echo".into(),
        description: "External result only".into(),
        input_parameters: json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
        result_contract: None,
    }]);
    serde_json::from_value(wire).unwrap()
}

#[tokio::test]
async fn native_frontend_two_exchanges_preserve_pending_set_and_typed_result() {
    let store = Arc::new(MemoryStore::new());
    let call = ToolCallId::new();
    let provider = provider(&call);
    let agent = frontend_agent(store.clone(), provider.clone());
    let session = SessionId::new();
    let context = crabber::ExecutionContext::new(
        SessionContext::new(session.clone(), "native-projection", "/native-projection"),
        selection(),
    );
    let crabber::Admission::Started { receipt, handle } = agent
        .prompt_input_keyed_in(
            &context,
            frontend_input(),
            AdmissionOptions {
                key: AdmissionKey::new("native-frontend-projection").unwrap(),
                fingerprint: InputFingerprint::new("d".repeat(64)).unwrap(),
                behavior_fingerprint: agent.behavior_fingerprint().clone(),
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("fresh native typed admission")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let first = projection::exchange(&store, &session, None, Completion::Paused).await;
    let snapshot = agent
        .tool_continuation_snapshot(&session, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first.waiting.interrupts,
        Vec::<crabber_core::continuation_source::HumanInterrupt>::new()
    );
    assert_eq!(first.waiting.backend_pending, Vec::<ToolCallId>::new());
    assert_eq!(first.waiting.frontend_pending, vec![call.clone()]);
    assert_eq!(snapshot.frontend_calls[0].call_id, call);
    let terminal = serde_json::to_value(first.output.last().unwrap()).unwrap();
    assert_eq!(terminal["outcome"]["pendingToolCallIds"], json!([call]));
    let sdk = support::consume(&first.output).await;
    let reduced = serde_json::to_value(&sdk.outcome).unwrap();
    assert_eq!(reduced["pendingToolCallIds"], json!([call]));
    let output = ToolOutput::from_json(&json!({"external":"native result"})).unwrap();
    let ToolContinuation::Started { handle, .. } = agent
        .continue_frontend(external_request(&snapshot, output.clone()))
        .await
        .unwrap()
    else {
        panic!("fresh native external result continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    projection::continued(&store, &session, first, sdk.messages, &call, PRIVATE).await;
    let messages = store.list_all_messages(&session).await.unwrap();
    assert!(messages.iter().flat_map(|message| &message.parts).any(|part|
        matches!(&part.content, ContentBlock::ToolResult { call_id, content, is_error: false }
            if call_id == &call && content == &output.to_content_blocks())));
    assert_eq!(provider.requests().len(), 2);
    agent.close_extensions().await.unwrap();
}

fn external_request(
    snapshot: &crabber_session::ToolContinuationSnapshot,
    output: ToolOutput,
) -> ContinueFrontend {
    ContinueFrontend {
        continuation: ContinueTool {
            id: ControlId::new(),
            session_id: snapshot.session_id.clone(),
            run_id: snapshot.run_id.clone(),
            continuation: snapshot.continuation.clone(),
            expected_revision: snapshot.revision,
        },
        resolver: FrontendResolverClaim {
            resolver_id: "authenticated-test-host".into(),
            authority_claim: PRIVATE.into(),
        },
        results: vec![
            FrontendToolResult::new(
                snapshot.frontend_calls[0].call_id.clone(),
                FrontendResultStatus::Completed,
                output,
            )
            .unwrap(),
        ],
    }
}
