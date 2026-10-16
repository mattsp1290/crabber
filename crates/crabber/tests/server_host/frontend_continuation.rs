#[path = "frontend_continuation/allocation.rs"]
mod allocation;
#[path = "frontend_continuation/handoff.rs"]
mod handoff;
#[cfg(feature = "sqlite")]
#[path = "frontend_continuation/process.rs"]
mod process;
// Frontend metadata waits for authenticated external results, without executors.
use super::*;
use crabber::core::{
    FrontendResultStatus, FrontendTool, FrontendToolResult, InputEnvelope, SessionRevision,
    ToolOutput,
};
use crabber::extension::{Point, ToolExecute};
use crabber::session::FrontendResolverClaim;
use crabber::{ContinueFrontend, ContinueTool, ToolContinuation};
struct BackendBoundary(Arc<AtomicUsize>);
#[async_trait]
impl Extension for BackendBoundary {
    fn id(&self) -> &'static str {
        "frontend-backend-boundary"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let counter = self.0.clone();
        registrar.on_around(
            ToolExecute::ID,
            0,
            "count",
            Arc::new(move |arguments, next| {
                let counter = counter.clone();
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    next.call(arguments).await
                })
            }),
        );
        Ok(())
    }
}

fn make(
    store: Arc<dyn Store>,
    provider: FakeProvider,
    protected: Arc<AtomicUsize>,
    backend_dispatches: Arc<AtomicUsize>,
) -> Agent {
    Agent::builder()
        .behavior_version("frontend-fixture.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection("scripted")))
        .shared_extension(Arc::new(BackendBoundary(backend_dispatches)), Scope::Global)
        .shared_extension(
            Arc::new(super::tool_continuation::protection::Protection(protected)),
            Scope::Global,
        )
        .build()
        .unwrap()
}
fn frontend_input() -> InputEnvelope {
    let descriptor = FrontendTool {
        name: "identity".into(),
        description: "External only; no backend executor".into(),
        input_parameters: json!({"type":"object"}),
        result_contract: None,
    };
    let mut wire = serde_json::to_value(
        InputEnvelope::text("frontend work", SessionRevision::INITIAL).unwrap(),
    )
    .unwrap();
    wire["frontend_tools"] = json!([descriptor]);
    serde_json::from_value(wire).unwrap()
}
fn response(frozen: &crabber::session::ToolContinuationSnapshot) -> ContinueFrontend {
    ContinueFrontend {
        continuation: ContinueTool {
            id: crabber::core::ControlId::new(),
            session_id: frozen.session_id.clone(),
            run_id: frozen.run_id.clone(),
            continuation: frozen.continuation.clone(),
            expected_revision: frozen.revision,
        },
        resolver: FrontendResolverClaim {
            resolver_id: "verified-host".into(),
            authority_claim: "authenticated session owner supplied external results".into(),
        },
        results: frozen
            .frontend_calls
            .iter()
            .map(|call| {
                FrontendToolResult::new(
                    call.call_id.clone(),
                    FrontendResultStatus::Completed,
                    ToolOutput::from_json(&json!("external response")).unwrap(),
                )
                .unwrap()
            })
            .collect(),
    }
}
fn frontend_calls() -> Vec<StreamDelta> {
    let mut calls = call("frontend-original");
    calls.pop();
    calls.extend(call("frontend-second"));
    calls
}
async fn start(agent: &Agent, session: &SessionId) -> crabber::core::RunId {
    let execution = context(&session.0, "frontend-workspace", "/frontend", "scripted");
    let opts = AdmissionOptions {
        key: AdmissionKey::new("frontend-input").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    };
    let Admission::Started { receipt, handle } = agent
        .prompt_input_keyed_in(&execution, frontend_input(), opts, None)
        .await
        .unwrap()
    else {
        panic!("fresh frontend admission missing")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    receipt.run_id
}
#[tokio::test]
async fn public_frontend_result_uses_protection_once_and_closed_replay_is_pure() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![frontend_calls(), done()]);
    let protected = Arc::new(AtomicUsize::new(0));
    let backend_dispatches = Arc::new(AtomicUsize::new(0));
    let make = || {
        make(
            store.clone(),
            provider.clone(),
            protected.clone(),
            backend_dispatches.clone(),
        )
    };
    let mut agent = make();
    let session = SessionId::new();
    let run = start(&agent, &session).await;
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(protected.load(Ordering::SeqCst), 0);
    let frozen = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert!(frozen.interactions.is_empty());
    assert_eq!(frozen.frontend_calls.len(), 2);
    assert_eq!(
        frozen.frontend_calls[0].call_id,
        ToolCallId::from("frontend-original")
    );
    let request = response(&frozen);
    agent.close_extensions().await.unwrap();
    let mut wrong = request.clone();
    wrong.results = vec![
        FrontendToolResult::new(
            ToolCallId::new(),
            FrontendResultStatus::Completed,
            ToolOutput::from_json(&json!("foreign response")).unwrap(),
        )
        .unwrap(),
    ];
    assert!(matches!(
        agent.continue_frontend(wrong).await,
        Err(RuntimeError::ToolContinuation(
            crabber::session::ToolContinuationError::SemanticConflict
        ))
    ));
    assert_eq!(
        store.session_revision(&session).await.unwrap(),
        request.continuation.expected_revision
    );
    agent = make();
    let ToolContinuation::Started { receipt, handle } =
        agent.continue_frontend(request.clone()).await.unwrap()
    else {
        panic!("fresh result phase missing")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(protected.load(Ordering::SeqCst), 4);
    assert_eq!(backend_dispatches.load(Ordering::SeqCst), 0);
    let expected = ToolOutput::from_json(&json!("external response"))
        .unwrap()
        .to_content_blocks();
    assert!(store.list_all_messages(&session).await.unwrap().iter().flat_map(|message| &message.parts).any(|part| matches!(&part.content, crabber::core::ContentBlock::ToolResult { call_id, content, is_error: false } if call_id == &ToolCallId::from("frontend-original") && content == &expected)));
    assert_eq!(provider.requests().len(), 2);
    let events = store.list_events(&session, None, 1000).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == crabber::core::EventKind::ToolCallRunning)
            .count(),
        2
    );
    agent.close_extensions().await.unwrap();
    assert!(
        matches!(agent.continue_frontend(request).await.unwrap(), ToolContinuation::Replayed(saved) if saved == receipt)
    );
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(protected.load(Ordering::SeqCst), 4);
    assert_eq!(backend_dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.list_events(&session, None, 1000).await.unwrap(),
        events
    );
}
#[tokio::test]
async fn waiting_frontend_cancellation_settles_without_fabricating_external_results() {
    use crabber::session::{
        RunControlAction, RunControlOutcome, RunControlRequest, ToolContinuationState,
    };
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![frontend_calls()]);
    let protected = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(AtomicUsize::new(0));
    let agent = make(
        store.clone(),
        provider.clone(),
        protected.clone(),
        backend.clone(),
    );
    let session = SessionId::new();
    let run = start(&agent, &session).await;
    let binding = agent.run_control_binding(&session, &run).await.unwrap();
    let request = RunControlRequest {
        id: crabber::core::ControlId::new(),
        target: binding.target,
        expected_revision: binding.revision,
        action: RunControlAction::Cancel {
            resolver: crabber::session::ApprovalResolverClaim {
                resolver_id: "frontend-controller".into(),
                authority: crabber::core::AuthorityRequirement::new("host-cancel", "1").unwrap(),
                authority_claim: "authorized session owner".into(),
            },
        },
    };
    agent.close_extensions().await.unwrap();
    assert!(matches!(
        agent.control_run(request.clone()).await.unwrap(),
        RunControlOutcome::Applied(_)
    ));
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    let view = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.state, ToolContinuationState::Released);
    assert!(view.frontend_calls.iter().all(|call| call.consumed));
    assert!(view.interactions.is_empty());
    assert_eq!(
        store.list_unfinished_tool_calls(&run).await.unwrap().len(),
        0
    );
    assert_eq!(protected.load(Ordering::SeqCst), 0);
    assert_eq!(backend.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
    assert!(matches!(
        agent.control_run(request).await.unwrap(),
        RunControlOutcome::Replayed(_)
    ));
}
