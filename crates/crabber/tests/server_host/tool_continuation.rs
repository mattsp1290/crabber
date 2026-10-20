//! An explicit state machine survives host reconstruction without reentering start.
#[path = "tool_continuation/handoff.rs"]
mod handoff;
#[path = "tool_continuation/identity.rs"]
mod identity;
#[path = "tool_continuation/mixed.rs"]
mod mixed;
#[cfg(feature = "sqlite")]
#[path = "tool_continuation/process.rs"]
mod process;
#[path = "tool_continuation/protection.rs"]
pub(super) mod protection;
#[path = "tool_continuation/recovery.rs"]
mod recovery;
#[path = "tool_continuation/resources.rs"]
mod resources;
use super::*;
use crabber::core::{
    AcceptedToolResponse, AuthorityRequirement, ControlId, InteractionDescription, InteractionKind,
    ResumableToolIdentity, ToolCheckpoint, ToolOutput, ToolPhaseOutcome, ToolSuspension,
};
use crabber::session::{ApprovalResolverClaim, ToolInteractionAnswer, ToolInteractionReply};
use crabber::{ContinueTool, ResumableTool, ToolContinuation};
struct Interactive {
    starts: Arc<AtomicUsize>,
    resumes: Arc<AtomicUsize>,
    effects: Arc<AtomicUsize>,
}
#[async_trait]
impl ResumableTool for Interactive {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "interactive-fixture".into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
            checkpoint_schema_id: "interactive-fixture".into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok(pause(1))
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.resumes.fetch_add(1, Ordering::SeqCst);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].value, Some(json!("accepted")));
        Ok(match checkpoint.value()["step"].as_u64().unwrap() {
            1 => pause(2),
            2 => ToolPhaseOutcome::Completed(
                ToolOutput::from_content_blocks(
                    vec![crabber::core::ContentBlock::Text {
                        text: "complete".into(),
                    }],
                    false,
                )
                .unwrap(),
            ),
            _ => panic!("unknown checkpoint"),
        })
    }
}
fn pause(step: u64) -> ToolPhaseOutcome {
    ToolPhaseOutcome::Suspended(
        ToolSuspension::new(
            ToolCheckpoint::new(
                "interactive-fixture",
                "1",
                json!({"step":step,"private":"never result"}),
            )
            .unwrap(),
            vec![
                InteractionDescription::new(
                    format!("question-{step}"),
                    InteractionKind::UserInput,
                    "Choose",
                    json!({"type":"string","enum":["accepted"]}),
                    AuthorityRequirement::new("operator", "1").unwrap(),
                    None,
                )
                .unwrap(),
            ],
        )
        .unwrap(),
    )
}
fn make(store: Arc<dyn Store>, provider: FakeProvider, tool: Arc<dyn ResumableTool>) -> Agent {
    builder(store, provider, tool).build().unwrap()
}
fn builder(
    store: Arc<dyn Store>,
    provider: FakeProvider,
    tool: Arc<dyn ResumableTool>,
) -> crabber::AgentBuilder {
    Agent::builder()
        .behavior_version("interactive-fixture.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(
            ToolDefinition::resumable(
                ToolInfo {
                    name: "identity".into(),
                    description: "explicit state machine".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                tool,
            )
            .unwrap(),
        ))
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // A complete public lifecycle journey.
async fn public_explicit_tool_pauses_twice_and_never_reenters_start() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call("interactive-original"), done()]);
    let tool = Arc::new(Interactive {
        starts: Arc::default(),
        resumes: Arc::default(),
        effects: Arc::default(),
    });
    let protected = Arc::new(AtomicUsize::new(0));
    let make = |store, provider, tool| protection::make(store, provider, tool, protected.clone());
    let mut agent = make(store.clone(), provider.clone(), tool.clone());
    let handle = agent.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(
        handle.done().await.unwrap().status,
        RunStatus::Paused,
        "calls={:?}, starts={}",
        store.list_unfinished_tool_calls(&run).await.unwrap(),
        tool.starts.load(Ordering::SeqCst)
    );
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    assert_eq!(protected.load(Ordering::SeqCst), 0);
    let mut original = None;
    for step in 1..=2 {
        let snapshot = agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.continuation.generation.get(), step);
        agent.close_extensions().await.unwrap();
        let question = &snapshot.interactions[0];
        let answer = ToolInteractionAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: question.id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "verified-host".into(),
                authority: question.description.authority().clone(),
                authority_claim: "authenticated operator".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("accepted"),
            },
        };
        let accepted = agent.answer_tool_interaction(answer.clone()).await.unwrap();
        assert_eq!(
            agent.answer_tool_interaction(answer).await.unwrap(),
            accepted
        );
        drop(agent);
        agent = make(store.clone(), provider.clone(), tool.clone());
        let request = ContinueTool {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation,
            expected_revision: accepted.revision,
        };
        let ToolContinuation::Started { receipt, handle } =
            agent.continue_tool(request.clone()).await.unwrap()
        else {
            panic!("fresh continuation")
        };
        assert_eq!(
            handle.done().await.unwrap().status,
            if step == 1 {
                RunStatus::Paused
            } else {
                RunStatus::Completed
            }
        );
        assert_eq!(
            protected.load(Ordering::SeqCst),
            if step == 2 { 2 } else { 0 }
        );
        if original.is_none() {
            original = Some((request.clone(), receipt.clone()));
        }
        assert!(
            matches!(agent.continue_tool(request).await.unwrap(),ToolContinuation::Replayed(actual) if actual==receipt)
        );
    }
    agent.close_extensions().await.unwrap();
    let (request, receipt) = original.unwrap();
    assert!(
        matches!(agent.continue_tool(request).await.unwrap(),ToolContinuation::Replayed(actual) if actual==receipt)
    );
    assert_eq!(protected.load(Ordering::SeqCst), 2);
    assert_eq!(tool.starts.load(Ordering::SeqCst), 1);
    assert_eq!(tool.resumes.load(Ordering::SeqCst), 2);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    let messages = store.list_all_messages(&session).await.unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.role == crabber::core::Role::Tool)
            .count(),
        1
    );
    assert!(
        !serde_json::to_string(&messages)
            .unwrap()
            .contains("never result")
    );
}
