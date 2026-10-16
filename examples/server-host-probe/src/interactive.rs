//! Explicit versioned checkpoints: start once, answer twice, resume twice.
use super::{Error, call, done, selection};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ContinueTool, PermissionDecision, ResumableTool, StaticPolicy,
    ToolContinuation, ToolDefinition,
    core::{
        AcceptedToolResponse, AuthorityRequirement, ControlId, InputFingerprint,
        InteractionDescription, InteractionKind, ResumableToolIdentity, RunStatus, ToolCheckpoint,
        ToolInfo, ToolOutput, ToolPhaseOutcome, ToolSuspension,
    },
    extension::{ExtensionError, ToolContext},
    session::{ApprovalResolverClaim, Store, ToolInteractionAnswer, ToolInteractionReply},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
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

fn agent(
    store: Arc<dyn Store>,
    provider: crabber::FakeProvider,
    tool: Arc<Interactive>,
) -> Result<Agent, Error> {
    Ok(Agent::builder()
        .behavior_version("interactive-probe.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition::resumable(
            ToolInfo {
                name: "interactive".into(),
                description: "Explicit two-step workflow".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            tool,
        )?))
        .build()?)
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<(usize, usize, usize), Error> {
    let provider = crabber::FakeProvider::scripted(vec![call("interactive"), done()]);
    let tool = Arc::new(Interactive {
        starts: Arc::default(),
        resumes: Arc::default(),
        effects: Arc::default(),
    });
    let mut host = agent(store.clone(), provider.clone(), tool.clone())?;
    let handle = host.prompt(None, "interactive").await?;
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await?.status, RunStatus::Paused);
    for step in 1..=2 {
        let snapshot = host
            .tool_continuation_snapshot(&session, &run)
            .await?
            .unwrap();
        assert_eq!(snapshot.continuation.generation.get(), step);
        host.close_extensions().await?;
        let question = &snapshot.interactions[0];
        let answer = ToolInteractionAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: question.id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "authenticated-probe-host".into(),
                authority: question.description.authority().clone(),
                authority_claim: "host verified operator".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("accepted"),
            },
        };
        let accepted = host.answer_tool_interaction(answer.clone()).await?;
        assert_eq!(host.answer_tool_interaction(answer).await?, accepted);
        drop(host);
        host = agent(store.clone(), provider.clone(), tool.clone())?;
        let request = ContinueTool {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation,
            expected_revision: accepted.revision,
        };
        let ToolContinuation::Started { receipt, handle } =
            host.continue_tool(request.clone()).await?
        else {
            panic!("fresh continuation")
        };
        assert_eq!(
            handle.done().await?.status,
            if step == 1 {
                RunStatus::Paused
            } else {
                RunStatus::Completed
            }
        );
        assert!(
            matches!(host.continue_tool(request).await?,ToolContinuation::Replayed(actual) if actual==receipt)
        );
    }
    host.close_extensions().await?;
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(tool.starts.load(Ordering::SeqCst), 1);
    assert_eq!(tool.resumes.load(Ordering::SeqCst), 2);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    let messages = store.list_all_messages(&session).await?;
    assert!(!serde_json::to_string(&messages)?.contains("never result"));
    Ok((1, 2, 1))
}
