use super::{
    contract::{cancel_action, request},
    provider::Gate,
};
use async_trait::async_trait;
use crabber::core::{
    AcceptedToolResponse, AuthorityRequirement, InputFingerprint, InteractionDescription,
    InteractionKind, InteractionState, ResumableToolIdentity, RunStatus, ToolCallId,
    ToolCheckpoint, ToolInfo, ToolPhaseOutcome, ToolSuspension,
};
use crabber::extension::ToolContext;
use crabber::session::{ApprovalResolverClaim, Store, ToolInteractionAnswer, ToolInteractionReply};
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, ResumableTool, Selection, StreamDelta,
    ToolDefinition,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Interactive(Arc<AtomicUsize>);
#[async_trait]
impl ResumableTool for Interactive {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "control-interactive".into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
            checkpoint_schema_id: "control-interactive".into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Suspended(
            ToolSuspension::new(
                ToolCheckpoint::new("control-interactive", "1", json!({"private":"original"}))
                    .unwrap(),
                vec![
                    InteractionDescription::new(
                        "choose",
                        InteractionKind::UserInput,
                        "Choose",
                        json!({"type":"string"}),
                        AuthorityRequirement::new("operator", "1").unwrap(),
                        None,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        ))
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        _: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        panic!("cancelled checkpoint never resumes")
    }
}
fn worker(store: Arc<dyn Store>, provider: FakeProvider, tool: Arc<Interactive>) -> Agent {
    Agent::builder()
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .behavior_version("control-interactive.v1")
        .policy(Arc::new(crabber_runtime::StaticPolicy::new(
            crabber_runtime::PermissionDecision::Allow,
        )))
        .shared_tool(Arc::new(
            ToolDefinition::resumable(
                ToolInfo {
                    name: "interactive".into(),
                    description: "pause".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                tool,
            )
            .unwrap(),
        ))
        .build()
        .unwrap()
}
fn provider() -> FakeProvider {
    let call = ToolCallId::new();
    FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "interactive".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("new work".into()),
            StreamDelta::Completed,
        ],
    ])
}
pub(super) async fn run(store: Arc<dyn Store>) {
    for answered in [false, true] {
        let effects = Arc::new(AtomicUsize::new(0));
        let tool = Arc::new(Interactive(effects.clone()));
        let provider = provider();
        let worker = worker(store.clone(), provider.clone(), tool);
        let handle = worker.prompt(None, "pause for input").await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let controller = super::contract::agent(store.clone(), &Gate::new(0));
        let snapshot = controller
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        if answered {
            let question = &snapshot.interactions[0];
            controller
                .answer_tool_interaction(ToolInteractionAnswer {
                    id: crabber::core::ControlId::new(),
                    session_id: session.clone(),
                    run_id: run.clone(),
                    continuation: snapshot.continuation,
                    interaction_id: question.id.clone(),
                    expected_revision: snapshot.revision,
                    resolver: ApprovalResolverClaim {
                        resolver_id: "host".into(),
                        authority: question.description.authority().clone(),
                        authority_claim: "authorized answer".into(),
                    },
                    reply: ToolInteractionReply::Answer {
                        value: json!("accepted"),
                    },
                })
                .await
                .unwrap();
        }
        let target = controller
            .run_control_binding(&session, &run)
            .await
            .unwrap()
            .target;
        let cancel = request(store.as_ref(), &target, cancel_action()).await;
        let turn = open_turn(store.as_ref(), &session, &run).await;
        controller.close_extensions().await.unwrap();
        let crabber::session::RunControlOutcome::Applied(receipt) =
            controller.control_run(cancel.clone()).await.unwrap()
        else {
            panic!("fresh cancellation")
        };
        assert_eq!(
            controller.control_run(cancel).await.unwrap(),
            crabber::session::RunControlOutcome::Replayed(receipt)
        );
        closed_turn(store.as_ref(), &session, &run, &turn).await;
        let snapshot = controller
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot.interactions[0].state,
            if answered {
                InteractionState::Answered
            } else {
                InteractionState::Cancelled
            }
        );
        assert!(snapshot.interactions[0].consumed);
        assert_eq!(
            store.get_run(&run).await.unwrap().unwrap().status,
            RunStatus::Interrupted
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(
            worker
                .prompt(Some(session), "new ordinary work")
                .await
                .unwrap()
                .done()
                .await
                .unwrap()
                .status,
            RunStatus::Completed
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
    }
}

async fn open_turn(
    store: &dyn Store,
    session: &crabber::core::SessionId,
    run: &crabber::core::RunId,
) -> crabber::core::TurnId {
    let events = store.list_events(session, None, 256).await.unwrap();
    let turns = events
        .iter()
        .filter(|event| event.run_id == *run && event.kind == crabber::core::EventKind::TurnStarted)
        .collect::<Vec<_>>();
    assert_eq!(turns.len(), 1);
    let turn = turns[0].turn_id.clone().unwrap();
    assert!(!events.iter().any(|event| event.run_id == *run
        && event.turn_id.as_ref() == Some(&turn)
        && event.kind == crabber::core::EventKind::TurnCompleted));
    turn
}
async fn closed_turn(
    store: &dyn Store,
    session: &crabber::core::SessionId,
    run: &crabber::core::RunId,
    turn: &crabber::core::TurnId,
) {
    let events = store.list_events(session, None, 256).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.run_id == *run
                && event.kind == crabber::core::EventKind::TurnCompleted
                && event.turn_id.as_ref() == Some(turn))
            .count(),
        1
    );
    for kind in [
        crabber::core::EventKind::ToolCallSettled,
        crabber::core::EventKind::RunSettled,
    ] {
        assert!(events.iter().any(|event| event.run_id == *run
            && event.kind == kind
            && event.turn_id.as_ref() == Some(turn)));
    }
}
