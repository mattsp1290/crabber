//! Durable questions through the public Agent/store protocol, without credentials.
use crabber::runtime::ExecutionMode;
use crabber::{
    Agent, ContinueTool, FakeProvider, PermissionDecision, StaticPolicy, ToolContinuation,
    ToolDefinition,
    core::{AuthorityRequirement, ControlId, RunStatus, ToolCallStatus},
    session::{
        ApprovalResolverClaim, MemoryStore, Store, ToolContinuationSnapshot, ToolInteractionAnswer,
        ToolInteractionReply,
    },
};
use crabber_tools_testkit as support;
use crabber_tools_userinteract::{Options, Surface, UserInteractPolicy, definition};
use serde_json::json;
use std::{path::Path, sync::Arc, time::Duration};
use support::*;
fn authority() -> AuthorityRequirement {
    AuthorityRequirement::new("test-operator", "1").unwrap()
}
fn tool(policy: UserInteractPolicy) -> Arc<ToolDefinition> {
    definition(Arc::new(Options { policy })).unwrap()
}
fn agent(
    root: &Path,
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    policy: UserInteractPolicy,
) -> Agent {
    Agent::builder()
        .behavior_version("durable-tests/0.4.0")
        .store(store)
        .provider(provider)
        .config(config(root))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .shared_tool(tool(policy))
        .build()
        .unwrap()
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
            resolver_id: "scripted-host".into(),
            authority: authority(),
            authority_claim: "synthetic authenticated fixture".into(),
        },
        reply,
    }
}
async fn resume(
    agent: &Agent,
    snapshot: &ToolContinuationSnapshot,
    revision: crabber::core::SessionRevision,
) -> ContinueTool {
    let request = ContinueTool {
        id: ControlId::new(),
        session_id: snapshot.session_id.clone(),
        run_id: snapshot.run_id.clone(),
        continuation: snapshot.continuation.clone(),
        expected_revision: revision,
    };
    let ToolContinuation::Started { handle, .. } =
        agent.continue_tool(request.clone()).await.unwrap()
    else {
        panic!("fresh continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert!(matches!(
        agent.continue_tool(request.clone()).await.unwrap(),
        ToolContinuation::Replayed(_)
    ));
    request
}
async fn paused(agent: &Agent, store: &MemoryStore) -> ToolContinuationSnapshot {
    let handle = agent.prompt(None, "ask").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let records = calls(store, &session).await;
    let snapshot = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert!(!snapshot.interactions.is_empty());
    for question in &snapshot.interactions {
        assert_eq!(
            records
                .iter()
                .find(|r| r.id == question.call_id)
                .unwrap()
                .status,
            ToolCallStatus::Waiting
        );
    }
    snapshot
}
#[tokio::test]
async fn durable_question_pauses_and_resumes_once() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        turn("user_interact", json!({"question":"Q?"})),
        done(),
    ]));
    let agent = agent(
        root.path(),
        store.clone(),
        provider.clone(),
        UserInteractPolicy::durable(authority()),
    );
    let snapshot = paused(&agent, &store).await;
    assert_eq!(snapshot.interactions.len(), 1);
    assert_eq!(snapshot.interactions[0].description.prompt(), "Q?");
    let receipt = agent
        .answer_tool_interaction(answer(
            &snapshot,
            0,
            ToolInteractionReply::Answer {
                value: json!("yes\n\n"),
            },
        ))
        .await
        .unwrap();
    resume(&agent, &snapshot, receipt.revision).await;
    let records = calls(&store, &snapshot.session_id).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, ToolCallStatus::Completed);
    assert_eq!(
        value(&records[0]),
        json!({"outcome":"succeeded","answer":"yes"})
    );
    assert_eq!(provider.requests().len(), 2);
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn denied_cancelled_expired_are_error_results() {
    for (reply, category) in [
        (ToolInteractionReply::Deny, "denied"),
        (ToolInteractionReply::Cancel, "cancelled"),
        (ToolInteractionReply::Expire, "timeout"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::new());
        let mut policy = UserInteractPolicy::durable(authority());
        if category == "timeout" {
            policy.surface = Surface::Durable {
                authority: authority(),
                expires_after: Some(Duration::from_millis(1)),
            };
        }
        let agent = agent(
            root.path(),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                turn("user_interact", json!({"question":"Q?"})),
                done(),
            ])),
            policy,
        );
        let snapshot = paused(&agent, &store).await;
        let receipt = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match agent
                    .answer_tool_interaction(answer(&snapshot, 0, reply.clone()))
                    .await
                {
                    Ok(receipt) => break receipt,
                    Err(error) if category == "timeout" => {
                        let _ = error;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(error) => panic!("{error}"),
                }
            }
        })
        .await
        .unwrap();
        resume(&agent, &snapshot, receipt.revision).await;
        let records = calls(&store, &snapshot.session_id).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(value(&records[0])["error"]["category"], category);
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn model_supplied_answer_is_rejected_without_pause() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = agent(
        root.path(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            turn("user_interact", json!({"question":"Q?","answer":"yes"})),
            done(),
        ])),
        UserInteractPolicy::durable(authority()),
    );
    let run = agent.prompt(None, "ask").await.unwrap();
    let id = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let records = calls(&store, &id).await;
    assert_eq!(records[0].status, ToolCallStatus::Failed);
    assert_eq!(value(&records[0])["error"]["category"], "validation");
    agent.close_extensions().await.unwrap();
}

#[path = "durable/controls.rs"]
mod controls;

#[path = "durable/restart.rs"]
mod restart;

#[path = "durable/compatibility.rs"]
mod compatibility;

#[path = "durable/parallel.rs"]
mod parallel;
