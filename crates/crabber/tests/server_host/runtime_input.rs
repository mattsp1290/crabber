//! The built-in input tool survives controller reconstruction with no live UI callback.
use super::*;
use crabber::core::{AuthorityRequirement, ContentBlock, ControlId};
use crabber::session::{ApprovalResolverClaim, ToolInteractionAnswer, ToolInteractionReply};
use crabber::{ContinueTool, ToolContinuation};
fn make(store: Arc<dyn Store>, provider: FakeProvider) -> Agent {
    make_policy(store, provider, "operator")
}
fn make_policy(store: Arc<dyn Store>, provider: FakeProvider, policy: &str) -> Agent {
    Agent::builder()
        .behavior_version("native-input-fixture.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(
            ToolDefinition::input(
                "request_input",
                AuthorityRequirement::new(policy, "1").unwrap(),
            )
            .unwrap(),
        ))
        .build()
        .unwrap()
}
fn requested() -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "request_input".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: json!({"questions":[
                {"name":"color","prompt":"Choose a color","choices":["blue","red"]},
                {"name":"notes","prompt":"Any notes?"}
            ]})
            .to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}
async fn journey(store: Arc<dyn Store>) {
    let provider = FakeProvider::scripted(vec![requested(), done()]);
    let agent = make(store.clone(), provider.clone());
    let handle = agent.prompt(None, "ask me").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let calls = store.list_unfinished_tool_calls(&run).await.unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].result.is_none());
    assert_eq!(provider.requests().len(), 1);
    let snapshot = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.interactions.len(), 2);
    let revision = answer_questions(&agent, &*store, &snapshot).await;
    agent.close_extensions().await.unwrap();
    let cold = make(store.clone(), provider.clone());
    let request = ContinueTool {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run,
        continuation: snapshot.continuation,
        expected_revision: revision,
    };
    let wrong = make_policy(store.clone(), provider.clone(), "different-policy");
    assert!(wrong.continue_tool(request.clone()).await.is_err());
    assert!(
        store
            .lookup_tool_continuation(&session, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    wrong.close_extensions().await.unwrap();
    let ToolContinuation::Started { receipt, handle } =
        cold.continue_tool(request.clone()).await.unwrap()
    else {
        panic!("fresh continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    cold.close_extensions().await.unwrap();
    assert!(
        matches!(wrong.continue_tool(request.clone()).await.unwrap(), ToolContinuation::Replayed(saved) if saved == receipt)
    );
    assert!(
        matches!(cold.continue_tool(request).await.unwrap(), ToolContinuation::Replayed(saved) if saved == receipt)
    );
    assert_eq!(provider.requests().len(), 2);
    check_results(&*store, &session).await;
}
async fn answer_questions(
    agent: &Agent,
    store: &dyn Store,
    snapshot: &crabber::session::ToolContinuationSnapshot,
) -> crabber::core::SessionRevision {
    let session = &snapshot.session_id;
    let run = &snapshot.run_id;
    let mut revision = snapshot.revision;
    for question in snapshot.interactions.iter().rev() {
        let mut answer = ToolInteractionAnswer {
            id: ControlId::new(),
            expected_revision: revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: question.id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "host".into(),
                authority: question.description.authority().clone(),
                authority_claim: "verified user".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!(if question.description.name() == "color" {
                    "blue"
                } else {
                    "hello"
                }),
            },
        };
        if question.description.name() == "color" {
            answer.reply = ToolInteractionReply::Answer {
                value: json!("green"),
            };
            assert!(agent.answer_tool_interaction(answer.clone()).await.is_err());
            assert_eq!(store.session_revision(session).await.unwrap(), revision);
            answer.reply = ToolInteractionReply::Answer {
                value: json!("blue"),
            };
        }
        revision = agent
            .answer_tool_interaction(answer)
            .await
            .unwrap()
            .revision;
    }
    revision
}
async fn check_results(store: &dyn Store, session: &SessionId) {
    let messages = store.list_all_messages(session).await.unwrap();
    let results: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| {
            if let ContentBlock::ToolResult { content, .. } = &part.content {
                Some(content)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(results.len(), 1);
    let [ContentBlock::Text { text }] = results[0].as_slice() else {
        panic!("structured answer result")
    };
    let values: Value = serde_json::from_str(text).unwrap();
    assert_eq!(values[0]["name"], "color");
    assert_eq!(values[0]["value"], "blue");
    assert_eq!(values[1]["name"], "notes");
    assert_eq!(values[1]["value"], "hello");
}

#[tokio::test]
async fn memory_native_input_tool() {
    Box::pin(journey(Arc::new(MemoryStore::new()))).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_native_input_tool() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    Box::pin(journey(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    )))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_native_input_tool() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(
            std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none(),
            "required PostgreSQL URL missing"
        );
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    Box::pin(journey(Arc::new(
        crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap(),
    )))
    .await;
}
