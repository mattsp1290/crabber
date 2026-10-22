use super::*;
use crabber::core::InteractionKind;

#[tokio::test]
async fn cold_agent_answers_once_and_identical_continue_replays() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call(), done()]);
    let (tool, count) = counted().await;
    let agent = builder(store.clone(), provider.clone(), tool.clone())
        .build()
        .unwrap();
    let (_, snapshot) = pause(&agent).await;
    assert_eq!(snapshot.interactions.len(), 1);
    let d = &snapshot.interactions[0].description;
    assert_eq!(d.kind(), InteractionKind::UserInput);
    assert_eq!(d.prompt(), " Pick one ");
    assert_eq!(d.authority(), &options().authority);
    assert_eq!(d.expires_after_ms(), Some(600_000));
    assert!(snapshot.interactions[0].expires_at.is_some());
    assert_eq!(count.starts.load(Ordering::SeqCst), 1);
    assert!(records(&store).await[0].result.is_none());
    let answer = answer(
        &snapshot,
        0,
        ToolInteractionReply::Answer {
            value: json!({"selected":2}),
        },
    );
    let accepted = agent.answer_tool_interaction(answer.clone()).await.unwrap();
    assert_eq!(
        agent.answer_tool_interaction(answer).await.unwrap(),
        accepted
    );
    agent.close_extensions().await.unwrap();
    drop(agent);
    let cold = builder(store.clone(), provider.clone(), tool)
        .build()
        .unwrap();
    let request = continuation(&snapshot, accepted.revision);
    let ToolContinuation::Started { receipt, handle } =
        cold.continue_tool(request.clone()).await.unwrap()
    else {
        panic!("fresh")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert!(
        matches!(cold.continue_tool(request).await.unwrap(), ToolContinuation::Replayed(r) if r == receipt)
    );
    let rows = records(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        value(&rows[0]),
        json!({"status":"selected","answer":"second","selected_option":2})
    );
    assert_eq!(count.starts.load(Ordering::SeqCst), 1);
    assert_eq!(count.resumes.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    cold.close_extensions().await.unwrap();
}
#[tokio::test]
async fn custom_and_dismissal_are_ordinary_results() {
    for (reply, expected) in [
        (
            ToolInteractionReply::Answer {
                value: json!({"selected":2.0}),
            },
            json!({"status":"selected","answer":"second","selected_option":2}),
        ),
        (
            ToolInteractionReply::Answer {
                value: json!({"custom":"  my answer  "}),
            },
            json!({"status":"custom","answer":"  my answer  "}),
        ),
        (ToolInteractionReply::Deny, json!({"status":"dismissed"})),
        (ToolInteractionReply::Cancel, json!({"status":"dismissed"})),
    ] {
        let store = Arc::new(MemoryStore::new());
        let (tool, count) = counted().await;
        let agent = builder(
            store.clone(),
            FakeProvider::scripted(vec![call(), done()]),
            tool,
        )
        .build()
        .unwrap();
        let (_, s) = pause(&agent).await;
        let accepted = agent
            .answer_tool_interaction(answer(&s, 0, reply))
            .await
            .unwrap();
        complete(&agent, continuation(&s, accepted.revision)).await;
        let rows = records(&store).await;
        assert_eq!(value(&rows[0]), expected);
        assert_eq!(
            rows[0].result.as_ref().unwrap().status,
            crabber::core::ToolResultStatus::Completed
        );
        assert_eq!(count.resumes.load(Ordering::SeqCst), 1);
        agent.close_extensions().await.unwrap();
    }
}

#[tokio::test]
async fn maximal_control_character_custom_answer_fits_store_transport_and_completes() {
    let mut o = options();
    o.limits.max_custom_answer_bytes = 10240;
    let (tool, count) = counted_with(AskUser::new(o).unwrap(), None).await;
    let store = Arc::new(MemoryStore::new());
    let agent = builder(
        store.clone(),
        FakeProvider::scripted(vec![call(), done()]),
        tool,
    )
    .build()
    .unwrap();
    let (_, s) = pause(&agent).await;
    let custom = "\u{1}".repeat(10240);
    let value = json!({"custom":custom});
    assert!(serde_json::to_vec(&value).unwrap().len() <= 65536);
    assert!(
        jsonschema::validator_for(s.interactions[0].description.response_schema())
            .unwrap()
            .is_valid(&value)
    );
    let accepted = agent
        .answer_tool_interaction(answer(&s, 0, ToolInteractionReply::Answer { value }))
        .await
        .unwrap();
    complete(&agent, continuation(&s, accepted.revision)).await;
    assert_eq!(
        super::value(&records(&store).await[0]),
        json!({"status":"custom","answer":custom})
    );
    assert_eq!(count.resumes.load(Ordering::SeqCst), 1);
    agent.close_extensions().await.unwrap();
}
