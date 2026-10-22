use super::*;
#[tokio::test]
async fn duplicate_conflicting_stale_cross_session_and_unauthorized_answers() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = agent(
        root.path(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            turn("user_interact", json!({"question":"Q?"})),
            done(),
        ])),
        UserInteractPolicy::durable(authority()),
    );
    let snapshot = paused(&agent, &store).await;
    let valid = answer(
        &snapshot,
        0,
        ToolInteractionReply::Answer {
            value: json!("yes"),
        },
    );
    let mut rejected = vec![];
    let mut stale = valid.clone();
    stale.expected_revision =
        crabber::core::SessionRevision::new(snapshot.revision.get() - 1).unwrap();
    rejected.push(stale);
    let mut cross = valid.clone();
    cross.session_id = crabber::core::SessionId::new();
    rejected.push(cross);
    let mut unauthorized = valid.clone();
    unauthorized.resolver.authority = AuthorityRequirement::new("wrong-policy", "1").unwrap();
    rejected.push(unauthorized);
    for value in [
        json!(true),
        json!("x".repeat(9 * 1024)),
        json!("x".repeat(70 * 1024)),
    ] {
        let mut invalid = valid.clone();
        invalid.reply = ToolInteractionReply::Answer { value };
        rejected.push(invalid);
    }
    for invalid in rejected {
        assert!(agent.answer_tool_interaction(invalid).await.is_err());
        assert_eq!(
            store.session_revision(&snapshot.session_id).await.unwrap(),
            snapshot.revision
        );
        let still = agent
            .tool_continuation_snapshot(&snapshot.session_id, &snapshot.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            still.interactions[0].state,
            crabber::core::InteractionState::Pending
        );
    }
    let receipt = agent.answer_tool_interaction(valid.clone()).await.unwrap();
    assert_eq!(
        agent.answer_tool_interaction(valid.clone()).await.unwrap(),
        receipt
    );
    let mut conflicting = valid;
    conflicting.reply = ToolInteractionReply::Answer {
        value: json!("different"),
    };
    assert!(agent.answer_tool_interaction(conflicting).await.is_err());
    resume(&agent, &snapshot, receipt.revision).await;
    agent.close_extensions().await.unwrap();
}
