use super::*;

#[tokio::test]
async fn permission_deny_or_ask_prevents_durable_tool_start() {
    for decision in [PermissionDecision::Deny, PermissionDecision::Ask] {
        let store = Arc::new(MemoryStore::new());
        let (tool, count) = counted().await;
        let agent = builder(
            store.clone(),
            FakeProvider::scripted(vec![call(), done()]),
            tool,
        )
        .policy(Arc::new(StaticPolicy::new(decision)))
        .build()
        .unwrap();
        let handle = agent_support::first_prompt(&agent, &execution(), "ask")
            .await
            .unwrap();
        let run = handle.run_id().clone();
        let outcome = handle.done().await.unwrap();
        assert_eq!(count.starts.load(Ordering::SeqCst), 0);
        assert_eq!(count.resumes.load(Ordering::SeqCst), 0);
        assert!(
            agent
                .tool_continuation_snapshot(&"session".into(), &run)
                .await
                .unwrap()
                .is_none()
        );
        let rows = records(&store).await;
        if decision == PermissionDecision::Ask {
            assert_eq!(outcome.status, RunStatus::Paused);
            assert!(
                rows[0].result.is_none(),
                "approval pause has no fabricated tool result"
            );
        } else {
            assert_eq!(outcome.status, RunStatus::Completed);
            assert_eq!(
                rows[0].result.as_ref().unwrap().status,
                crabber::core::ToolResultStatus::Failed
            );
        }
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn continued_answer_passes_final_redaction_before_persistence_and_provider_reentry() {
    use crabber_extensions::tool_result_redactor as redactor;
    let redactor = redactor::ToolResultRedactor::new(redactor::Options {
        order: redactor::DEFAULT_ORDER,
        excluded_tools: vec![],
        additional_patterns: vec![redactor::Pattern {
            id: "synthetic-answer".into(),
            expression: "synthetic-private-answer".into(),
        }],
        limits: redactor::Limits {
            max_field_bytes: 1000,
            max_total_bytes: 10000,
            max_depth: 10,
            max_nodes: 1000,
            max_matches_per_field: 10,
            max_patterns: 10,
            max_pattern_bytes: 100,
            max_in_flight: 1,
        },
    })
    .unwrap();
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call(), done()]);
    let (tool, _) = counted().await;
    let agent = builder(store.clone(), provider.clone(), tool)
        .shared_extension(Arc::new(redactor), Scope::Global)
        .build()
        .unwrap();
    let (_, s) = pause(&agent).await;
    let accepted = agent
        .answer_tool_interaction(answer(
            &s,
            0,
            ToolInteractionReply::Answer {
                value: json!({"custom":"synthetic-private-answer"}),
            },
        ))
        .await
        .unwrap();
    complete(&agent, continuation(&s, accepted.revision)).await;
    assert_eq!(
        value(&records(&store).await[0]),
        json!({"status":"custom","answer":redactor::PLACEHOLDER})
    );
    let requests = provider.requests();
    let replay = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(!replay.contains("synthetic-private-answer"));
    assert!(replay.contains(redactor::PLACEHOLDER));
    agent.close_extensions().await.unwrap();
}
