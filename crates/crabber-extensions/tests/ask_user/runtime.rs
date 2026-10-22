use super::*;
use crabber::core::{Clock, ManualClock, SystemClock};
use crabber::session::{ToolContinuationClaim, ToolContinuationClaimOutcome};

#[tokio::test]
async fn two_parallel_questions_require_the_complete_response_set() {
    let store = Arc::new(MemoryStore::new());
    let (tool, count) =
        counted_with(extension(), Some(Arc::new(tokio::sync::Barrier::new(2)))).await;
    let agent = builder(
        store.clone(),
        FakeProvider::scripted(vec![calls(2), done()]),
        tool,
    )
    .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
    .build()
    .unwrap();
    let (_, s) = pause(&agent).await;
    assert_eq!(s.interactions.len(), 2);
    assert_ne!(s.interactions[0].id, s.interactions[1].id);
    let first = agent
        .answer_tool_interaction(answer(
            &s,
            0,
            ToolInteractionReply::Answer {
                value: json!({"selected":2}),
            },
        ))
        .await
        .unwrap();
    tool_error(
        agent
            .continue_tool(continuation(&s, first.revision))
            .await
            .err()
            .unwrap(),
        ToolContinuationError::Pending,
    );
    assert_eq!(count.resumes.load(Ordering::SeqCst), 0);
    let mut second = answer(&s, 1, ToolInteractionReply::Cancel);
    second.expected_revision = first.revision;
    let receipt = agent.answer_tool_interaction(second).await.unwrap();
    complete(&agent, continuation(&s, receipt.revision)).await;
    let rows = records(&store).await;
    assert_eq!(rows.len(), 2);
    let values = rows.iter().map(value).collect::<Vec<_>>();
    assert!(values.contains(&json!({"status":"dismissed"})));
    assert!(values.contains(&json!({"status":"selected","answer":"second","selected_option":2})));
    assert_eq!(count.starts.load(Ordering::SeqCst), 2);
    assert_eq!(count.resumes.load(Ordering::SeqCst), 2);
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn lost_unstarted_claim_recovers_after_lease_once_on_cold_agent() {
    let clock = Arc::new(ManualClock::new(SystemClock.now()));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let provider = FakeProvider::scripted(vec![call(), done()]);
    let (tool, count) = counted().await;
    let agent = builder(store.clone(), provider.clone(), tool.clone())
        .build()
        .unwrap();
    let (run, s) = pause(&agent).await;
    let accepted = agent
        .answer_tool_interaction(answer(
            &s,
            0,
            ToolInteractionReply::Answer {
                value: json!({"selected":2}),
            },
        ))
        .await
        .unwrap();
    let original = continuation(&s, accepted.revision);
    let stored = store.get_run(&run).await.unwrap().unwrap();
    let ToolContinuationClaimOutcome::Applied {
        receipt: claimed, ..
    } = store
        .claim_tool_continuation(ToolContinuationClaim {
            id: original.id.clone(),
            session_id: s.session_id.clone(),
            run_id: run,
            continuation: original.continuation.clone(),
            expected_revision: original.expected_revision,
            expected_plan: InputFingerprint::new(stored.plan_fingerprint).unwrap(),
            owner: "lost-claim".into(),
            lease_ms: 1,
        })
        .await
        .unwrap()
    else {
        panic!("fresh lost claim")
    };
    assert!(
        matches!(agent.continue_tool(original.clone()).await.unwrap(), ToolContinuation::Replayed(r) if r == claimed)
    );
    assert_eq!(count.resumes.load(Ordering::SeqCst), 0);
    agent.close_extensions().await.unwrap();
    drop(agent);
    clock.set(clock.now() + Duration::from_millis(2));
    let cold = builder(store.clone(), provider, tool).build().unwrap();
    let recovery = crabber::RecoverTool {
        id: ControlId::new(),
        expected_revision: claimed.revision,
        original,
    };
    let ToolContinuation::Started { receipt, handle } = cold
        .recover_tool_continuation(recovery.clone())
        .await
        .unwrap()
    else {
        panic!("fresh recovery")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert!(
        matches!(cold.recover_tool_continuation(recovery).await.unwrap(), ToolContinuation::Replayed(r) if r == receipt)
    );
    assert_eq!(count.starts.load(Ordering::SeqCst), 1);
    assert_eq!(count.resumes.load(Ordering::SeqCst), 1);
    assert_eq!(records(&store).await.len(), 1);
    cold.close_extensions().await.unwrap();
}
