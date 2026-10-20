use super::*;
use crabber::core::{Clock, ManualClock};
use crabber::session::{ToolContinuationClaim, ToolContinuationClaimOutcome};
#[tokio::test]
async fn public_tool_claim_ack_loss_requires_explicit_unstarted_recovery() {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let provider = FakeProvider::scripted(vec![call("interactive-recovery"), done()]);
    let tool = Arc::new(Interactive {
        starts: Arc::default(),
        resumes: Arc::default(),
        effects: Arc::default(),
    });
    let agent = make(store.clone(), provider.clone(), tool.clone());
    let handle = agent.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let snapshot = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let question = &snapshot.interactions[0];
    let accepted = agent
        .answer_tool_interaction(ToolInteractionAnswer {
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
        })
        .await
        .unwrap();
    let original = ContinueTool {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: accepted.revision,
    };
    let stored = store.get_run(&run).await.unwrap().unwrap();
    let ToolContinuationClaimOutcome::Applied {
        receipt: claimed, ..
    } = store
        .claim_tool_continuation(ToolContinuationClaim {
            id: original.id.clone(),
            session_id: session,
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
        panic!("lost fresh claim")
    };
    agent.close_extensions().await.unwrap();
    assert!(
        matches!(agent.continue_tool(original.clone()).await.unwrap(),ToolContinuation::Replayed(receipt) if receipt==claimed)
    );
    assert_eq!(tool.resumes.load(Ordering::SeqCst), 0);
    drop(agent);
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let cold = make(store, provider.clone(), tool.clone());
    let request = crabber::RecoverTool {
        id: ControlId::new(),
        expected_revision: claimed.revision,
        original: original.clone(),
    };
    let ToolContinuation::Started { receipt, handle } = cold
        .recover_tool_continuation(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh recovery")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(tool.starts.load(Ordering::SeqCst), 1);
    assert_eq!(tool.resumes.load(Ordering::SeqCst), 1);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    cold.close_extensions().await.unwrap();
    assert!(
        matches!(cold.recover_tool_continuation(request).await.unwrap(),ToolContinuation::Replayed(actual) if actual==receipt)
    );
    assert!(
        matches!(cold.continue_tool(original).await.unwrap(),ToolContinuation::Replayed(actual) if actual==claimed)
    );
    assert_eq!(provider.requests().len(), 1);
}
