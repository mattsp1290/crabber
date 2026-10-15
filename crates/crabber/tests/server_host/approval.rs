//! Public approval controls keep native authority inside the retained worker.
use super::*;
use crabber::session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval};
use crabber::{Continuation, ContinueApproval};

struct CountEffect(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for CountEffect {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(value)
    }
}
#[tokio::test]
async fn public_approval_continues_once_and_replays_after_extensions_close() {
    let store = Arc::new(MemoryStore::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![call("public-approval"), done()]);
    let agent = Agent::builder()
        .behavior_version("public-approval.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "identity".into(),
                description: "count once".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(CountEffect(effects.clone())),
        }))
        .build()
        .unwrap();
    let handle = agent.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let snapshot = agent
        .approval_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let before = store.get_run(&run).await.unwrap().unwrap();
    assert_eq!(
        agent
            .abandon(crabber::core::AbandonRequest {
                expected: crabber::core::RunFence {
                    run_id: run.clone(),
                    claim_token: before.claim_token.clone()
                },
                expected_owner: before.owner.clone(),
                authority: crabber::core::AbandonAuthority::HostStoppedOwner,
            })
            .await
            .unwrap_err(),
        crabber::core::AbandonError::NativeReconciliationRequired
    );
    assert_eq!(store.get_run(&run).await.unwrap().unwrap(), before);
    assert_eq!(
        store.session_revision(&session).await.unwrap(),
        snapshot.revision
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("pending approval");
    };
    let answer = ApprovalAnswer {
        id: crabber::core::ControlId::new(),
        expected_revision: snapshot.revision,
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation.clone(),
        interaction_id: id.clone(),
        resolver: ApprovalResolverClaim {
            resolver_id: "verified-test-host".into(),
            authority: description.authority().clone(),
            authority_claim: "host policy accepted the test resolver".into(),
        },
        reply: ApprovalReply::Grant,
    };
    let answered = agent.answer_approval(answer.clone()).await.unwrap();
    let request = ContinueApproval {
        id: crabber::core::ControlId::new(),
        session_id: session,
        run_id: run,
        continuation: snapshot.continuation,
        expected_revision: answered.revision,
    };
    let Continuation::Started { receipt, handle } =
        agent.continue_approval(request.clone()).await.unwrap()
    else {
        panic!("fresh continuation handle");
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    agent.close_extensions().await.unwrap();
    let Continuation::Replayed(replayed) = agent.continue_approval(request).await.unwrap() else {
        panic!("closed host must only return metadata");
    };
    assert_eq!(replayed, receipt);
    assert_eq!(agent.answer_approval(answer).await.unwrap(), answered);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn public_explicit_recovery_uses_fresh_authority_and_replays_after_close() {
    use crabber::core::{ControlId, ManualClock};
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let effects = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![call("public-recover"), done()]);
    let make = || {
        Agent::builder()
            .behavior_version("public-recovery.v1")
            .store(store.clone())
            .provider(Arc::new(provider.clone()))
            .config(AgentConfig::new(selection("scripted")))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
            .shared_tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: "identity".into(),
                    description: "count once".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                executor: Arc::new(CountEffect(effects.clone())),
            }))
            .build()
            .unwrap()
    };
    let agent = make();
    let handle = agent.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let snapshot = agent
        .approval_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("question")
    };
    let answer = agent
        .answer_approval(ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "verified-host".into(),
                authority: description.authority().clone(),
                authority_claim: "host accepted".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let original = ContinueApproval {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: answer.revision,
    };
    let original_receipt = lose_claim_ack(&store, &original).await;
    assert!(
        matches!(agent.continue_approval(original.clone()).await.unwrap(),Continuation::Replayed(saved) if saved==original_receipt)
    );
    let recovery = crabber::RecoverApproval {
        id: ControlId::new(),
        expected_revision: original_receipt.revision,
        original: original.clone(),
    };
    clock.set(now + time::Duration::milliseconds(2));
    drop(agent);
    let cold = make();
    let Continuation::Started { receipt, handle } = cold
        .recover_approval_continuation(recovery.clone())
        .await
        .unwrap()
    else {
        panic!("fresh recovery")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    cold.close_extensions().await.unwrap();
    assert!(
        matches!(cold.recover_approval_continuation(recovery).await.unwrap(),Continuation::Replayed(saved) if saved==receipt)
    );
    assert!(
        matches!(cold.continue_approval(original).await.unwrap(),Continuation::Replayed(saved) if saved==original_receipt)
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
}

async fn lose_claim_ack(
    store: &MemoryStore,
    original: &ContinueApproval,
) -> crabber::session::ApprovalReceipt {
    use crabber::session::{ApprovalClaimOutcome, ApprovalContinuationClaim};
    let persisted = store.get_run(&original.run_id).await.unwrap().unwrap();
    // Discard the private grant to model a committed claim with a lost response.
    let ApprovalClaimOutcome::Applied { receipt, .. } = store
        .claim_approval_continuation(ApprovalContinuationClaim {
            id: original.id.clone(),
            expected_revision: original.expected_revision,
            session_id: original.session_id.clone(),
            run_id: original.run_id.clone(),
            continuation: original.continuation.clone(),
            expected_plan: InputFingerprint::new(persisted.plan_fingerprint).unwrap(),
            owner: "lost-claim-host".into(),
            lease_ms: 1,
        })
        .await
        .unwrap()
    else {
        panic!("fresh claim")
    };
    receipt
}
