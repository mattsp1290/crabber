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
