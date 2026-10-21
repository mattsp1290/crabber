//! Actual frontend result workers can hand their next model turn to approval.
use super::*;
use crabber::core::ControlId;
use crabber::session::{
    ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval, FrontendContinuationClaim,
    ToolContinuationClaim, ToolContinuationClaimOutcome,
};

struct Effect(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Effect {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!("approved backend result"))
    }
}
fn worker(store: Arc<dyn Store>, provider: FakeProvider, effects: Arc<AtomicUsize>) -> Agent {
    Agent::builder()
        .behavior_version("frontend-approval-handoff.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "approved_backend".into(),
                description: "Execute once after explicit approval".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Effect(effects)),
        }))
        .build()
        .unwrap()
}
#[allow(clippy::too_many_lines)] // One complete frontend-to-approval worker journey.
async fn journey(store: Arc<dyn Store>, recover: bool) {
    let prefix = ToolCallId::new().0;
    let external = call(&format!("{prefix}-external"));
    let backend = call(&format!("{prefix}-backend"))
        .into_iter()
        .map(|delta| match delta {
            StreamDelta::ToolCallStart { call_id, .. } => StreamDelta::ToolCallStart {
                call_id,
                name: "approved_backend".into(),
            },
            other => other,
        })
        .collect();
    let provider = FakeProvider::scripted(vec![external, backend, done()]);
    let effects = Arc::new(AtomicUsize::new(0));
    let mut agent = worker(store.clone(), provider.clone(), effects.clone());
    let session = SessionId::new();
    let run = start(&agent, &session).await;
    let waiting = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let original_page = aggregate_v2_page(&agent, &session).await;
    assert_eq!(
        original_page.header.tool.as_ref().unwrap().state,
        crabber::session::ToolContinuationState::Waiting
    );
    let request = response(&waiting);
    let recovery = if recover {
        let stored = store.get_run(&run).await.unwrap().unwrap();
        let ToolContinuationClaimOutcome::Applied { receipt, .. } = store
            .claim_frontend_continuation(FrontendContinuationClaim {
                claim: ToolContinuationClaim {
                    id: request.continuation.id.clone(),
                    session_id: session.clone(),
                    run_id: run.clone(),
                    continuation: request.continuation.continuation.clone(),
                    expected_revision: request.continuation.expected_revision,
                    expected_plan: InputFingerprint::new(stored.plan_fingerprint).unwrap(),
                    owner: "lost-frontend-claim".into(),
                    lease_ms: 1,
                },
                resolver: request.resolver.clone(),
                results: request.results.clone(),
            })
            .await
            .unwrap()
        else {
            panic!("fresh unstarted frontend claim")
        };
        agent.close_extensions().await.unwrap();
        assert!(
            matches!(agent.continue_frontend(request.clone()).await.unwrap(),
            ToolContinuation::Replayed(saved) if saved == receipt)
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        agent = worker(store.clone(), provider.clone(), effects.clone());
        Some(crabber::RecoverFrontend {
            id: ControlId::new(),
            expected_revision: receipt.revision,
            original: request.clone(),
        })
    } else {
        None
    };
    let outcome = if let Some(recovery) = &recovery {
        agent
            .recover_frontend_continuation(recovery.clone())
            .await
            .unwrap()
    } else {
        agent.continue_frontend(request.clone()).await.unwrap()
    };
    let ToolContinuation::Started {
        receipt: frontend_receipt,
        handle,
    } = outcome
    else {
        panic!("fresh frontend worker")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 2);
    let paused_page = aggregate_v2_page(&agent, &session).await;
    assert_eq!(
        paused_page.header.tool.as_ref().unwrap().state,
        crabber::session::ToolContinuationState::Released
    );
    assert_eq!(
        paused_page.header.approval.as_ref().unwrap().state,
        crabber::session::ApprovalContinuationState::Waiting
    );
    assert!(matches!(
        paused_page.header.barrier,
        Some(crabber::PublicContinuationBarrierV2::Approval(_))
    ));

    let events = store.list_events(&session, None, 1000).await.unwrap();
    let handoff = events
        .iter()
        .rev()
        .find_map(|event| event.payload.get("handoff"))
        .expect("immutable frontend-to-approval handoff");
    assert_eq!(
        handoff["predecessor"]["operation_id"],
        json!(frontend_receipt.operation_id)
    );
    assert_eq!(
        handoff["predecessor"]["digest"],
        json!(frontend_receipt.semantic_digest)
    );
    let approval = agent
        .approval_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .state,
        crabber::session::ToolContinuationState::Released
    );
    let CallApproval::Ask { id, description } = &approval.calls[0].decision else {
        panic!("next turn waits for native approval")
    };
    let answered = agent
        .answer_approval(ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: approval.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: approval.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "handoff-controller".into(),
                authority: description.authority().clone(),
                authority_claim: "authenticated host".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let crabber::Continuation::Started { handle, .. } = agent
        .continue_approval(crabber::ContinueApproval {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: approval.continuation,
            expected_revision: answered.revision,
        })
        .await
        .unwrap()
    else {
        panic!("fresh approved worker")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 3);
    let terminal_page = aggregate_v2_page(&agent, &session).await;
    assert!(terminal_page.is_terminal());
    assert!(terminal_page.header.barrier.is_none());

    agent.close_extensions().await.unwrap();
    let before = store.list_events(&session, None, 1000).await.unwrap();
    let replay = if let Some(recovery) = recovery {
        agent.recover_frontend_continuation(recovery).await.unwrap()
    } else {
        agent.continue_frontend(request).await.unwrap()
    };
    assert!(matches!(replay, ToolContinuation::Replayed(saved) if saved == frontend_receipt));
    assert_eq!(
        store.list_events(&session, None, 1000).await.unwrap(),
        before
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 3);
}
#[tokio::test]
async fn memory_frontend_result_and_recovery_handoff_to_approval() {
    for recover in [false, true] {
        journey(Arc::new(MemoryStore::new()), recover).await;
    }
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_frontend_result_and_recovery_handoff_to_approval() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("frontend-handoff.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    for recover in [false, true] {
        journey(
            Arc::new(crabber::session::SqliteStore::connect(&path).await.unwrap()),
            recover,
        )
        .await;
    }
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_frontend_result_and_recovery_handoff_to_approval() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        for recover in [false, true] {
            journey(
                Arc::new(
                    crabber::session::PostgresStore::connect(&url)
                        .await
                        .unwrap(),
                ),
                recover,
            )
            .await;
        }
    })
    .await
    .unwrap();
}
