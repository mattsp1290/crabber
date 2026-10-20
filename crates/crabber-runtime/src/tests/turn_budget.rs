//! Model turn limits survive durable approval and ordinary pause boundaries.
use super::*;
use crabber_session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval};

fn runtime(
    store: Arc<dyn Store>,
    provider: FakeProvider,
    effects: Arc<AtomicUsize>,
    max_turns: usize,
    policy: Arc<dyn PermissionPolicy>,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_version("persisted-turn-budget.v1")
        .store(store)
        .resolver(Arc::new(provider))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(effects)))],
            vec![],
        )))
        .policy(policy)
        .max_turns(max_turns)
        .build()
        .unwrap()
}

async fn grant(
    runtime: &Orchestrator,
    session: &SessionId,
    run: &RunId,
) -> crate::ContinueApproval {
    let snapshot = runtime
        .approval_snapshot(session, run)
        .await
        .unwrap()
        .unwrap();
    let (question, authority) = snapshot
        .calls
        .iter()
        .find_map(|call| match &call.decision {
            CallApproval::Ask { id, description } => {
                Some((id.clone(), description.authority().clone()))
            }
            _ => None,
        })
        .unwrap();
    let receipt = runtime
        .answer_approval(ApprovalAnswer {
            id: crabber_core::ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: question,
            resolver: ApprovalResolverClaim {
                resolver_id: "test-host".into(),
                authority,
                authority_claim: "verified test decision".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    crate::ContinueApproval {
        id: crabber_core::ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: receipt.revision,
    }
}

#[tokio::test]
async fn repeated_approvals_share_one_total_model_turn_budget() {
    for max_turns in [1, 3] {
        let store = Arc::new(MemoryStore::new());
        let effects = Arc::new(AtomicUsize::new(0));
        let provider = FakeProvider::scripted(
            (0..max_turns)
                .map(|_| call_script(ToolCallId::new(), r#"{"text":"approved"}"#))
                .collect(),
        );
        let runtime = runtime(
            store.clone(),
            provider.clone(),
            effects.clone(),
            max_turns,
            Arc::new(StaticPolicy::new(PermissionDecision::Ask)),
        );
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        for turn in 1..=max_turns {
            let persisted = store.get_run(&run).await.unwrap().unwrap();
            assert_eq!(
                persisted.checkpoint.as_ref().unwrap()["turns_used"],
                json!(turn)
            );
            let request = grant(&runtime, &session, &run).await;
            let crate::Continuation::Started { handle, .. } =
                runtime.continue_approval(request).await.unwrap()
            else {
                panic!("fresh continuation");
            };
            if turn == max_turns {
                assert!(matches!(handle.done().await, Err(RuntimeError::TurnLimit)));
            } else {
                assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
            }
            assert_eq!(effects.load(Ordering::SeqCst), turn);
        }
        assert_eq!(provider.requests().len(), max_turns);
        assert_eq!(
            store.get_run(&run).await.unwrap().unwrap().status,
            RunStatus::Failed
        );
    }
}

#[tokio::test]
async fn ordinary_pause_preserves_the_consumed_model_turn() {
    let store = Arc::new(MemoryStore::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let provider =
        FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"paused"}"#)]);
    let runtime = runtime(
        store.clone(),
        provider.clone(),
        effects.clone(),
        1,
        Arc::new(PausePolicy),
    );
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert!(matches!(
        runtime.resume(&run).await,
        Err(RuntimeError::TurnLimit)
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn ambiguous_or_invalid_pause_budget_is_refused_before_claim() {
    for count in [None, Some(json!(0)), Some(json!(u64::MAX))] {
        let store = Arc::new(MemoryStore::new());
        let gate = Arc::new(SettlementGate::default());
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let provider =
            FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"paused"}"#)]);
        let runtime = runtime(
            wrapped,
            provider.clone(),
            effects.clone(),
            1,
            Arc::new(PausePolicy),
        );
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        let session = handle.session_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let run = store.get_run(&run_id).await.unwrap().unwrap();
        let mut checkpoint = run.checkpoint.clone().unwrap();
        if let Some(count) = count {
            checkpoint["turns_used"] = count;
        } else {
            checkpoint.as_object_mut().unwrap().remove("turns_used");
        }
        // Inject a retained legacy/corrupt read without bypassing native
        // mutation guards. Rejection must leave the authoritative Store intact.
        let before = run;
        let revision = store.session_revision(&session).await.unwrap();
        let events = store.list_events(&session, None, 1000).await.unwrap();
        *gate.checkpoint_override.lock().unwrap() = Some(checkpoint);
        assert!(matches!(
            runtime.resume(&run_id).await,
            Err(RuntimeError::Store(StoreError::HostUnsupported))
        ));
        assert_eq!(store.get_run(&run_id).await.unwrap().unwrap(), before);
        assert_eq!(store.session_revision(&session).await.unwrap(), revision);
        assert_eq!(
            store.list_events(&session, None, 1000).await.unwrap(),
            events
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert_eq!(provider.requests().len(), 1);
    }
}
