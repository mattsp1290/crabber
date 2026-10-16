//! Cold explicit recovery never derives execution authority from a receipt.
use super::*;
use crabber_session::{
    ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, AttemptState, CallApproval,
};
struct Decisions(Arc<AtomicUsize>);
impl PermissionPolicy for Decisions {
    fn decide(&self, _: &crabber_core::ToolInfo, args: &Value) -> PermissionDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        match args["text"].as_str().unwrap() {
            "grant" | "refuse" => PermissionDecision::Ask,
            "deny" => PermissionDecision::Deny,
            _ => PermissionDecision::Allow,
        }
    }
}
struct Effects(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl JsonToolExecutor for Effects {
    async fn execute(&self, args: Value) -> Result<Value, ExtensionError> {
        self.0
            .lock()
            .unwrap()
            .push(args["text"].as_str().unwrap().into());
        Ok(args)
    }
}
fn batch() -> Vec<StreamDelta> {
    let mut result = Vec::new();
    for (id, text) in [
        ("z:/λ", "allow"),
        ("y:/λ", "grant"),
        ("b:/λ", "refuse"),
        ("a:/λ", "deny"),
    ] {
        let mut call = call_script(ToolCallId(id.into()), &json!({"text":text}).to_string());
        call.pop();
        result.extend(call);
    }
    result.push(StreamDelta::Completed);
    result
}
async fn answers(
    runtime: &Orchestrator,
    session: &SessionId,
    run: &RunId,
) -> crate::ContinueApproval {
    let snapshot = runtime
        .approval_snapshot(session, run)
        .await
        .unwrap()
        .unwrap();
    let mut revision = snapshot.revision;
    for (index, call) in snapshot.calls.iter().enumerate() {
        if let CallApproval::Ask { id, description } = &call.decision {
            revision = runtime
                .answer_approval(ApprovalAnswer {
                    id: crabber_core::ControlId::new(),
                    expected_revision: revision,
                    session_id: session.clone(),
                    run_id: run.clone(),
                    continuation: snapshot.continuation.clone(),
                    interaction_id: id.clone(),
                    resolver: ApprovalResolverClaim {
                        resolver_id: "test-host".into(),
                        authority: description.authority().clone(),
                        authority_claim: "verified choice".into(),
                    },
                    reply: if index == 1 {
                        ApprovalReply::Grant
                    } else {
                        ApprovalReply::Deny
                    },
                })
                .await
                .unwrap()
                .revision;
        }
    }
    crate::ContinueApproval {
        id: crabber_core::ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: revision,
    }
}
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cold_recovery_preserves_frozen_decisions_order_and_total_turn_budget() {
    for mode in [
        ExecutionMode::Sequential,
        ExecutionMode::Parallel { max: 4 },
    ] {
        for fault in 0..3 {
            let now = time::OffsetDateTime::UNIX_EPOCH;
            let clock = Arc::new(ManualClock::new(now));
            let store = Arc::new(MemoryStore::with_clock(clock.clone()));
            let gate = Arc::new(SettlementGate::default());
            let effects = Arc::new(Mutex::new(Vec::new()));
            let decisions = Arc::new(AtomicUsize::new(0));
            let prepares = Arc::new(AtomicUsize::new(0));
            let provider = FakeProvider::scripted(vec![batch(), text_script("done")]);
            let make = || {
                Orchestrator::builder()
                    .behavior_version("recover-frozen-approval.v1")
                    .clock(clock.clone())
                    .store(Arc::new(DelayedTerminalStore {
                        inner: store.clone(),
                        gate: gate.clone(),
                    }))
                    .resolver(Arc::new(provider.clone()))
                    .plan_provider(Arc::new(StaticPlanProvider::new(
                        vec![tool(Arc::new(Effects(effects.clone())))],
                        vec![],
                    )))
                    .policy(Arc::new(Decisions(decisions.clone())))
                    .tool_pipeline(Arc::new(CountingPipeline(prepares.clone())))
                    .execution_mode(mode)
                    .max_turns(2)
                    .build()
                    .unwrap()
            };
            let runtime = make();
            let handle = runtime.start(request()).await.unwrap();
            let session = handle.session_id().clone();
            let run = handle.run_id().clone();
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
            let original = answers(&runtime, &session, &run).await;
            gate.approval_claim_ack_lost.store(true, Ordering::SeqCst);
            let Err(error) = runtime.continue_approval(original.clone()).await else {
                panic!("lost native acknowledgment");
            };
            assert!(matches!(
                error.operation_error(),
                RuntimeError::Approval(crabber_session::ApprovalError::UnknownStoreOutcome)
            ));
            let preparation = error.preparation_reconciliation().unwrap();
            assert_eq!(
                store
                    .get_session_owner(&session, &preparation.owner.id)
                    .await
                    .unwrap()
                    .state,
                crabber_session::session_owner::SessionOwnerState::Joined
            );
            assert!(effects.lock().unwrap().is_empty());
            assert_eq!(provider.requests().len(), 1);
            gate.reject_run_reads.store(true, Ordering::SeqCst);
            assert!(matches!(
                runtime.continue_approval(original.clone()).await.unwrap(),
                crate::Continuation::Replayed(_)
            ));
            let mut changed = original.clone();
            changed.continuation.generation = changed.continuation.generation.next().unwrap();
            assert!(matches!(
                runtime.lookup_approval_continuation(&changed).await,
                Err(RuntimeError::Approval(
                    crabber_session::ApprovalError::SemanticConflict
                ))
            ));
            changed = original.clone();
            changed.expected_revision = changed.expected_revision.next().unwrap();
            assert!(matches!(
                runtime.lookup_approval_continuation(&changed).await,
                Err(RuntimeError::Approval(
                    crabber_session::ApprovalError::SemanticConflict
                ))
            ));
            gate.reject_run_reads.store(false, Ordering::SeqCst);
            let mut recovery = crate::RecoverApproval {
                id: crabber_core::ControlId::new(),
                expected_revision: store.session_revision(&session).await.unwrap(),
                original: original.clone(),
            };
            assert!(matches!(
                runtime
                    .recover_approval_continuation(recovery.clone())
                    .await,
                Err(RuntimeError::Approval(
                    crabber_session::ApprovalError::LiveOwner
                ))
            ));
            drop(runtime);
            clock.set(now + time::Duration::seconds(31));
            let cold = make();
            if fault == 1 {
                gate.approval_recovery_ack_lost
                    .store(true, Ordering::SeqCst);
                let Err(error) = cold.recover_approval_continuation(recovery.clone()).await else {
                    panic!("lost recovery acknowledgment");
                };
                assert!(matches!(
                    error.operation_error(),
                    RuntimeError::Approval(crabber_session::ApprovalError::UnknownStoreOutcome)
                ));
                assert!(error.preparation_reconciliation().is_some());
                assert!(matches!(
                    cold.recover_approval_continuation(recovery.clone())
                        .await
                        .unwrap(),
                    crate::Continuation::Replayed(_)
                ));
                assert!(effects.lock().unwrap().is_empty());
                gate.approval_recovery_ack_lost
                    .store(false, Ordering::SeqCst);
                clock.set(now + time::Duration::seconds(62));
                recovery.id = crabber_core::ControlId::new();
                recovery.expected_revision = store.session_revision(&session).await.unwrap();
            }
            if fault == 2 {
                gate.begin_ack_lost.store(true, Ordering::SeqCst);
            }
            gate.release.notify_one();
            let crate::Continuation::Started { receipt, handle } = cold
                .recover_approval_continuation(recovery.clone())
                .await
                .unwrap()
            else {
                panic!("fresh recovery handle")
            };
            let done = tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
                .await
                .unwrap();
            if fault == 2 {
                assert!(matches!(
                    done,
                    Err(RuntimeError::Attempt(
                        crabber_session::AttemptError::UnknownStoreOutcome
                    ))
                ));
                assert_eq!(
                    store
                        .latest_execution_attempt(&run)
                        .await
                        .unwrap()
                        .unwrap()
                        .attempt
                        .state,
                    AttemptState::Started
                );
                assert!(effects.lock().unwrap().is_empty());
                clock.set(now + time::Duration::seconds(62));
                let mut next = recovery.clone();
                next.id = crabber_core::ControlId::new();
                next.expected_revision = store.session_revision(&session).await.unwrap();
                assert!(matches!(
                    cold.recover_approval_continuation(next).await,
                    Err(RuntimeError::Approval(
                        crabber_session::ApprovalError::InvalidTransition
                    ))
                ));
            } else {
                assert_eq!(done.unwrap().status, RunStatus::Completed);
                assert_eq!(provider.requests().len(), 2);
                let seen = effects.lock().unwrap().clone();
                if mode == ExecutionMode::Sequential {
                    assert_eq!(seen, vec!["allow", "grant"]);
                } else {
                    assert_eq!(seen.len(), 2);
                    assert!(seen.contains(&"allow".into()) && seen.contains(&"grant".into()));
                }
                let events = store.list_events(&session, None, 1000).await.unwrap();
                assert_eq!(
                    events
                        .iter()
                        .filter(|e| e.kind == EventKind::ToolCallSettled)
                        .map(|e| e.payload["call_id"].as_str().unwrap())
                        .collect::<Vec<_>>(),
                    vec!["z:/λ", "y:/λ", "b:/λ", "a:/λ"]
                );
                let started = events
                    .iter()
                    .filter(|e| e.kind == EventKind::TurnStarted)
                    .map(|e| e.turn_id.clone())
                    .collect::<Vec<_>>();
                let completed = events
                    .iter()
                    .filter(|e| e.kind == EventKind::TurnCompleted)
                    .map(|e| e.turn_id.clone())
                    .collect::<Vec<_>>();
                assert_eq!(started, completed);
                assert_eq!(started.len(), 2);
                assert_eq!(
                    store
                        .list_all_messages(&session)
                        .await
                        .unwrap()
                        .iter()
                        .filter(|m| m.role == Role::User)
                        .count(),
                    1
                );
            }
            assert_eq!(decisions.load(Ordering::SeqCst), 4);
            assert_eq!(prepares.load(Ordering::SeqCst), 4);
            gate.reject_run_reads.store(true, Ordering::SeqCst);
            assert!(
                matches!(cold.recover_approval_continuation(recovery).await.unwrap(),crate::Continuation::Replayed(saved) if saved==receipt)
            );
            assert!(matches!(
                cold.continue_approval(original).await.unwrap(),
                crate::Continuation::Replayed(_)
            ));
            gate.reject_run_reads.store(false, Ordering::SeqCst);
            assert!(matches!(
                cold.resume(&run).await,
                Err(RuntimeError::DurableContinuationRequired { .. }
                    | RuntimeError::Store(StoreError::Conflict))
            ));
        }
    }
}

#[tokio::test]
async fn dropping_recovery_observation_retains_the_mutation_and_worker() {
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let gate = Arc::new(SettlementGate::default());
    let effects = Arc::new(Mutex::new(Vec::new()));
    let provider = FakeProvider::scripted(vec![batch(), text_script("done")]);
    let runtime = Orchestrator::builder()
        .behavior_version("recover-retained-approval.v1")
        .clock(clock.clone())
        .store(Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(Effects(effects.clone())))],
            vec![],
        )))
        .policy(Arc::new(Decisions(Arc::new(AtomicUsize::new(0)))))
        .max_turns(2)
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let original = answers(&runtime, &session, &run).await;
    gate.approval_claim_ack_lost.store(true, Ordering::SeqCst);
    assert!(runtime.continue_approval(original.clone()).await.is_err());
    clock.set(now + time::Duration::seconds(31));
    let recovery = crate::RecoverApproval {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&session).await.unwrap(),
        original: original.clone(),
    };
    gate.approval_recovery_gate.store(true, Ordering::SeqCst);
    let caller_runtime = runtime.clone();
    let caller_request = recovery.clone();
    let caller = tokio::spawn(async move {
        caller_runtime
            .recover_approval_continuation(caller_request)
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        gate.approval_recovery_entered.notified(),
    )
    .await
    .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert!(
        store
            .lookup_approval(&session, &recovery.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(effects.lock().unwrap().is_empty());
    gate.release.notify_one();
    gate.approval_recovery_release.notify_one();
    let drain = runtime
        .drain_execution_tasks(std::time::Duration::from_secs(2))
        .await;
    assert_eq!(drain.pending, vec![]);
    assert_eq!(drain.pending_preparations, vec![]);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(*effects.lock().unwrap(), vec!["allow", "grant"]);
    assert_eq!(provider.requests().len(), 2);
    assert!(matches!(
        runtime
            .recover_approval_continuation(recovery)
            .await
            .unwrap(),
        crate::Continuation::Replayed(_)
    ));
    assert!(matches!(
        runtime.continue_approval(original).await.unwrap(),
        crate::Continuation::Replayed(_)
    ));
}

#[tokio::test]
async fn recovery_allows_prior_completed_phases_without_resetting_turn_budget() {
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let gate = Arc::new(SettlementGate::default());
    let effects = Arc::new(Mutex::new(Vec::new()));
    let decisions = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![
        call_script(ToolCallId("prior-completed".into()), r#"{"text":"prior"}"#),
        batch(),
    ]);
    let make = || {
        Orchestrator::builder()
            .behavior_version("recovery-total-budget.v1")
            .clock(clock.clone())
            .store(Arc::new(DelayedTerminalStore {
                inner: store.clone(),
                gate: gate.clone(),
            }))
            .resolver(Arc::new(provider.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(Effects(effects.clone())))],
                vec![],
            )))
            .policy(Arc::new(Decisions(decisions.clone())))
            .max_turns(2)
            .build()
            .unwrap()
    };
    let runtime = make();
    // The first completed phase must not obstruct recovery of a later frozen batch.
    gate.release.notify_one();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(*effects.lock().unwrap(), vec!["prior"]);
    let original = answers(&runtime, &session, &run).await;
    gate.approval_claim_ack_lost.store(true, Ordering::SeqCst);
    assert!(runtime.continue_approval(original.clone()).await.is_err());
    let recovery = crate::RecoverApproval {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&session).await.unwrap(),
        original,
    };
    clock.set(now + time::Duration::seconds(31));
    drop(runtime);
    let cold = make();
    gate.release.notify_one();
    let crate::Continuation::Started { handle, .. } =
        cold.recover_approval_continuation(recovery).await.unwrap()
    else {
        panic!("fresh recovery")
    };
    assert!(matches!(handle.done().await, Err(RuntimeError::TurnLimit)));
    assert_eq!(*effects.lock().unwrap(), vec!["prior", "allow", "grant"]);
    assert_eq!(decisions.load(Ordering::SeqCst), 5);
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
}
