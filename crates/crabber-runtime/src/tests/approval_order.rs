//! Provider order survives storage enumeration at durable pause boundaries.
use super::*;
use crabber_session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval};

struct OrderedTool(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl JsonToolExecutor for OrderedTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0
            .lock()
            .unwrap()
            .push(arguments["text"].as_str().unwrap().to_owned());
        // Make a parallel batch genuinely overlap without constraining its start order.
        tokio::task::yield_now().await;
        Ok(arguments)
    }
}
#[derive(Clone, Copy)]
enum Boundary {
    Fresh,
    Approval,
    Generic,
}
fn script() -> Vec<StreamDelta> {
    let mut script = Vec::new();
    for (id, text) in [("z-provider:/λ", "first"), ("a-provider:/λ", "second")] {
        let mut call = call_script(ToolCallId(id.into()), &json!({"text":text}).to_string());
        call.pop();
        script.extend(call);
    }
    script.push(StreamDelta::Completed);
    script
}
fn policy(boundary: Boundary) -> Arc<dyn PermissionPolicy> {
    match boundary {
        Boundary::Fresh => Arc::new(StaticPolicy::new(PermissionDecision::Allow)),
        Boundary::Approval => Arc::new(StaticPolicy::new(PermissionDecision::Ask)),
        Boundary::Generic => Arc::new(PausePolicy),
    }
}
async fn answer_all(
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
    for call in &snapshot.calls {
        let CallApproval::Ask { id, description } = &call.decision else {
            panic!("approval")
        };
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
                    authority_claim: "verified test decision".into(),
                },
                reply: ApprovalReply::Grant,
            })
            .await
            .unwrap()
            .revision;
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
async fn opaque_reverse_ids_preserve_frozen_executor_and_settlement_order() {
    for mode in [
        ExecutionMode::Sequential,
        ExecutionMode::Parallel { max: 2 },
    ] {
        for boundary in [Boundary::Fresh, Boundary::Approval, Boundary::Generic] {
            let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
            let store = Arc::new(MemoryStore::with_clock(clock.clone()));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let provider = FakeProvider::scripted(vec![script(), text_script("done")]);
            let runtime = Orchestrator::builder()
                .behavior_version("frozen-order.v1")
                .store(store.clone())
                .clock(clock)
                .resolver(Arc::new(provider.clone()))
                .plan_provider(Arc::new(StaticPlanProvider::new(
                    vec![tool(Arc::new(OrderedTool(seen.clone())))],
                    vec![],
                )))
                .policy(policy(boundary))
                .execution_mode(mode)
                .build()
                .unwrap();
            let handle = runtime.start(request()).await.unwrap();
            let session = handle.session_id().clone();
            let run = handle.run_id().clone();
            let first = handle.done().await.unwrap();
            if matches!(boundary, Boundary::Fresh) {
                assert_eq!(first.status, RunStatus::Completed);
            } else {
                assert_eq!(first.status, RunStatus::Paused);
                assert!(seen.lock().unwrap().is_empty());
                let pending = store.list_unfinished_tool_calls(&run).await.unwrap();
                assert_eq!(
                    pending
                        .iter()
                        .map(|call| call.id.0.as_str())
                        .collect::<Vec<_>>(),
                    vec!["a-provider:/λ", "z-provider:/λ"]
                );
                match boundary {
                    Boundary::Approval => {
                        let request = answer_all(&runtime, &session, &run).await;
                        let crate::Continuation::Started { handle, .. } =
                            runtime.continue_approval(request).await.unwrap()
                        else {
                            panic!("fresh continuation")
                        };
                        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
                    }
                    Boundary::Generic => {
                        assert_eq!(
                            runtime.resume(&run).await.unwrap().status,
                            RunStatus::Completed
                        );
                    }
                    Boundary::Fresh => unreachable!(),
                }
            }
            let seen = seen.lock().unwrap().clone();
            if matches!(mode, ExecutionMode::Sequential) {
                assert_eq!(seen, vec!["first", "second"]);
            } else {
                let mut seen = seen;
                seen.sort();
                assert_eq!(seen, vec!["first", "second"]);
            }
            let events = store.list_events(&session, None, 1000).await.unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.kind == EventKind::ToolCallSettled)
                    .map(|event| event.payload["call_id"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec!["z-provider:/λ", "a-provider:/λ"]
            );
            let started = events
                .iter()
                .filter(|event| event.kind == EventKind::TurnStarted)
                .map(|event| event.turn_id.clone())
                .collect::<Vec<_>>();
            let completed = events
                .iter()
                .filter(|event| event.kind == EventKind::TurnCompleted)
                .map(|event| event.turn_id.clone())
                .collect::<Vec<_>>();
            assert_eq!(completed, started);
            assert_eq!(provider.requests().len(), 2);
        }
    }
}

#[tokio::test]
async fn invalid_generic_checkpoint_call_sets_are_rejected_before_claim_or_effects() {
    for ids in [
        json!(["z-provider:/λ"]),
        json!(["z-provider:/λ", "z-provider:/λ"]),
        json!(["z-provider:/λ", "a-provider:/λ", "extra"]),
        json!(["z-provider:/λ", "foreign"]),
        Value::Null,
    ] {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let gate = Arc::new(SettlementGate::default());
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let provider = FakeProvider::scripted(vec![script()]);
        let runtime = Orchestrator::builder()
            .behavior_version("frozen-order.v1")
            .store(wrapped)
            .clock(clock)
            .resolver(Arc::new(provider.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(OrderedTool(seen.clone())))],
                vec![],
            )))
            .policy(Arc::new(PausePolicy))
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let before = store.get_run(&run).await.unwrap().unwrap();
        let events = store.list_events(&session, None, 1000).await.unwrap();
        let revision = store.session_revision(&session).await.unwrap();
        let mut checkpoint = before.checkpoint.clone().unwrap();
        if ids.is_null() {
            checkpoint.as_object_mut().unwrap().remove("pending_calls");
        } else {
            checkpoint["pending_calls"] = ids;
        }
        *gate.checkpoint_override.lock().unwrap() = Some(checkpoint);
        assert!(matches!(
            runtime.resume(&run).await,
            Err(RuntimeError::Approval(
                crabber_session::ApprovalError::SemanticConflict
            ))
        ));
        assert_eq!(store.get_run(&run).await.unwrap().unwrap(), before);
        assert_eq!(
            store.list_events(&session, None, 1000).await.unwrap(),
            events
        );
        assert_eq!(store.session_revision(&session).await.unwrap(), revision);
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(provider.requests().len(), 1);
    }
}

struct AskAfterGenericPause(AtomicUsize);
impl PermissionPolicy for AskAfterGenericPause {
    fn decide(&self, _: &crabber_core::ToolInfo, _: &Value) -> PermissionDecision {
        if self.0.fetch_add(1, Ordering::SeqCst) < 2 {
            PermissionDecision::Allow
        } else {
            PermissionDecision::Ask
        }
    }
    fn interrupt_policy(&self, _: &crabber_core::ToolInfo, _: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}

#[tokio::test]
async fn generic_pause_transition_to_approval_keeps_the_original_turn() {
    let store = Arc::new(MemoryStore::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let policy = Arc::new(AskAfterGenericPause(AtomicUsize::new(0)));
    let provider = FakeProvider::scripted(vec![script(), text_script("done")]);
    let runtime = Orchestrator::builder()
        .behavior_version("generic-to-approval-turn.v1")
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(OrderedTool(seen.clone())))],
            vec![],
        )))
        .policy(policy.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let initial = store.list_events(&session, None, 1000).await.unwrap();
    let original_turn = initial
        .iter()
        .find(|event| event.kind == EventKind::TurnStarted)
        .unwrap()
        .turn_id
        .clone()
        .unwrap();
    assert_eq!(
        runtime.resume(&run).await.unwrap().status,
        RunStatus::Paused
    );
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(policy.0.load(Ordering::SeqCst), 4);
    let waiting = store.list_events(&session, None, 1000).await.unwrap();
    let pauses = waiting
        .iter()
        .filter(|event| event.kind == EventKind::RunPaused)
        .collect::<Vec<_>>();
    assert_eq!(pauses.len(), 2);
    assert!(
        pauses
            .iter()
            .all(|event| event.turn_id.as_ref() == Some(&original_turn))
    );
    let request = answer_all(&runtime, &session, &run).await;
    let crate::Continuation::Started { handle, .. } =
        runtime.continue_approval(request).await.unwrap()
    else {
        panic!("fresh continuation");
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(*seen.lock().unwrap(), vec!["first", "second"]);
    assert_eq!(policy.0.load(Ordering::SeqCst), 4);
    assert_eq!(provider.requests().len(), 2);
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let tool_events = events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                EventKind::ToolCallRunning | EventKind::ToolCallSettled
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(tool_events.len(), 4);
    assert!(
        tool_events
            .iter()
            .all(|event| event.turn_id.as_ref() == Some(&original_turn))
    );
    let started = events
        .iter()
        .filter(|event| event.kind == EventKind::TurnStarted)
        .map(|event| event.turn_id.clone())
        .collect::<Vec<_>>();
    let completed = events
        .iter()
        .filter(|event| event.kind == EventKind::TurnCompleted)
        .map(|event| event.turn_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(started.len(), 2);
    assert_eq!(completed, started);
}
