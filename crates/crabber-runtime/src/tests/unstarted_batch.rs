//! A lost generic continuation reservation ACK cannot discard its frozen batch.
use super::*;
use crabber_session::{AttemptError, AttemptState};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cold_unstarted_continuation_executes_pending_batch_before_another_model_turn() {
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let gate = Arc::new(SettlementGate::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let call = ToolCallId("provider:/λ/frozen".into());
    let provider = FakeProvider::scripted(vec![
        call_script(call.clone(), r#"{"text":"one frozen effect"}"#),
        text_script("done"),
    ]);
    let make = || {
        Orchestrator::builder()
            .behavior_version("unstarted-frozen-batch.v1")
            .clock(clock.clone())
            .store(Arc::new(DelayedTerminalStore {
                inner: store.clone(),
                gate: gate.clone(),
            }))
            .resolver(Arc::new(provider.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(EchoTool(effects.clone())))],
                vec![],
            )))
            .policy(Arc::new(PausePolicy))
            .build()
            .unwrap()
    };
    let runtime = make();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let paused = store.get_run(&run).await.unwrap().unwrap();
    gate.reserve_ack_lost.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.resume(&run).await,
        Err(RuntimeError::Attempt(AttemptError::UnknownStoreOutcome))
    ));
    let reserved = store.get_run(&run).await.unwrap().unwrap();
    assert_eq!(reserved.status, RunStatus::Running);
    assert_eq!(reserved.checkpoint, paused.checkpoint);
    let attempt = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(attempt.state, AttemptState::Unstarted);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
    drop(runtime);
    clock.set(now + time::Duration::seconds(31));
    gate.reserve_ack_lost.store(false, Ordering::SeqCst);
    let cold = make();

    // Invalid checkpoint reads must be refused before reclaiming even this
    // provably Unstarted attempt. The private batch cannot be inferred away.
    let mut invalid = reserved.checkpoint.clone().unwrap();
    invalid["pending_calls"] = json!([]);
    *gate.checkpoint_override.lock().unwrap() = Some(invalid);
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let revision = store.session_revision(&session).await.unwrap();
    assert!(matches!(
        cold.resume(&run).await,
        Err(RuntimeError::Approval(
            crabber_session::ApprovalError::SemanticConflict
        ))
    ));
    assert_eq!(store.get_run(&run).await.unwrap().unwrap(), reserved);
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt,
        attempt
    );
    assert_eq!(
        store.list_events(&session, None, 1000).await.unwrap(),
        events
    );
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    *gate.checkpoint_override.lock().unwrap() = None;

    // This shared fault wrapper also delays terminal ACKs until released.
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), cold.resume(&run))
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    let completed = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(completed.target, attempt.target);
    assert_eq!(completed.state, AttemptState::Completed);
    assert_eq!(
        store.list_unfinished_tool_calls(&run).await.unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
    );
    let history = store.list_all_messages(&session).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        1
    );
    assert_eq!(history.iter().flat_map(|message| &message.parts).filter(|part|
        matches!(&part.content, ContentBlock::ToolResult { call_id, .. } if *call_id == call)).count(), 1);
    let events = store.list_events(&session, None, 1000).await.unwrap();
    for kind in [
        EventKind::RunAdmitted,
        EventKind::RunStarted,
        EventKind::ToolCallRunning,
        EventKind::ToolCallSettled,
    ] {
        assert_eq!(events.iter().filter(|event| event.kind == kind).count(), 1);
    }
    let started = events
        .iter()
        .filter(|event| event.kind == EventKind::TurnStarted)
        .map(|event| event.turn_id.clone())
        .collect::<Vec<_>>();
    let finished = events
        .iter()
        .filter(|event| event.kind == EventKind::TurnCompleted)
        .map(|event| event.turn_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(started.len(), 2);
    assert_eq!(finished, started);
    assert!(matches!(
        cold.resume(&run).await,
        Err(RuntimeError::Store(StoreError::Conflict))
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
}
