//! Real executor counters and original-key reconciliation across unknown ACKs.
use super::*;
use crabber_session::{AttemptState, PhaseState};

#[tokio::test]
async fn unknown_phase_start_and_settlement_never_dispatch_or_settle_twice() {
    for fault in 1..=4 {
        let now = time::OffsetDateTime::UNIX_EPOCH;
        let clock = Arc::new(ManualClock::new(now));
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let gate = Arc::new(SettlementGate::default());
        gate.phase_fault.store(fault, Ordering::SeqCst);
        let effects = Arc::new(AtomicUsize::new(0));
        let provider =
            FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"effect"}"#)]);
        let observer = Arc::new(OperationalCapture::default());
        let runtime = Orchestrator::builder()
            .behavior_version("native-phase-fault.v1")
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
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .observer(observer.clone())
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        let RuntimeError::PhaseOutcomeUnknown {
            phase,
            operation_id: Some(key),
            reason,
            ..
        } = handle.done().await.unwrap_err()
        else {
            panic!("retain exact native operation identity")
        };
        assert_eq!(reason, crabber_session::PhaseError::UnknownStoreOutcome);
        assert_eq!(effects.load(Ordering::SeqCst), usize::from(fault >= 3));
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(gate.settlements.load(Ordering::SeqCst), 0);
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
        let stored = store.tool_phase(&run, &phase.id).await.unwrap().phase;
        assert_eq!(stored.target, *phase);
        assert_eq!(
            stored.state,
            match fault {
                1 => PhaseState::Claimed,
                4 => PhaseState::Completed,
                _ => PhaseState::Started,
            }
        );
        let receipt = store.lookup_phase_mutation(&session, &key).await.unwrap();
        assert_eq!(receipt.is_some(), fault == 2 || fault == 4);
        if let Some(receipt) = receipt {
            assert_eq!(receipt.phase.target, *phase);
        }
        let events = store.list_events(&session, None, 1000).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::ToolCallSettled)
                .count(),
            usize::from(fault == 4)
        );
        assert!(!events.iter().any(|e| e.kind == EventKind::RunSettled));
        assert!(
            !observer
                .records
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.kind == EventKind::ToolCallSettled)
        );
        assert!(matches!(
            runtime.resume(&run).await,
            Err(RuntimeError::Store(StoreError::Conflict))
        ));
        clock.set(now + time::Duration::seconds(31));
        assert!(matches!(
            runtime.resume(&run).await,
            Err(RuntimeError::Attempt(crabber_session::AttemptError::Busy))
        ));
        assert_eq!(effects.load(Ordering::SeqCst), usize::from(fault >= 3));
        assert_eq!(provider.requests().len(), 1);
    }
}

struct PanickingSettlementObserver;
impl Observer for PanickingSettlementObserver {
    fn emit(&self, event: &EventRecord) {
        assert_ne!(
            event.kind,
            EventKind::ToolCallSettled,
            "notification failed after commit"
        );
    }
}
#[tokio::test]
async fn postcommit_observer_panic_preserves_the_acknowledged_phase() {
    let store = Arc::new(MemoryStore::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let provider =
        FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"effect"}"#)]);
    let runtime = Orchestrator::builder()
        .behavior_version("native-phase-observer.v1")
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(effects.clone())))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(Arc::new(PanickingSettlementObserver))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert!(
        matches!(handle.done().await, Err(RuntimeError::Extension(message))
        if message == "tool settlement notification failed")
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    let report = runtime
        .drain_execution_tasks(std::time::Duration::from_secs(1))
        .await;
    let target = report.resources[0].phase.as_ref().unwrap();
    let phase = store.tool_phase(&run, &target.id).await.unwrap().phase;
    assert_eq!(phase.state, PhaseState::Completed);
    assert_eq!(phase.quiescence, crabber_session::PhaseQuiescence::Complete);
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .state,
        AttemptState::Failed
    );
    assert_eq!(
        store.list_unfinished_tool_calls(&run).await.unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
    );
    let events = store.list_events(&session, None, 1000).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::RunSettled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolPhaseChanged)
            .count(),
        3
    );
}

#[tokio::test]
async fn fresh_phase_claim_must_match_the_authorized_call_binding() {
    let store = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![call_script(
        ToolCallId::new(),
        r#"{"text":"original"}"#,
    )]);
    let runtime = Orchestrator::builder()
        .behavior_version("native-phase-binding.v1")
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
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    *gate.call_arguments_override.lock().unwrap() = Some(json!({"text":"forged-read"}));
    let RuntimeError::PhaseOutcomeUnknown {
        phase,
        operation_id: Some(key),
        ..
    } = runtime.resume(&run).await.unwrap_err()
    else {
        panic!("fresh claim must attest to the exact authorized stored input")
    };
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
    let receipt = store
        .lookup_phase_mutation(&session, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.operation, crabber_session::PhaseOperation::Claim);
    assert_eq!(receipt.phase.target, *phase);
    assert_eq!(receipt.phase.state, PhaseState::Claimed);
    let calls = store.list_unfinished_tool_calls(&run).await.unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].arguments, json!({"text":"original"}));
    assert_eq!(calls[0].status, crabber_core::ToolCallStatus::Pending);
    assert!(calls[0].result.is_none());
    assert!(
        !store
            .list_events(&session, None, 1000)
            .await
            .unwrap()
            .iter()
            .any(|event| matches!(
                event.kind,
                EventKind::ToolCallRunning | EventKind::ToolCallSettled | EventKind::RunSettled
            ))
    );
}
