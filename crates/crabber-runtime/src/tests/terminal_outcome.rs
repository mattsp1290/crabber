use super::*;

#[tokio::test]
async fn interruption_requires_acknowledged_terminal_commit() {
    for fault in [1, 2] {
        missing_terminal_ack(true, fault).await;
    }
}
#[tokio::test]
async fn failed_execution_exposes_unknown_terminal_persistence() {
    for fault in [1, 2] {
        missing_terminal_ack(false, fault).await;
    }
}
async fn missing_terminal_ack(interrupted: bool, fault: usize) {
    let inner = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.terminal_fault.store(fault, Ordering::SeqCst);
    let capture = Arc::new(OperationalCapture::default());
    let clock = Arc::new(OperationalClock::default());
    let mut stream = operational_stream(clock);
    stream.startup_error = !interrupted;
    stream.block_end = interrupted;
    stream.script = vec![(1, StreamDelta::TextDelta("partial".into()))];
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(DelayedTerminalStore {
            inner: inner.clone(),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(stream))
        .observer(capture.clone())
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    let session = handle.session_id().clone();
    if interrupted {
        tokio::time::timeout(std::time::Duration::from_secs(5), capture.text.notified())
            .await
            .unwrap();
        handle.interrupt();
    }
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), handle.done())
        .await
        .unwrap()
        .unwrap_err();
    let RuntimeError::TerminalOutcomeUnknown {
        run_id: uncertain_run,
        reason,
    } = error
    else {
        panic!("terminal acknowledgement failure must remain explicit: {error:?}");
    };
    assert_eq!(uncertain_run, run_id);
    assert_eq!(
        reason,
        StoreError::Validation("terminal acknowledgement unavailable".into())
    );
    assert_eq!(gate.settlements.load(Ordering::SeqCst), 1);
    let expected = if interrupted {
        RunStatus::Interrupted
    } else {
        RunStatus::Failed
    };
    assert_eq!(
        inner.get_run(&run_id).await.unwrap().unwrap().status,
        if fault == 1 {
            RunStatus::Running
        } else {
            expected
        }
    );
    let events = inner.list_events(&session, None, 100).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::RunSettled)
            .count(),
        usize::from(fault == 2)
    );
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.kind == EventKind::RunSettled)
    );
    assert_eq!(
        capture.values.lock().unwrap().last().unwrap().reason,
        crate::TerminalReason::RuntimeError
    );
}
