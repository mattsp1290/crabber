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

#[tokio::test]
async fn resumed_interruption_requires_acknowledged_terminal_commit() {
    for fault in [1, 2] {
        resumed_missing_terminal_ack(fault).await;
    }
}
async fn resumed_missing_terminal_ack(fault: usize) {
    let inner = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.terminal_fault.store(fault, Ordering::SeqCst);
    let capture = Arc::new(OperationalCapture::default());
    let (entered, ready) = oneshot::channel();
    let runtime = Orchestrator::builder()
        .behavior_version("resumed-terminal-test.v1")
        .store(Arc::new(DelayedTerminalStore {
            inner: inner.clone(),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(FakeProvider::scripted(vec![call_script(
            ToolCallId::new(),
            r#"{"text":"pending"}"#,
        )])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(CancellableTool {
                entered: Mutex::new(Some(entered)),
                release: Arc::new(Notify::new()),
                completed: Arc::new(AtomicUsize::new(0)),
            }))],
            vec![],
        )))
        .policy(Arc::new(PausePolicy))
        .observer(capture.clone())
        .build()
        .unwrap();
    let initial = runtime.start(request()).await.unwrap();
    let run = initial.run_id().clone();
    let session = initial.session_id().clone();
    assert_eq!(initial.done().await.unwrap().status, RunStatus::Paused);
    let mut resumed = runtime.resume_execution(&run).await.unwrap();
    let mut events = resumed.events();
    tokio::time::timeout(std::time::Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    resumed.control().interrupt();
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), resumed.done())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, RuntimeError::TerminalOutcomeUnknown { run_id, .. } if run_id == run));
    assert_eq!(gate.settlements.load(Ordering::SeqCst), 1);
    assert_eq!(
        inner.get_run(&run).await.unwrap().unwrap().status,
        if fault == 1 {
            // The claim rotates ownership; a rejected terminal write retains
            // the previously committed logical status.
            RunStatus::Paused
        } else {
            RunStatus::Interrupted
        }
    );
    let durable = inner.list_events(&session, None, 1000).await.unwrap();
    assert_eq!(
        durable
            .iter()
            .filter(|event| event.kind == EventKind::RunSettled)
            .count(),
        usize::from(fault == 2)
    );
    while let Some(event) = events.recv().await.unwrap() {
        assert_ne!(event.kind, EventKind::RunSettled);
    }
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.kind == EventKind::RunSettled)
    );
}
