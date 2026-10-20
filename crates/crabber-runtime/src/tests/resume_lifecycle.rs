//! Existing private fixture/suite, preserving actual native cuts and joins.
use super::*;

#[tokio::test]
async fn interrupting_an_inflight_executor_retains_uncertainty() {
    let store = Arc::new(MemoryStore::new());
    let (entered_tx, entered_rx) = oneshot::channel();
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::clone(&store),
        FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"ok"}"#)]),
        Arc::new(CancellableTool {
            entered: Mutex::new(Some(entered_tx)),
            release: Arc::new(Notify::new()),
            completed: Arc::clone(&completed),
        }),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    entered_rx.await.unwrap();
    handle.interrupt();
    let RuntimeError::PhaseOutcomeUnknown { phase, reason, .. } = handle.done().await.unwrap_err()
    else {
        panic!("interrupted executor needs reconciliation")
    };
    assert_eq!(reason, crabber_session::PhaseError::Uncertain);
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    let stored = store.tool_phase(&run_id, &phase.id).await.unwrap().phase;
    assert_eq!(stored.state, crabber_session::PhaseState::Uncertain);
    assert_eq!(
        stored.quiescence,
        crabber_session::PhaseQuiescence::Complete
    );
    let calls = store.list_unfinished_tool_calls(&run_id).await.unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, crabber_core::ToolCallStatus::Running);
    assert!(calls[0].result.is_none());
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    assert!(matches!(
        runtime.resume(&run_id).await,
        Err(RuntimeError::Store(StoreError::Conflict))
    ));
}

#[tokio::test]
async fn paused_call_resumes_from_persisted_input() {
    let store = Arc::new(MemoryStore::new());
    let executed = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(AtomicUsize::new(0));
    let capture = Arc::new(OperationalCapture::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .observer(capture.clone())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
            text_script("resumed"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .policy(Arc::new(PausePolicy))
        .tool_pipeline(Arc::new(CountingPipeline(Arc::clone(&prepared))))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        runtime.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    let values = capture.values.lock().unwrap().clone();
    let runs: Vec<_> = values
        .iter()
        .filter(|value| value.kind == crate::OperationKind::Run)
        .collect();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].reason, crate::TerminalReason::Paused);
    assert_eq!(runs[1].reason, crate::TerminalReason::Success);
    assert_eq!(
        store.list_unfinished_tool_calls(&run_id).await.unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
    );
}

#[tokio::test]
async fn resumed_tool_stops_when_lease_is_reclaimed() {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let (entered_tx, entered_rx) = oneshot::channel();
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .resolver(Arc::new(FakeProvider::scripted(vec![call_script(
            ToolCallId::new(),
            r#"{"text":"ok"}"#,
        )])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(CancellableTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::new(Notify::new()),
                completed: Arc::clone(&completed),
            }))],
            Vec::new(),
        )))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let resumed = tokio::spawn({
        let runtime = runtime.clone();
        let run_id = run_id.clone();
        async move { runtime.resume(&run_id).await }
    });
    entered_rx.await.unwrap();
    clock.set(now + time::Duration::seconds(31));
    store.claim_expired_run(&run_id, "new owner").await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), resumed)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(RuntimeError::LeaseLost)));
    assert_eq!(completed.load(Ordering::SeqCst), 0);
}

#[allow(clippy::too_many_lines)]
async fn paused_with_legacy_history_drift(
    has_tool: bool,
    compacted: bool,
) -> (
    Orchestrator,
    Arc<MemoryStore>,
    FakeProvider,
    Arc<AtomicUsize>,
    RunId,
) {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"first"}"#),
        text_script("final"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .clock(clock.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    let session_id = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let fence = store
        .claim_expired_run(&run_id, "crashed resumed owner")
        .await
        .unwrap();
    let execution = store.execution(fence).await.unwrap();
    let original_call = store
        .list_unfinished_tool_calls(&run_id)
        .await
        .unwrap()
        .remove(0);
    let event = |kind| EventRecord {
        cursor: None,
        session_id: session_id.clone(),
        run_id: run_id.clone(),
        turn_id: None,
        kind,
        payload: Value::Null,
        correlation: None,
        live_only: false,
        created_at: now,
    };
    execution
        .claim_tool_call(&original_call.id, event(EventKind::ToolCallRunning))
        .await
        .unwrap();
    let result_message_id = crabber_core::MessageId::new();
    execution
        .settle_tool_call(
            &original_call.id,
            ToolResult {
                status: crabber_core::ToolResultStatus::Completed,
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
            Message {
                id: result_message_id.clone(),
                session_id: session_id.clone(),
                run_id: Some(run_id.clone()),
                role: Role::Tool,
                parent_id: None,
                created_at: now,
                parts: vec![Part {
                    id: crabber_core::PartId::new(),
                    message_id: result_message_id.clone(),
                    ordinal: 0,
                    kind: crabber_core::PartKind::FunctionToolResult,
                    content: ContentBlock::ToolResult {
                        call_id: original_call.id.clone(),
                        content: vec![ContentBlock::Text {
                            text: "done".into(),
                        }],
                        is_error: false,
                    },
                }],
            },
            event(EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    if compacted {
        let history = store.list_all_messages(&session_id).await.unwrap();
        let epoch = ContextEpoch {
            id: EpochId::new(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            parent: Some(store.get_run(&run_id).await.unwrap().unwrap().epoch_id),
            summarized_range: Some((history[0].id.clone(), history[history.len() - 2].id.clone())),
            summary_message_id: None,
            tail_start_message_id: Some(result_message_id.clone()),
            provider_id: "fake".into(),
            model_id: "fake".into(),
            reason: "test crash window".into(),
            next_policy: None,
        };
        execution.start_epoch(epoch.clone()).await.unwrap();
        let summary_id = crabber_core::MessageId::new();
        execution
            .finish_epoch(
                &epoch.id,
                Message {
                    id: summary_id.clone(),
                    session_id: session_id.clone(),
                    run_id: Some(run_id.clone()),
                    role: Role::Assistant,
                    parent_id: None,
                    created_at: now,
                    parts: vec![Part {
                        id: crabber_core::PartId::new(),
                        message_id: summary_id,
                        ordinal: 0,
                        kind: crabber_core::PartKind::CompactionSummary,
                        content: ContentBlock::Text {
                            text: "prior context".into(),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        assert!(store.list_messages(&session_id, None).await.unwrap().iter().all(|message|
            !message.parts.iter().any(|part| matches!(&part.content, ContentBlock::ToolCall { call_id, .. } if call_id == &original_call.id))));
    }
    let message_id = crabber_core::MessageId::new();
    let (kind, content) = if has_tool {
        (
            crabber_core::PartKind::FunctionToolCall,
            ContentBlock::ToolCall {
                call_id: ToolCallId::new(),
                name: "echo".into(),
                arguments: json!({"text":"recovered"}),
            },
        )
    } else {
        (
            crabber_core::PartKind::AssistantText,
            ContentBlock::Text {
                text: "already completed".into(),
            },
        )
    };
    execution
        .append_message(Message {
            id: message_id.clone(),
            session_id,
            run_id: Some(run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            created_at: now,
            parts: vec![Part {
                id: crabber_core::PartId::new(),
                message_id,
                ordinal: 0,
                kind,
                content,
            }],
        })
        .await
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    (runtime, store, fake, executed, run_id)
}

#[tokio::test]
async fn recovery_refuses_history_that_drifted_from_the_native_pause() {
    for has_tool in [false, true] {
        for compacted in [false, true] {
            // Legacy writes fabricated later progress after a genuine native
            // suspension. It cannot replace the frozen continuation evidence.
            let (runtime, store, provider, effects, run) =
                paused_with_legacy_history_drift(has_tool, compacted).await;
            let before = store.get_run(&run).await.unwrap().unwrap();
            let revision = store.session_revision(&before.session_id).await.unwrap();
            let messages = store.list_all_messages(&before.session_id).await.unwrap();
            let events = store
                .list_events(&before.session_id, None, 1000)
                .await
                .unwrap();
            let attempt = store.latest_execution_attempt(&run).await.unwrap();
            assert!(matches!(
                runtime.resume(&run).await,
                Err(RuntimeError::Approval(
                    crabber_session::ApprovalError::SemanticConflict
                ))
            ));
            assert_eq!(provider.requests().len(), 1);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert_eq!(store.get_run(&run).await.unwrap().unwrap(), before);
            assert_eq!(
                store.session_revision(&before.session_id).await.unwrap(),
                revision
            );
            assert_eq!(
                store.list_all_messages(&before.session_id).await.unwrap(),
                messages
            );
            assert_eq!(
                store
                    .list_events(&before.session_id, None, 1000)
                    .await
                    .unwrap(),
                events
            );
            assert_eq!(store.latest_execution_attempt(&run).await.unwrap(), attempt);
        }
    }
}
