use super::*;
use crabber_core::event_payload::{MessageEnded, StreamOutcome};

#[derive(Default)]
struct Capture {
    records: Mutex<Vec<EventRecord>>,
    delta: Notify,
}
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.records.lock().unwrap().push(event.clone());
        if matches!(
            event.kind,
            EventKind::TextDelta | EventKind::ToolCallArgsDelta
        ) {
            self.delta.notify_one();
        }
    }
}

fn runtime(
    store: Arc<dyn Store>,
    fake: FakeProvider,
    capture: Arc<Capture>,
) -> crate::OrchestratorBuilder {
    Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(capture)
}

fn assert_boundaries(records: &[EventRecord]) {
    let mut active = None;
    let mut calls = std::collections::HashSet::new();
    for record in records {
        match record.kind {
            EventKind::MessageStarted => {
                assert!(active.is_none());
                active = Some(record.payload["message_id"].clone());
            }
            EventKind::TextDelta
            | EventKind::ReasoningDelta
            | EventKind::ToolCallStarted
            | EventKind::ToolCallArgsDelta
            | EventKind::ToolCallArgsCompleted
            | EventKind::MessageStreamEnded => {
                assert_eq!(active.as_ref(), Some(&record.payload["message_id"]));
                assert!(record.live_only);
                assert!(record.turn_id.is_some());
                match record.kind {
                    EventKind::ToolCallStarted => {
                        assert!(calls.insert(record.payload["call_id"].to_string()));
                    }
                    EventKind::ToolCallArgsCompleted => {
                        assert!(calls.remove(&record.payload["call_id"].to_string()));
                    }
                    EventKind::MessageStreamEnded => {
                        assert!(calls.is_empty());
                        active = None;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    assert!(active.is_none());
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Exercise three real source/store journeys with common correlation assertions.
async fn identities_correlate_live_durable_and_stored_parallel_rounds() {
    for scripts in [
        vec![text_script("hello 🌍")],
        vec![vec![
            StreamDelta::ReasoningDelta("thought".into()),
            StreamDelta::Completed,
        ]],
        vec![
            {
                let mut script = call_script(ToolCallId::from("a"), r#"{"text":"one"}"#);
                script.pop();
                script.extend([
                    StreamDelta::ToolCallStart {
                        call_id: ToolCallId::from("b"),
                        name: "echo".into(),
                    },
                    StreamDelta::ToolCallArgsDelta {
                        call_id: ToolCallId::from("b"),
                        text: r#"{"text":"two"}"#.into(),
                    },
                    // No explicit done: retain the runtime's raw string fallback.
                    StreamDelta::Completed,
                ]);
                script
            },
            text_script("after"),
        ],
    ] {
        let store = Arc::new(MemoryStore::new());
        let capture = Arc::new(Capture::default());
        let builder = runtime(
            store.clone(),
            FakeProvider::scripted(scripts),
            capture.clone(),
        );
        let runtime = builder
            .execution_mode(ExecutionMode::Parallel { max: 2 })
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        handle.done().await.unwrap();
        let records = capture.records.lock().unwrap().clone();
        assert_boundaries(&records);
        let messages = store.list_messages(&session, None).await.unwrap();
        for message in messages.iter().filter(|m| m.role == Role::Assistant) {
            let committed = records
                .iter()
                .find(|e| {
                    e.kind == EventKind::MessageCommitted
                        && e.payload["message_id"] == json!(message.id)
                })
                .unwrap();
            let text: String = records
                .iter()
                .filter(|e| {
                    e.kind == EventKind::TextDelta && e.payload["message_id"] == json!(message.id)
                })
                .map(|e| e.payload["text"].as_str().unwrap())
                .collect();
            let stored: String = message
                .parts
                .iter()
                .filter_map(|p| match &p.content {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(text, stored);
            assert!(
                records
                    .iter()
                    .filter(|e| e.payload["message_id"] == json!(message.id))
                    .all(|e| e.turn_id == committed.turn_id)
            );
        }
        for message in messages.iter().filter(|m| m.role == Role::Tool) {
            let settled = records
                .iter()
                .find(|e| {
                    e.kind == EventKind::ToolCallSettled
                        && e.payload["message_id"] == json!(message.id)
                })
                .unwrap();
            let ContentBlock::ToolResult {
                content, is_error, ..
            } = &message.parts[0].content
            else {
                panic!("tool result")
            };
            assert_eq!(settled.payload["content"], json!(content));
            assert_eq!(settled.payload["is_error"], json!(is_error));
            assert!(settled.turn_id.is_some());
        }
        assert!(
            store
                .list_events(&session, None, 100)
                .await
                .unwrap()
                .iter()
                .all(|e| !e.kind.is_live_only())
        );
    }
}

#[tokio::test]
async fn invalid_call_boundaries_fail_and_close_without_dispatch() {
    let id = ToolCallId::from("same");
    let start = StreamDelta::ToolCallStart {
        call_id: id.clone(),
        name: "echo".into(),
    };
    for script in [
        vec![start.clone(), start.clone()],
        vec![StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: "{}".into(),
        }],
        vec![StreamDelta::ToolCallDone {
            call_id: id.clone(),
        }],
        vec![
            start.clone(),
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
        ],
        vec![
            start,
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id,
                text: "{}".into(),
            },
        ],
    ] {
        let store = Arc::new(MemoryStore::new());
        let capture = Arc::new(Capture::default());
        let runtime = runtime(
            store.clone(),
            FakeProvider::scripted(vec![script]),
            capture.clone(),
        )
        .build()
        .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        assert!(handle.done().await.is_err());
        let records = capture.records.lock().unwrap().clone();
        assert_boundaries(&records);
        assert!(!records.iter().any(|e| matches!(
            e.kind,
            EventKind::MessageCommitted | EventKind::ToolCallPending
        )));
        let end: MessageEnded = serde_json::from_value(
            records
                .iter()
                .find(|e| e.kind == EventKind::MessageStreamEnded)
                .unwrap()
                .payload
                .clone(),
        )
        .unwrap();
        assert_eq!(end.outcome, StreamOutcome::Failed);
        assert_eq!(store.list_messages(&session, None).await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn retry_keeps_turn_but_allocates_distinct_attempt_message_ids() {
    for kind in [
        crabber_providers::ProviderErrorKind::Server,
        crabber_providers::ProviderErrorKind::ContextOverflow,
    ] {
        let store = Arc::new(MemoryStore::new());
        let capture = Arc::new(Capture::default());
        let mut scripts = vec![vec![
            StreamDelta::TextDelta("uncommitted".into()),
            StreamDelta::Error(ProviderError {
                kind: kind.clone(),
                message: "SECRET".into(),
                retryable: true,
            }),
        ]];
        if kind == crabber_providers::ProviderErrorKind::ContextOverflow {
            scripts.push(text_script("summary"));
        }
        scripts.push(text_script("committed"));
        let runtime = runtime(
            store.clone(),
            FakeProvider::scripted(scripts),
            capture.clone(),
        )
        .build()
        .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        handle.done().await.unwrap();
        let records = capture.records.lock().unwrap().clone();
        assert_boundaries(&records);
        let starts: Vec<_> = records
            .iter()
            .filter(|e| e.kind == EventKind::MessageStarted)
            .collect();
        assert_eq!(starts.len(), 2);
        assert_eq!(starts[0].turn_id, starts[1].turn_id);
        assert_ne!(starts[0].payload, starts[1].payload);
        assert!(!contains_text(
            &store.list_messages(&session, None).await.unwrap(),
            "uncommitted"
        ));
        assert!(!serde_json::to_string(&*records).unwrap().contains("SECRET"));
    }
}

struct ArgumentStream;
#[async_trait]
impl ModelStream for ArgumentStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let _ = next.stream(request).await?;
        Ok(Box::pin(
            futures::stream::iter(vec![
                StreamDelta::TextDelta("partial".into()),
                StreamDelta::ToolCallStart {
                    call_id: ToolCallId::from("call"),
                    name: "echo".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: ToolCallId::from("call"),
                    text: "{\"text\":".into(),
                },
            ])
            .chain(futures::stream::pending()),
        ))
    }
}

#[tokio::test]
async fn cancellation_closes_arguments_and_preserves_partial_identity() {
    let store = Arc::new(MemoryStore::new());
    let capture = Arc::new(Capture::default());
    let builder = runtime(
        store.clone(),
        FakeProvider::scripted(vec![text_script("unused")]),
        capture.clone(),
    );
    let runtime = builder
        .model_stream(Arc::new(ArgumentStream))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    loop {
        capture.delta.notified().await;
        if capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.kind == EventKind::ToolCallArgsDelta)
        {
            break;
        }
    }
    handle.interrupt();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
    let records = capture.records.lock().unwrap().clone();
    assert_boundaries(&records);
    let start = records
        .iter()
        .find(|e| e.kind == EventKind::MessageStarted)
        .unwrap();
    let stored = store.list_messages(&session, None).await.unwrap();
    assert_eq!(start.payload["message_id"], json!(stored[1].id));
    assert!(contains_text(&stored, "partial"));
    assert!(!records.iter().any(|e| e.kind == EventKind::ToolCallPending));
}

#[tokio::test]
async fn store_fault_does_not_publish_unpersisted_tool_content() {
    let inner = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.fail_settle.store(true, Ordering::SeqCst);
    let capture = Arc::new(Capture::default());
    let runtime = runtime(
        Arc::new(DelayedTerminalStore {
            inner: inner.clone(),
            gate,
        }),
        FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"ok"}"#)]),
        capture.clone(),
    )
    .build()
    .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert!(handle.done().await.is_err());
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.kind == EventKind::ToolCallSettled)
    );
    assert!(
        inner
            .list_messages(&session, None)
            .await
            .unwrap()
            .iter()
            .all(|m| m.role != Role::Tool)
    );
}

#[tokio::test(start_paused = true)]
async fn midstream_lease_loss_drops_future_with_presentation_cleanup_only() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let capture = Arc::new(Capture::default());
    let runtime = runtime(
        store.clone(),
        FakeProvider::scripted(vec![text_script("unused")]),
        capture.clone(),
    )
    .model_stream(Arc::new(ArgumentStream))
    .clock(wall.clone())
    .heartbeat_interval(std::time::Duration::from_secs(1))
    .build()
    .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    capture.delta.notified().await;
    wall.set(now + time::Duration::seconds(31));
    store.claim_expired_run(&run, "replacement").await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert!(matches!(handle.done().await, Err(RuntimeError::LeaseLost)));
    let records = capture.records.lock().unwrap().clone();
    assert_boundaries(&records);
    assert!(!records.iter().any(|e| matches!(
        e.kind,
        EventKind::MessageCommitted | EventKind::ToolCallSettled | EventKind::RunSettled
    )));
    let ended: MessageEnded = serde_json::from_value(
        records
            .iter()
            .find(|e| e.kind == EventKind::MessageStreamEnded)
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    assert_eq!(ended.outcome, StreamOutcome::Failed);
}
