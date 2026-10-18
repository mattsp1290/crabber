//! Actual public event/history and input-routing outcomes of Runtime work.
use super::*;
#[path = "public_state/aggregate.rs"]
mod aggregate;

#[tokio::test]
async fn interrupt_closes_partial_text_message() {
    let store = Arc::new(MemoryStore::new());
    let (sender, receiver) = oneshot::channel();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script(
            "unused",
        )])))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .model_stream(Arc::new(PartialModelStream(Arc::new(Notify::new()))))
        .observer(Arc::new(DeltaObserver(Mutex::new(Some(sender)))))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    receiver.await.unwrap();
    handle.interrupt();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
    assert!(contains_text(
        &store.list_messages(&session, None).await.unwrap(),
        "partial"
    ));
}

#[tokio::test]
async fn text_tool_text_has_exact_order_and_three_generated_messages() {
    let store = Arc::new(MemoryStore::new());
    let observer = Arc::new(RecordingObserver::default());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("after"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::clone(&executed))),
        Arc::clone(&observer),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let result = handle.done().await.unwrap();
    assert_eq!(result.status, crabber_core::RunStatus::Completed);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    let messages = store.list_messages(&session, None).await.unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role != Role::User)
            .count(),
        3
    );
    assert!(contains_text(&messages, "before"));
    assert!(contains_text(&messages, "after"));
    assert_eq!(fake.requests().len(), 2);
    assert_eq!(
        observer.0.lock().unwrap().as_slice(),
        &[
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::MessageStarted,
            EventKind::TextDelta,
            EventKind::ToolCallStarted,
            EventKind::ToolCallArgsDelta,
            EventKind::ToolCallArgsCompleted,
            EventKind::MessageStreamEnded,
            EventKind::MessageCommitted,
            EventKind::ToolCallPending,
            EventKind::ToolCallRunning,
            EventKind::ToolCallSettled,
            EventKind::TurnCompleted,
            EventKind::TurnStarted,
            EventKind::MessageStarted,
            EventKind::TextDelta,
            EventKind::MessageStreamEnded,
            EventKind::MessageCommitted,
            EventKind::TurnCompleted,
            EventKind::RunSettled,
        ]
    );
    let durable = store.list_events(&session, None, 100).await.unwrap();
    assert_eq!(
        durable
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            EventKind::ExecutionAttemptChanged,
            EventKind::ExecutionAttemptChanged,
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::MessageCommitted,
            EventKind::ToolCallPending,
            EventKind::ToolPhaseChanged,
            EventKind::ToolCallRunning,
            EventKind::ToolPhaseChanged,
            EventKind::ToolCallSettled,
            EventKind::ToolPhaseChanged,
            EventKind::TurnCompleted,
            EventKind::TurnStarted,
            EventKind::MessageCommitted,
            EventKind::TurnCompleted,
            EventKind::RunSettled,
            EventKind::ExecutionAttemptChanged,
        ]
    );
    assert!(durable.iter().all(|event| !event.live_only));
}

#[tokio::test]
async fn steering_and_follow_up_reach_later_requests_and_busy_is_rejected() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("second"),
        text_script("third"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let gate = Arc::new(GatedTool {
        entered: Mutex::new(Some(entered_tx)),
        release: Arc::clone(&release),
    });
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        gate,
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let mut busy = request();
    busy.session_id = Some(handle.session_id().clone());
    let busy_result = runtime.start(busy).await;
    assert!(
        matches!(busy_result, Err(RuntimeError::SessionBusy)),
        "{:?}",
        busy_result.as_ref().err()
    );
    handle.steer("steer now").await.unwrap();
    handle.follow_up("follow later").await.unwrap();
    release.notify_one();
    handle.done().await.unwrap();
    let requests = fake.requests();
    assert_eq!(requests.len(), 3);
    assert!(contains_text(&requests[1].messages, "steer now"));
    assert!(!contains_text(&requests[1].messages, "follow later"));
    assert!(contains_text(&requests[2].messages, "follow later"));
}

#[tokio::test]
async fn stream_eof_without_completed_fails_without_assistant_commit() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![vec![StreamDelta::TextDelta("partial".into())]]);
    let runtime = orchestrator(
        Arc::clone(&store),
        fake,
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Provider(_))
    ));
    let messages = store.list_messages(&session, None).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, Role::User);
    assert_eq!(
        store.list_unfinished_runs().await.unwrap(),
        [] as [crabber_core::Run; 0]
    );
    assert_eq!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            EventKind::ExecutionAttemptChanged,
            EventKind::ExecutionAttemptChanged,
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::RunSettled,
            EventKind::ExecutionAttemptChanged,
        ]
    );
}

#[path = "public_state/source_writer.rs"]
mod source_writer;

#[path = "public_state/source_nested.rs"]
mod source_nested;
