use crate::{
    ApprovalRequester, ExecutionMode, InterruptPolicy, ModelStream, Observer, Orchestrator,
    PermissionDecision, PermissionPolicy, Request, RuntimeError, StaticPolicy, ToolPipeline,
};
use async_trait::async_trait;
use crabber_core::{
    ContentBlock, ContextEpoch, EpochId, EventCursor, EventKind, EventRecord, ManualClock, Message,
    Part, Role, Run, RunFence, RunId, RunStatus, Session, SessionId, ToolCallId, ToolCallRecord,
    ToolResult, Usage,
};
use crabber_extension::{
    ExtensionError, RunPlanProvider, StaticPlanProvider, ToolDefinition, ToolExecutor,
};
use crabber_providers::{
    DeltaStream, FakeProvider, ModelRequest, ProviderError, Resolver, Selection, StreamDelta,
    Streamer,
};
use crabber_session::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, MemoryStore, Store, StoreError,
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify, oneshot};

#[derive(Default)]
struct RecordingObserver(Mutex<Vec<EventKind>>);

impl Observer for RecordingObserver {
    fn emit(&self, event: &crabber_core::EventRecord) {
        self.0
            .lock()
            .expect("observer poisoned")
            .push(event.kind.clone());
    }
}

struct PausePolicy;
impl PermissionPolicy for PausePolicy {
    fn decide(&self, _tool: &crabber_core::ToolInfo, _arguments: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(
        &self,
        _tool: &crabber_core::ToolInfo,
        _arguments: &Value,
    ) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}

struct CountingPipeline(Arc<AtomicUsize>);
#[async_trait]
impl ToolPipeline for CountingPipeline {
    async fn prepare(
        &self,
        _tool: &crabber_core::ToolInfo,
        arguments: Value,
    ) -> Result<Value, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        _tool: &crabber_core::ToolInfo,
        result: Value,
    ) -> Result<Value, String> {
        Ok(result)
    }
}

struct PartialModelStream(Arc<Notify>);
#[async_trait]
impl ModelStream for PartialModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let _ = next.stream(request).await?;
        let release = Arc::clone(&self.0);
        Ok(Box::pin(
            futures::stream::once(async { StreamDelta::TextDelta("partial".into()) }).chain(
                futures::stream::once(async move {
                    release.notified().await;
                    StreamDelta::Completed
                }),
            ),
        ))
    }
}

struct DeltaObserver(Mutex<Option<oneshot::Sender<()>>>);
impl Observer for DeltaObserver {
    fn emit(&self, event: &EventRecord) {
        if event.kind == EventKind::TextDelta
            && let Some(sender) = self.0.lock().unwrap().take()
        {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn interrupt_closes_partial_text_message() {
    let store = Arc::new(MemoryStore::new());
    let (sender, receiver) = oneshot::channel();
    let runtime = Orchestrator::builder()
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
async fn interrupt_settles_running_tool_and_run() {
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
    let result = handle.done().await.unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
}

#[tokio::test]
async fn paused_call_resumes_from_persisted_input() {
    let store = Arc::new(MemoryStore::new());
    let executed = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
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
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
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
async fn paused_with_unstaged_assistant(
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
async fn recovery_does_not_request_model_after_committed_text_response() {
    let (runtime, store, fake, executed, run_id) =
        paused_with_unstaged_assistant(false, false).await;
    let result = runtime.resume(&run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(fake.requests().len(), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recovery_stages_committed_assistant_call_before_next_model_turn() {
    let (runtime, store, fake, executed, run_id) =
        paused_with_unstaged_assistant(true, false).await;
    let result = runtime.resume(&run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(fake.requests().len(), 2);
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recovery_reconciles_text_after_compaction_hides_paused_anchor() {
    let (runtime, _, fake, executed, run_id) = paused_with_unstaged_assistant(false, true).await;
    let result = runtime.resume(&run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(fake.requests().len(), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovery_stages_tool_after_compaction_hides_paused_anchor() {
    let (runtime, store, fake, executed, run_id) = paused_with_unstaged_assistant(true, true).await;
    let result = runtime.resume(&run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(fake.requests().len(), 2);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn overflow_compacts_once_and_retries_open_turn() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        vec![StreamDelta::Error(ProviderError {
            kind: crabber_providers::ProviderErrorKind::ContextOverflow,
            message: "full".into(),
            retryable: false,
        })],
        text_script("summary"),
        text_script("after"),
    ]);
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(fake.requests().len(), 3);
    assert!(
        store
            .list_messages(&session, None)
            .await
            .unwrap()
            .iter()
            .any(|message| message
                .parts
                .iter()
                .any(|part| part.kind == crabber_core::PartKind::CompactionSummary))
    );
}

#[tokio::test]
async fn overflow_keeps_recent_tail_in_projected_retry() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        vec![StreamDelta::Error(ProviderError {
            kind: crabber_providers::ProviderErrorKind::ContextOverflow,
            message: "full".into(),
            retryable: false,
        })],
        text_script("summary"),
        text_script("after"),
    ]);
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .compaction(crate::CompactionPolicy {
            trigger_ratio: 0.85,
            keep_tail_messages: 2,
        })
        .build()
        .unwrap();
    assert_eq!(
        runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    let requests = fake.requests();
    assert_eq!(requests.len(), 4);
    let original = &requests[1].messages;
    let retry = &requests[3].messages;
    assert_eq!(retry.len(), 3);
    assert_eq!(retry[1].id, original[original.len() - 2].id);
    assert_eq!(retry[2].id, original[original.len() - 1].id);
    assert_eq!(
        retry[0].parts[0].kind,
        crabber_core::PartKind::CompactionSummary
    );
}

#[derive(Clone)]
struct LimitedProvider {
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    limit: usize,
    summary_entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    block_summary: bool,
    block_acquisition: bool,
}

#[async_trait]
impl Resolver for LimitedProvider {
    async fn resolve(&self, _selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl Streamer for LimitedProvider {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let summary = request
            .system
            .as_deref()
            .is_some_and(|system| system.starts_with("Summarize"));
        let bytes = serde_json::to_vec(&request.messages).unwrap().len();
        self.requests.lock().unwrap().push(request);
        if summary {
            if let Some(sender) = self.summary_entered.lock().unwrap().take() {
                let _ = sender.send(());
            }
            if self.block_acquisition {
                return futures::future::pending().await;
            }
            if self.block_summary {
                return Ok(Box::pin(futures::stream::pending()));
            }
            if bytes > self.limit {
                return Ok(Box::pin(futures::stream::iter(vec![StreamDelta::Error(
                    ProviderError {
                        kind: crabber_providers::ProviderErrorKind::ContextOverflow,
                        message: "summary exceeded actual limit".into(),
                        retryable: false,
                    },
                )])));
            }
            let carries_sentinel = self.requests.lock().unwrap().last().is_some_and(|request| {
                serde_json::to_string(&request.messages)
                    .unwrap()
                    .contains("EARLY_STANDING_INSTRUCTION")
            });
            return Ok(Box::pin(futures::stream::iter(text_script(
                if carries_sentinel {
                    "EARLY_STANDING_INSTRUCTION small summary"
                } else {
                    "small summary"
                },
            ))));
        }
        if bytes > self.limit {
            return Ok(Box::pin(futures::stream::iter(vec![StreamDelta::Error(
                ProviderError {
                    kind: crabber_providers::ProviderErrorKind::ContextOverflow,
                    message: "actual limit".into(),
                    retryable: false,
                },
            )])));
        }
        Ok(Box::pin(futures::stream::iter(text_script("done"))))
    }
}

#[tokio::test]
async fn real_size_limit_overflow_summarizes_bounded_input() {
    let provider = LimitedProvider {
        requests: Arc::new(Mutex::new(Vec::new())),
        limit: 3000,
        summary_entered: Arc::new(Mutex::new(None)),
        block_summary: false,
        block_acquisition: false,
    };
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .build()
        .unwrap();
    let mut request = request();
    request.text = format!("EARLY_STANDING_INSTRUCTION {}", "large input ".repeat(1000));
    let handle = runtime.start(request).await.unwrap();
    let session_id = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    {
        let requests = provider.requests.lock().unwrap();
        assert!(requests.len() > 3);
        assert!(serde_json::to_vec(&requests[0].messages).unwrap().len() > provider.limit);
        assert!(
            requests[1..]
                .iter()
                .all(
                    |request| serde_json::to_vec(&request.messages).unwrap().len() < provider.limit
                )
        );
    }
    let projected = store.list_messages(&session_id, None).await.unwrap();
    assert!(
        serde_json::to_string(&projected)
            .unwrap()
            .contains("EARLY_STANDING_INSTRUCTION")
    );
}

#[tokio::test]
async fn interrupt_cancels_blocked_compaction_summary() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let provider = LimitedProvider {
        requests: Arc::new(Mutex::new(Vec::new())),
        limit: 3000,
        summary_entered: Arc::new(Mutex::new(Some(entered_tx))),
        block_summary: true,
        block_acquisition: false,
    };
    let runtime = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(provider))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .build()
        .unwrap();
    let mut request = request();
    request.text = "large input ".repeat(1000);
    let handle = runtime.start(request).await.unwrap();
    entered_rx.await.unwrap();
    handle.interrupt();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
}

#[tokio::test]
async fn interrupt_cancels_summary_stream_acquisition() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let provider = LimitedProvider {
        requests: Arc::new(Mutex::new(Vec::new())),
        limit: 3000,
        summary_entered: Arc::new(Mutex::new(Some(entered_tx))),
        block_summary: false,
        block_acquisition: true,
    };
    let runtime = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(provider))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .build()
        .unwrap();
    let mut request = request();
    request.text = "large input ".repeat(1000);
    let handle = runtime.start(request).await.unwrap();
    entered_rx.await.unwrap();
    handle.interrupt();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
}

#[tokio::test]
async fn retryable_provider_error_retries_same_turn() {
    let fake = FakeProvider::scripted(vec![
        vec![StreamDelta::Error(ProviderError {
            kind: crabber_providers::ProviderErrorKind::Server,
            message: "temporary".into(),
            retryable: true,
        })],
        text_script("done"),
    ]);
    let runtime = orchestrator(
        Arc::new(MemoryStore::new()),
        fake.clone(),
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    assert_eq!(
        runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(fake.requests().len(), 2);
}

#[tokio::test]
async fn second_overflow_fails_after_one_epoch() {
    let store = Arc::new(MemoryStore::new());
    let overflow = || {
        StreamDelta::Error(ProviderError {
            kind: crabber_providers::ProviderErrorKind::ContextOverflow,
            message: "full".into(),
            retryable: false,
        })
    };
    let fake = FakeProvider::scripted(vec![
        vec![overflow()],
        text_script("summary"),
        vec![overflow()],
    ]);
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Provider(_))
    ));
    assert_eq!(fake.requests().len(), 3);
    assert_eq!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.kind == EventKind::ContextEpochStarted)
            .count(),
        1
    );
}

struct BarrierTool(Arc<Barrier>);
#[async_trait]
impl ToolExecutor for BarrierTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0.wait().await;
        Ok(arguments)
    }
}

#[tokio::test]
async fn parallel_tools_execute_together_and_settle_in_assistant_order() {
    let store = Arc::new(MemoryStore::new());
    let ids: Vec<_> = (0..3).map(|_| ToolCallId::new()).collect();
    let mut script = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        script.push(StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: "echo".into(),
        });
        script.push(StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: format!(r#"{{"text":"{index}"}}"#),
        });
        script.push(StreamDelta::ToolCallDone {
            call_id: id.clone(),
        });
    }
    script.push(StreamDelta::Completed);
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(vec![
            script,
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(BarrierTool(Arc::new(Barrier::new(3)))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 3 })
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let settled: Vec<_> = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallSettled)
        .map(|event| event.correlation.unwrap())
        .collect();
    assert_eq!(
        settled,
        ids.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
}

async fn crashed_run_with_calls(
    running: bool,
) -> (Orchestrator, Arc<MemoryStore>, Arc<AtomicUsize>, RunId) {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let executed = Arc::new(AtomicUsize::new(0));
    let plan_provider = Arc::new(StaticPlanProvider::new(
        vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
        Vec::new(),
    ));
    let session = SessionId::new();
    let plan = plan_provider.acquire_plan(&session).await.unwrap();
    let fingerprint = plan.fingerprint().to_string();
    plan.release();
    let message_id = crabber_core::MessageId::new();
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            workspace_id: "test".into(),
            directory: "/tmp".into(),
            title: "test".into(),
            user_message: Message {
                id: message_id.clone(),
                session_id: session,
                run_id: None,
                role: Role::User,
                parent_id: None,
                created_at: now,
                parts: vec![Part {
                    id: crabber_core::PartId::new(),
                    message_id,
                    ordinal: 0,
                    kind: crabber_core::PartKind::UserInputText,
                    content: ContentBlock::Text {
                        text: "hello".into(),
                    },
                }],
            },
            config_hash: "test".into(),
            plan_fingerprint: fingerprint,
            owner: "crashed".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    for safe in [true, false] {
        let id = ToolCallId::new();
        let event = EventRecord {
            cursor: None,
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            turn_id: None,
            kind: EventKind::ToolCallPending,
            payload: Value::Null,
            correlation: None,
            live_only: false,
            created_at: now,
        };
        execution
            .create_tool_call(
                ToolCallRecord {
                    id: id.clone(),
                    run_id: admitted.run.id.clone(),
                    name: "echo".into(),
                    arguments: json!({"text":"ok"}),
                    status: crabber_core::ToolCallStatus::Pending,
                    retry_safe: safe,
                    result: None,
                },
                event,
            )
            .await
            .unwrap();
        if running && safe {
            let event = EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallRunning,
                payload: Value::Null,
                correlation: None,
                live_only: false,
                created_at: now,
            };
            execution.claim_tool_call(&id, event).await.unwrap();
        }
    }
    clock.set(now + time::Duration::seconds(31));
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(Vec::new())))
        .plan_provider(plan_provider)
        .clock(clock)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    (runtime, store, executed, admitted.run.id)
}

#[tokio::test]
async fn resume_reexecutes_only_retry_safe_pending_call() {
    let (runtime, store, executed, run_id) = crashed_run_with_calls(false).await;
    let result = runtime.resume(&run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recover_interrupts_expired_running_call() {
    let (runtime, store, executed, run_id) = crashed_run_with_calls(true).await;
    let result = runtime.recover().await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].status, RunStatus::Interrupted);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

struct EchoTool(Arc<AtomicUsize>);

#[async_trait]
impl ToolExecutor for EchoTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}

struct GatedTool {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

struct CancellableTool {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl ToolExecutor for CancellableTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("tool gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}

struct BlockingSettledObserver {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl Observer for BlockingSettledObserver {
    fn emit(&self, event: &crabber_core::EventRecord) {
        if event.kind == EventKind::RunSettled {
            if let Some(sender) = self.entered.lock().expect("observer gate poisoned").take() {
                let _ = sender.send(());
            }
            let (lock, ready) = &*self.released;
            let mut released = lock.lock().expect("observer gate poisoned");
            while !*released {
                released = ready.wait(released).expect("observer gate poisoned");
            }
        }
    }
}

struct GatedModelStream {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl ModelStream for GatedModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let entered = self.entered.lock().expect("model gate poisoned").take();
        if let Some(sender) = entered {
            let _ = sender.send(());
            self.release.notified().await;
        }
        next.stream(request).await
    }
}

struct GatedApprover {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl ApprovalRequester for GatedApprover {
    async fn approve(&self, _tool: &crabber_core::ToolInfo, _arguments: &Value) -> bool {
        if let Some(sender) = self.entered.lock().expect("approval gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        true
    }
}

#[async_trait]
impl ToolExecutor for GatedTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        Ok(arguments)
    }
}

fn tool(executor: Arc<dyn ToolExecutor>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: crabber_core::ToolInfo {
            name: "echo".into(),
            description: "Echo input".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: Vec::new(),
        },
        executor,
    })
}

fn request() -> Request {
    Request {
        session_id: None,
        workspace_id: "test".into(),
        directory: "/tmp".into(),
        title: "test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
    }
}

fn orchestrator(
    store: Arc<MemoryStore>,
    fake: FakeProvider,
    executor: Arc<dyn ToolExecutor>,
    observer: Arc<RecordingObserver>,
) -> Orchestrator {
    Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(executor)],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(observer)
        .build()
        .unwrap()
}

fn call_script(call_id: ToolCallId, arguments: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("before".into()),
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "echo".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: arguments.into(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

fn text_script(text: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(text.into()), StreamDelta::Completed]
}

fn contains_text(messages: &[Message], text: &str) -> bool {
    messages
        .iter()
        .flat_map(|message| &message.parts)
        .any(|part| matches!(&part.content, ContentBlock::Text { text: actual } if actual == text))
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
            EventKind::TextDelta,
            EventKind::MessageCommitted,
            EventKind::ToolCallPending,
            EventKind::ToolCallRunning,
            EventKind::ToolCallSettled,
            EventKind::TurnCompleted,
            EventKind::TurnStarted,
            EventKind::TextDelta,
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
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::MessageCommitted,
            EventKind::ToolCallPending,
            EventKind::ToolCallRunning,
            EventKind::ToolCallSettled,
            EventKind::TurnCompleted,
            EventKind::TurnStarted,
            EventKind::MessageCommitted,
            EventKind::TurnCompleted,
            EventKind::RunSettled,
        ]
    );
    assert!(durable.iter().all(|event| !event.live_only));
}

#[tokio::test]
async fn invalid_tool_arguments_settle_failed_and_continue() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), "{}"),
        text_script("recovered"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::clone(&executed))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 2);
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(contains_text(&messages, "recovered"));
    assert!(
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .any(|part| matches!(
                &part.content,
                ContentBlock::ToolResult { is_error: true, .. }
            ))
    );
    assert!(
        store
            .list_unfinished_tool_calls(&fake.requests()[0].identity.run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn ask_policy_uses_default_deny_approver() {
    let store = Arc::new(MemoryStore::new());
    let observer = Arc::new(RecordingObserver::default());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("after denial"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .observer(Arc::clone(&observer) as Arc<dyn Observer>)
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 2);
    assert!(
        observer
            .0
            .lock()
            .unwrap()
            .contains(&EventKind::PermissionRequested)
    );
    assert!(
        observer
            .0
            .lock()
            .unwrap()
            .contains(&EventKind::PermissionDecided)
    );
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .any(|part| matches!(
                &part.content,
                ContentBlock::ToolResult { is_error: true, .. }
            ))
    );
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
    assert!(matches!(
        runtime.start(busy).await,
        Err(RuntimeError::SessionBusy)
    ));
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
async fn enum_and_nested_schema_failures_never_execute_tool() {
    for arguments in [
        r#"{"choice":"forbidden","nested":{"flag":true}}"#,
        r#"{"choice":"allowed","nested":{}}"#,
    ] {
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), arguments),
            text_script("continued"),
        ]);
        let executed = Arc::new(AtomicUsize::new(0));
        let mut definition = tool(Arc::new(EchoTool(Arc::clone(&executed))));
        Arc::get_mut(&mut definition).unwrap().info.parameters = json!({
            "type":"object", "required":["choice","nested"],
            "properties":{
                "choice":{"enum":["allowed"]},
                "nested":{"type":"object","required":["flag"],"properties":{"flag":{"type":"boolean"}}}
            }
        });
        let runtime = Orchestrator::builder()
            .store(Arc::clone(&store) as Arc<dyn Store>)
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![definition],
                Vec::new(),
            )))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        handle.done().await.unwrap();
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(fake.requests().len(), 2);
        let messages = store.list_messages(&session, None).await.unwrap();
        assert!(
            messages
                .iter()
                .flat_map(|message| &message.parts)
                .any(|part| matches!(
                    &part.content,
                    ContentBlock::ToolResult { is_error: true, .. }
                ))
        );
    }
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
    assert!(store.list_unfinished_runs().await.unwrap().is_empty());
    assert_eq!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::RunSettled,
        ]
    );
}

#[tokio::test]
async fn heartbeat_keeps_gated_tool_run_owned_past_initial_lease() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("finished"),
        text_script("another run"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(GatedTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
            }))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(25));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store.list_unfinished_runs().await.unwrap()[0].lease_until
                > now + time::Duration::seconds(50)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    clock.set(now + time::Duration::seconds(35));
    release.notify_one();
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
    let mut next = request();
    next.session_id = Some(session);
    assert_eq!(
        runtime
            .start(next)
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        crabber_core::RunStatus::Completed
    );
}

#[tokio::test]
async fn heartbeat_covers_model_stream_and_approval_waits() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("done"),
    ]);
    let (model_tx, model_rx) = oneshot::channel();
    let (approval_tx, approval_rx) = oneshot::channel();
    let model_release = Arc::new(Notify::new());
    let approval_release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .model_stream(Arc::new(GatedModelStream {
            entered: Mutex::new(Some(model_tx)),
            release: Arc::clone(&model_release),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .approver(Arc::new(GatedApprover {
            entered: Mutex::new(Some(approval_tx)),
            release: Arc::clone(&approval_release),
        }))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), model_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(25));
    wait_for_lease(&store, now + time::Duration::seconds(50)).await;
    model_release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), approval_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(50));
    wait_for_lease(&store, now + time::Duration::seconds(75)).await;
    approval_release.notify_one();
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
}

async fn wait_for_lease(store: &MemoryStore, threshold: time::OffsetDateTime) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store.list_unfinished_runs().await.unwrap()[0].lease_until > threshold {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn heartbeat_interval_requires_margin_inside_lease() {
    for interval in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(15),
        std::time::Duration::from_secs(30),
    ] {
        assert!(matches!(
            Orchestrator::builder().heartbeat_interval(interval).build(),
            Err(RuntimeError::InvalidConfiguration(_))
        ));
    }
}

#[tokio::test]
async fn reclaimed_lease_cancels_pending_approval_before_tool_body() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let (approval_tx, approval_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .approver(Arc::new(GatedApprover {
            entered: Mutex::new(Some(approval_tx)),
            release: Arc::clone(&release),
        }))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), approval_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
        .await
        .unwrap();
    assert!(matches!(result, Err(RuntimeError::LeaseLost)));
    release.notify_one();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::PermissionRequested));
    assert!(!kinds.contains(&EventKind::PermissionDecided));
    assert!(!kinds.contains(&EventKind::ToolCallSettled));
    assert!(!kinds.contains(&EventKind::RunSettled));
    let unfinished = store.list_unfinished_runs().await.unwrap();
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].owner, "replacement owner");
    assert_eq!(unfinished[0].claim_token, new_fence.claim_token);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_settlement_wins_heartbeat_tick_during_observer_callback() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let observer = Arc::new(BlockingSettledObserver {
        entered: Mutex::new(Some(entered_tx)),
        released: Arc::clone(&released),
    });
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(observer)
        .heartbeat_interval(std::time::Duration::from_millis(5))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        crabber_core::RunStatus::Completed
    );
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    {
        let (lock, ready) = &*released;
        *lock.lock().unwrap() = true;
        ready.notify_all();
    }
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber_core::RunStatus::Completed
    );
}

#[tokio::test]
async fn reclaimed_lease_cancels_pending_tool_body() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(CancellableTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            }))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap(),
        Err(RuntimeError::LeaseLost)
    ));
    release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(!kinds.contains(&EventKind::ToolCallSettled));
    assert!(!kinds.contains(&EventKind::RunSettled));
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().claim_token,
        new_fence.claim_token
    );
}

struct SettlementGate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Notify,
    terminal_reads: AtomicUsize,
}

struct DelayedTerminalStore {
    inner: Arc<MemoryStore>,
    gate: Arc<SettlementGate>,
}

struct DelayedTerminalExecution {
    inner: Box<dyn ExecutionStore>,
    gate: Arc<SettlementGate>,
}

#[async_trait]
impl Store for DelayedTerminalStore {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        request: crabber_session::KeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        self.inner.admit_keyed_run(request).await
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &crabber_core::AdmissionKey,
    ) -> Result<Option<crabber_core::AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        Ok(Box::new(DelayedTerminalExecution {
            inner: self.inner.execution(fence).await?,
            gate: Arc::clone(&self.gate),
        }))
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        let run = self.inner.get_run(id).await?;
        if run
            .as_ref()
            .is_some_and(|run| run.status == RunStatus::Completed)
        {
            self.gate.terminal_reads.fetch_add(1, Ordering::SeqCst);
        }
        Ok(run)
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
}

#[async_trait]
impl ExecutionStore for DelayedTerminalExecution {
    async fn renew_lease(&self, until: time::OffsetDateTime) -> Result<(), StoreError> {
        self.inner.renew_lease(until).await
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        self.inner.append_message(message).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.inner.append_part(part).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        self.inner.append_event(event).await
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.create_tool_call(call, pending_event).await
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.claim_tool_call(id, running_event).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner
            .settle_tool_call(id, result, result_message, terminal_event)
            .await
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        self.inner.start_epoch(epoch).await
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        self.inner.finish_epoch(id, summary).await
    }
    async fn pause_run(&self, checkpoint: Value, event: EventRecord) -> Result<(), StoreError> {
        self.inner.pause_run(checkpoint, event).await
    }
    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let result = self.inner.settle_run(status, error, usage, event).await;
        if status == RunStatus::Completed && result.is_ok() {
            if let Some(sender) = self
                .gate
                .entered
                .lock()
                .expect("settlement gate poisoned")
                .take()
            {
                let _ = sender.send(());
            }
            self.gate.release.notified().await;
        }
        result
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        self.inner.put_extension_state(extension_id, entries).await
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox(kind).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox_into_history(kind).await
    }
}

#[tokio::test]
async fn completed_commit_survives_heartbeat_channel_closing_before_settle_returns() {
    let inner = Arc::new(MemoryStore::new());
    let (entered_tx, entered_rx) = oneshot::channel();
    let gate = Arc::new(SettlementGate {
        entered: Mutex::new(Some(entered_tx)),
        release: Notify::new(),
        terminal_reads: AtomicUsize::new(0),
    });
    let store = Arc::new(DelayedTerminalStore {
        inner: Arc::clone(&inner),
        gate: Arc::clone(&gate),
    });
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let runtime = Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .heartbeat_interval(std::time::Duration::from_millis(5))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inner.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while gate.terminal_reads.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
}
