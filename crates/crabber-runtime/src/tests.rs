use crate::{
    ExecutionMode, InterruptPolicy, ModelStream, Observer, Orchestrator, PermissionDecision,
    PermissionPolicy, Request, RuntimeError, StaticPolicy, ToolPipeline,
};
use async_trait::async_trait;
use crabber_core::{
    ContentBlock, ContextEpoch, EpochId, EventCursor, EventKind, EventRecord, ManualClock, Message,
    Part, Role, Run, RunFence, RunId, RunStatus, Session, SessionId, ToolCallId, ToolCallRecord,
    ToolResult, Usage,
};
use crabber_extension::{
    ExtensionError, JsonToolExecutor, RunPlanProvider, StaticPlanProvider, ToolDefinition,
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

mod approval_order;
mod approval_recovery;
#[path = "tests/continuations.rs"]
mod continuations;
#[path = "tests/frontend_replay.rs"]
mod frontend_replay;
mod native_phase;
#[path = "tests/preparation_resources.rs"]
mod preparation_resources;
#[path = "tests/public_state.rs"]
mod public_state;
#[path = "tests/suspension_observation.rs"]
mod suspension_observation;
#[path = "tests/tool_suspension_faults.rs"]
mod tool_suspension_faults;
mod turn_budget;
mod unstarted_batch;

#[path = "tests/native_attempt.rs"]
pub(crate) mod native_attempt;

#[path = "tests/owned_execution.rs"]
mod owned_execution;
#[path = "tests/terminal_outcome.rs"]
mod terminal_outcome;

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
        _context: &crabber_extension::ToolResultContext,
        _tool: &crabber_core::ToolInfo,
        result: crabber_core::ToolOutput,
    ) -> Result<crabber_extension::TransformOutput, String> {
        let result = result
            .json_value()
            .map_err(|_| "JSON fixture result required".to_owned())?;
        Ok(crabber_extension::TransformOutput::new(
            crabber_core::ToolOutput::from_json(&(result)).unwrap(),
        ))
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
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber_core::ProviderBindingDescriptor, crabber_providers::ProviderBindingError>
    {
        crabber_core::ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "limited-fixture",
            "1",
            crabber_core::InputFingerprint::new(crabber_session::admission_config_hash(&json!([
                self.limit,
                self.block_summary,
                self.block_acquisition
            ])))
            .unwrap(),
            crabber_core::ProviderBindingLimits::default(),
        )
        .map_err(|_| crabber_providers::ProviderBindingError::InvalidDescriptor)
    }

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
    let capture = Arc::new(OperationalCapture::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .observer(capture.clone())
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
    let values = capture.values.lock().unwrap().clone();
    let summaries: Vec<_> = values
        .iter()
        .filter(|value| {
            matches!(
                value.kind,
                crate::OperationKind::Model {
                    purpose: crate::ModelPurpose::Compaction,
                    ..
                }
            )
        })
        .collect();
    assert_ne!(summaries, [] as [&crate::OperationalObservation; 0]);
    assert!(
        summaries
            .iter()
            .all(|value| value.reason == crate::TerminalReason::Success
                && value.first_token.is_some())
    );
    assert_eq!(
        summaries.len(),
        provider
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request
                .system
                .as_deref()
                .is_some_and(|system| system.starts_with("Summarize this context")))
            .count()
    );
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
    let capture = Arc::new(OperationalCapture::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .observer(capture.clone())
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
    let values = capture.values.lock().unwrap().clone();
    let summaries: Vec<_> = values
        .iter()
        .filter(|value| {
            matches!(
                value.kind,
                crate::OperationKind::Model {
                    purpose: crate::ModelPurpose::Compaction,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].reason, crate::TerminalReason::Cancelled);
    assert_eq!(summaries[0].first_token, None);
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
    let capture = Arc::new(OperationalCapture::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .observer(capture.clone())
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
    let values = capture.values.lock().unwrap().clone();
    let summaries: Vec<_> = values
        .iter()
        .filter(|value| {
            matches!(
                value.kind,
                crate::OperationKind::Model {
                    purpose: crate::ModelPurpose::Compaction,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].reason, crate::TerminalReason::Cancelled);
    assert_eq!(summaries[0].first_token, None);
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
impl JsonToolExecutor for BarrierTool {
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
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
    crashed_run_with_observer(running, Arc::new(crate::NoopObserver)).await
}

async fn crashed_run_with_observer(
    running: bool,
    observer: Arc<dyn Observer>,
) -> (Orchestrator, Arc<MemoryStore>, Arc<AtomicUsize>, RunId) {
    crashed_run_admitted_under(running, observer, None).await
}

/// `admitted_fingerprint` replaces the plan fingerprint stored with the run.
#[allow(clippy::too_many_lines)]
async fn crashed_run_admitted_under(
    running: bool,
    observer: Arc<dyn Observer>,
    admitted_fingerprint: Option<fn(&crabber_extension::RunPlan) -> String>,
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
    let stale = admitted_fingerprint.map(|stale| stale(&plan));
    plan.release();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(FakeProvider::scripted(Vec::new())))
        .observer(observer)
        .plan_provider(plan_provider)
        .clock(clock.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let mut original = request();
    original.session_id = Some(session);
    let (plan, mut prepared, _) = runtime.prepare_admission(&original).await.unwrap();
    plan.release();
    prepared.request.session_id = None;
    if let Some(stale) = stale {
        prepared.request.plan_fingerprint.clone_from(&stale);
        prepared.binding.plan_fingerprint = crabber_core::InputFingerprint::new(stale).unwrap();
        prepared.binding.input_fingerprint = prepared.binding.computed_input_fingerprint().unwrap();
    }
    let admitted = store.admit_bound_run(prepared).await.unwrap();
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
    (runtime, store, executed, admitted.run.id)
}

/// The digest `compute_fingerprint` produced before the result-transform
/// contract component (D9): SHA-256 of the sorted component identities.
fn pre_contract_fingerprint(plan: &crabber_extension::RunPlan) -> String {
    use sha2::{Digest, Sha256};
    let mut components = plan.components.clone();
    components.sort_by(|a, b| (&a.id, &a.version).cmp(&(&b.id, &b.version)));
    crabber_extension::PlanFingerprint(
        Sha256::digest(serde_json::to_vec(&components).unwrap()).into(),
    )
    .to_string()
}

#[tokio::test]
async fn resume_refuses_run_admitted_under_pre_contract_fingerprint() {
    let (runtime, store, executed, run_id) = crashed_run_admitted_under(
        false,
        Arc::new(crate::NoopObserver),
        Some(pre_contract_fingerprint),
    )
    .await;
    let stored = store
        .get_run(&run_id)
        .await
        .unwrap()
        .unwrap()
        .plan_fingerprint;
    assert_eq!(stored.len(), 64);
    let error = runtime.resume(&run_id).await.unwrap_err();
    assert!(matches!(error, RuntimeError::PlanChanged), "{error:?}");
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn legacy_retry_safe_pending_calls_do_not_grant_native_dispatch() {
    let (runtime, store, executed, run_id) = crashed_run_with_calls(false).await;
    let before = store.list_unfinished_tool_calls(&run_id).await.unwrap();
    assert!(matches!(
        runtime.resume(&run_id).await,
        Err(RuntimeError::Attempt(
            crabber_session::AttemptError::InvalidTransition
        ))
    ));
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.list_unfinished_tool_calls(&run_id).await.unwrap(),
        before
    );
}

#[tokio::test]
async fn legacy_running_call_cannot_be_declared_interrupted_by_recovery() {
    let (runtime, store, executed, run_id) = crashed_run_with_calls(true).await;
    let before = store.list_unfinished_tool_calls(&run_id).await.unwrap();
    let report = runtime.recover().await.unwrap();
    assert_eq!(report.recovered, Vec::new());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(
        report.skipped[0].reason,
        crate::RecoverySkipReason::Attempt(crabber_session::AttemptError::InvalidTransition)
    );
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.list_unfinished_tool_calls(&run_id).await.unwrap(),
        before
    );
}

struct EchoTool(Arc<AtomicUsize>);

#[async_trait]
impl JsonToolExecutor for EchoTool {
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
impl JsonToolExecutor for CancellableTool {
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

#[async_trait]
impl JsonToolExecutor for GatedTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        Ok(arguments)
    }
}

fn tool(executor: Arc<dyn crabber_extension::ToolExecutor>) -> Arc<ToolDefinition> {
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
        selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
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
        max_output_tokens: None,
    }
}

fn orchestrator<S: Store + 'static>(
    store: Arc<S>,
    fake: FakeProvider,
    executor: Arc<dyn crabber_extension::ToolExecutor>,
    observer: Arc<RecordingObserver>,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
    assert_eq!(
        store
            .list_unfinished_tool_calls(&fake.requests()[0].identity.run_id)
            .await
            .unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
    );
}

#[tokio::test]
async fn ask_policy_waits_for_a_durable_host_decision() {
    let store = Arc::new(MemoryStore::new());
    let executed = Arc::new(AtomicUsize::new(0));
    let fake = FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"ok"}"#)]);
    let runtime = Orchestrator::builder()
        .behavior_version("ask-test.v1")
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(executed.clone())))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    assert_eq!(
        store
            .approval_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .interactions
            .len(),
        1
    );
    assert!(
        store
            .list_unfinished_tool_calls(&run)
            .await
            .unwrap()
            .iter()
            .all(|call| call.result.is_none())
    );
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
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
async fn heartbeat_covers_model_stream_and_ends_at_durable_approval() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("done"),
    ]);
    let (model_tx, model_rx) = oneshot::channel();
    let model_release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let lease = store.get_run(&run).await.unwrap().unwrap().lease_until;
    clock.set(now + time::Duration::seconds(90));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().lease_until,
        lease
    );
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .state,
        crabber_session::AttemptState::Suspended
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
            Orchestrator::builder()
                .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
                .heartbeat_interval(interval)
                .build(),
            Err(RuntimeError::InvalidConfiguration(_))
        ));
    }
}

#[tokio::test]
async fn reclaimed_lease_does_not_authorize_pending_approval() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    assert!(matches!(
        runtime.resume(&run_id).await,
        Err(RuntimeError::DurableContinuationRequired { .. })
    ));
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::RunPaused));
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
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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

#[derive(Default)]
struct SettlementGate {
    preparation_heartbeat: Option<Arc<preparation_resources::heartbeat::Gate>>,
    service_register_fault: AtomicUsize,
    service_register_calls: AtomicUsize,
    service_register_hold: std::sync::atomic::AtomicBool,
    service_register_entered: Notify,
    service_register_release: Notify,
    service_access_panic: std::sync::atomic::AtomicBool,
    service_activation_fault: AtomicUsize,
    service_activation_hold: std::sync::atomic::AtomicBool,
    service_activation_entered: Notify,
    service_activation_release: Notify,
    service_join_fault: AtomicUsize,
    service_join_panic: AtomicUsize,
    service_join_bad_termination: std::sync::atomic::AtomicBool,
    service_join_bad_binding: std::sync::atomic::AtomicBool,
    service_join_calls: AtomicUsize,
    service_join_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
    invocation_poll_panic: std::sync::atomic::AtomicBool,
    invocation_poll_entered: Notify,
    driver_register_fault: AtomicUsize,
    driver_register_request_override:
        Mutex<Option<crabber_session::session_owner::driver::RegisterExecutionDriver>>,
    driver_register_captured:
        Mutex<Option<crabber_session::session_owner::driver::ExecutionDriverOutcome>>,
    driver_register_hold: std::sync::atomic::AtomicBool,
    driver_register_entered: Notify,
    driver_register_release: Notify,
    driver_activation_fault: AtomicUsize,
    driver_join_fault: AtomicUsize,
    driver_join_calls: AtomicUsize,
    driver_poll_panic: std::sync::atomic::AtomicBool,
    driver_poll_entered: Notify,
    driver_poll_hold: std::sync::atomic::AtomicBool,
    driver_poll_release: Notify,
    driver_poll_completed: AtomicUsize,
    preparation_fault: AtomicUsize,
    unsupported_recovery_inspection: std::sync::atomic::AtomicBool,
    activation_fault: AtomicUsize,
    owned_join_fault: AtomicUsize,
    owned_join_calls: AtomicUsize,
    cleanup_uncertain_fault: AtomicUsize,
    cleanup_uncertain_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
    malformed_owned_join: AtomicUsize,
    observation: Option<Arc<suspension_observation::ObservationGate>>,
    admission: Option<Arc<owned_execution::preparation::AdmissionResponseGate>>,
    append: Option<Arc<OperationalAppendGate>>,
    settlements: AtomicUsize,
    tool_settlements: AtomicUsize,
    unfinished_reads: AtomicUsize,
    fail_claim: std::sync::atomic::AtomicBool,
    fail_settle: std::sync::atomic::AtomicBool,
    fail_permission_event: std::sync::atomic::AtomicBool,
    terminal_fault: AtomicUsize,
    phase_fault: AtomicUsize,
    tool_suspension_fault: AtomicUsize,
    tool_suspension_calls: AtomicUsize,
    tool_suspension_keys: Mutex<Vec<crabber_core::ControlId>>,
    checkpoint_override: Mutex<Option<Value>>,
    call_arguments_override: Mutex<Option<Value>>,
    begin_ack_lost: std::sync::atomic::AtomicBool,
    approval_claim_ack_lost: std::sync::atomic::AtomicBool,
    approval_recovery_ack_lost: std::sync::atomic::AtomicBool,
    approval_recovery_gate: std::sync::atomic::AtomicBool,
    approval_recovery_entered: Notify,
    approval_recovery_release: Notify,
    reserve_ack_lost: std::sync::atomic::AtomicBool,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Notify,
    terminal_reads: AtomicUsize,
    reject_run_reads: std::sync::atomic::AtomicBool,
    session_owner_ack_lost: AtomicUsize,
    session_owner_cancel_on_begin: AtomicUsize,
    owner_observe_ack_lost: std::sync::atomic::AtomicBool,
    owner_observe_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
}

struct DelayedTerminalStore {
    inner: Arc<dyn Store>,
    gate: Arc<SettlementGate>,
}

struct DelayedTerminalExecution {
    inner: Box<dyn ExecutionStore>,
    gate: Arc<SettlementGate>,
}

#[async_trait]
impl Store for DelayedTerminalStore {
    fn supports_run_controls(&self) -> bool {
        self.gate.observation.is_some()
    }
    async fn run_control_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<crabber_session::RunControlBinding, crabber_session::RunControlError> {
        self.inner.run_control_binding(session, run).await
    }
    async fn run_controls(
        &self,
        target: &crabber_session::RunControlTarget,
    ) -> Result<crabber_session::RunControlPage, crabber_session::RunControlError> {
        self.inner.run_controls(target).await
    }
    fn supports_preparation_protocol(&self) -> bool {
        true
    }
    async fn reserve_preparation(
        &self,
        request: crabber_session::session_owner::preparation::ReservePreparation,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.reserve_preparation(request).await
    }
    async fn lookup_preparation(
        &self,
        session: &crabber_core::SessionId,
        key: &crabber_session::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationReconciliation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_preparation(session, key).await
    }
    async fn complete_preparation(
        &self,
        joined: crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationCompletion,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        let preparation_fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        if preparation_fault == 1 {
            return Err(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
            );
        }
        if joined.operation_kind() == crabber_session::session_owner::preparation::PreparationOperationKind::ApprovalRecovery
            && self.gate.approval_recovery_gate.load(Ordering::SeqCst)
        {
            self.gate.approval_recovery_entered.notify_one();
            self.gate.approval_recovery_release.notified().await;
        }
        let completed = self.inner.complete_preparation(joined).await?;
        if preparation_fault == 2 {
            return Err(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
            );
        }
        if let crabber_session::session_owner::preparation::PreparationCompletion::Approval(
            crabber_session::ApprovalClaimOutcome::Applied { receipt, .. },
        ) = &completed
        {
            let lost = match receipt.outcome {
                crabber_session::ApprovalOutcome::Claimed { .. } => {
                    self.gate.approval_claim_ack_lost.load(Ordering::SeqCst)
                }
                crabber_session::ApprovalOutcome::ReclaimedUnstarted { .. } => {
                    self.gate.approval_recovery_ack_lost.load(Ordering::SeqCst)
                }
                _ => false,
            };
            if lost {
                return Err(crabber_session::ApprovalError::UnknownStoreOutcome.into());
            }
        }
        if let Some(gate) = &self.gate.admission {
            use crabber_session::session_owner::preparation::PreparationCompletion;
            match &completed {
                PreparationCompletion::AdmissionRecovery(
                    crabber_session::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(
                        claimed,
                    ),
                ) => {
                    gate.hold(&claimed.run.session_id, &claimed.run.id).await;
                }
                PreparationCompletion::Admitted(admitted) => {
                    gate.hold(&admitted.session.id, &admitted.run.id).await;
                }
                PreparationCompletion::Keyed(crabber_session::KeyedAdmitOutcome::Started {
                    admitted,
                    ..
                }) => gate.hold(&admitted.session.id, &admitted.run.id).await,
                _ => (),
            }
        }
        Ok(completed)
    }

    fn supports_session_service_dependencies(&self) -> bool {
        self.inner.supports_session_service_dependencies()
    }
    async fn register_session_service_dependency(
        &self,
        request: crabber_session::session_owner::service::RegisterSessionServiceDependency,
    ) -> Result<
        crabber_session::session_owner::service::SessionServiceDependencyOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate
            .service_register_calls
            .fetch_add(1, Ordering::SeqCst);
        let point = self.gate.service_register_fault.swap(0, Ordering::SeqCst);
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let result = self
            .inner
            .register_session_service_dependency(request)
            .await?;
        if self.gate.service_register_hold.load(Ordering::SeqCst) {
            self.gate.service_register_entered.notify_one();
            self.gate.service_register_release.notified().await;
        }
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn check_session_service_dependency_access(
        &self,
        request: crabber_session::session_owner::service::SessionServiceDependencyAccess,
    ) -> Result<(), crabber_session::session_owner::SessionOwnerError> {
        assert!(
            !self.gate.service_access_panic.swap(false, Ordering::SeqCst),
            "service access store future panicked after actual task dispatch"
        );
        self.inner
            .check_session_service_dependency_access(request)
            .await
    }
    fn supports_execution_drivers(&self) -> bool {
        self.inner.supports_execution_drivers()
    }
    async fn register_execution_driver(
        &self,
        request: crabber_session::session_owner::driver::RegisterExecutionDriver,
    ) -> Result<
        crabber_session::session_owner::driver::ExecutionDriverOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let point = self.gate.driver_register_fault.swap(0, Ordering::SeqCst);
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let substitute = self
            .gate
            .driver_register_request_override
            .lock()
            .unwrap()
            .take();
        let outcome = self
            .inner
            .register_execution_driver(substitute.clone().unwrap_or(request))
            .await?;
        if substitute.is_some() {
            *self.gate.driver_register_captured.lock().unwrap() = Some(outcome.clone());
        }
        if self.gate.driver_register_hold.load(Ordering::SeqCst) {
            self.gate.driver_register_entered.notify_one();
            self.gate.driver_register_release.notified().await;
        }
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    fn supports_preparation_resources(&self) -> bool {
        true
    }
    fn supports_owned_scopes(&self) -> bool {
        true
    }
    async fn retain_preparation_resources(
        &self,
        grant: crabber_session::session_owner::preparation::PreparationGrant,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationResourceOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.retain_preparation_resources(grant).await
    }
    async fn publish_owned_scope_activation(
        &self,
        request: crabber_session::session_owner::owned_scope::OwnedScopeActivation,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
        parent: Option<crabber_session::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let fault = if authority.target().id.0.starts_with("service-owner-") {
            &self.gate.service_activation_fault
        } else if authority.target().id.0.starts_with("driver-owner-") {
            &self.gate.driver_activation_fault
        } else {
            &self.gate.activation_fault
        };
        let point = fault.swap(0, Ordering::SeqCst);
        assert_ne!(point, 3, "activation Store panic before publication");
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let outcome = self
            .inner
            .publish_owned_scope_activation(request, authority.clone(), parent)
            .await?;
        assert_ne!(point, 4, "activation Store panic after publication");
        if authority.target().id.0.starts_with("service-owner-")
            && self.gate.service_activation_hold.load(Ordering::SeqCst)
        {
            self.gate.service_activation_entered.notify_one();
            self.gate.service_activation_release.notified().await;
        }
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    async fn join_owned_scope(
        &self,
        proof: crabber_session::session_owner::owned_scope::JoinedOwnedScope,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        if authority.target().id.0.starts_with("service-owner-") {
            self.gate.service_join_calls.fetch_add(1, Ordering::SeqCst);
            self.gate
                .service_join_keys
                .lock()
                .unwrap()
                .push(proof.request()?.id);
            let point = self.gate.service_join_fault.swap(0, Ordering::SeqCst);
            let panic = self.gate.service_join_panic.load(Ordering::SeqCst);
            assert_ne!(panic, 1, "service Join store panicked before commit");
            if point == 1 {
                return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
            }
            let mut result = self.inner.join_owned_scope(proof, authority).await?;
            assert_ne!(panic, 2, "service Join store panicked after commit");
            match &mut result {
                crabber_session::session_owner::SessionOwnerOutcome::Applied {
                    receipt, ..
                }
                | crabber_session::session_owner::SessionOwnerOutcome::Replayed(receipt) => {
                    if self
                        .gate
                        .service_join_bad_termination
                        .load(Ordering::SeqCst)
                    {
                        receipt.owner.termination = Some(
                            crabber_session::session_owner::SessionOwnerTermination::Cancelled,
                        );
                    }
                    if self.gate.service_join_bad_binding.load(Ordering::SeqCst) {
                        receipt.owner.binding.component_id = "changed-service-component".into();
                    }
                }
            }
            if point == 2 {
                return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
            }
            return Ok(result);
        }
        if authority.target().id.0.starts_with("driver-owner-") {
            self.gate.driver_join_calls.fetch_add(1, Ordering::SeqCst);
            let point = self.gate.driver_join_fault.swap(0, Ordering::SeqCst);
            assert_ne!(point, 3, "Join Store panic before publication");
            if point == 1 {
                return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
            }
            let outcome = self.inner.join_owned_scope(proof, authority).await?;
            assert_ne!(point, 4, "Join Store panic after publication");
            if point == 2 {
                return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
            }
            return Ok(outcome);
        }
        self.gate.owned_join_calls.fetch_add(1, Ordering::SeqCst);
        let point = self.gate.owned_join_fault.swap(0, Ordering::SeqCst);
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let outcome = self
            .inner
            .join_owned_scope(proof, authority.clone())
            .await?;
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let malformed = self.gate.malformed_owned_join.swap(0, Ordering::SeqCst);
        if malformed != 0 {
            use crabber_session::session_owner::{
                SessionOwnerOutcome, SessionOwnerState, SessionOwnerTermination,
            };
            let mut receipt = match outcome {
                SessionOwnerOutcome::Applied { receipt, .. }
                | SessionOwnerOutcome::Replayed(receipt) => receipt,
            };
            match malformed {
                1 => receipt.owner.state = SessionOwnerState::Active,
                2 => receipt.operation_id = crabber_core::SessionOwnerOperationId::new(),
                3 => receipt.owner.termination = Some(SessionOwnerTermination::Panicked),
                5 => receipt.owner.binding.workspace_id = "different".into(),
                6 => receipt.owner.created_at += time::Duration::seconds(1),
                _ => (),
            }
            return Ok(SessionOwnerOutcome::Applied {
                receipt,
                authority: (malformed == 4).then_some(authority),
            });
        }
        Ok(outcome)
    }

    fn supports_session_owner_cancellation(&self) -> bool {
        true
    }
    async fn request_session_owner_cancel(
        &self,
        request: crabber_session::session_owner::RequestSessionOwnerCancel,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.request_session_owner_cancel(request).await
    }
    async fn pending_session_owner_cancel(
        &self,
        authority: &crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        Option<crabber_session::session_owner::SessionOwnerCancellation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        if self.gate.invocation_poll_panic.load(Ordering::SeqCst)
            && self
                .inner
                .get_session_owner(authority.session_id(), &authority.target().id)
                .await?
                .binding
                .kind
                == crabber_session::session_owner::SessionOwnerKind::DetachedInvocation
            && self
                .gate
                .invocation_poll_panic
                .swap(false, Ordering::SeqCst)
        {
            self.gate.invocation_poll_entered.notify_one();
            panic!("detached control Store future panicked");
        }
        if authority.target().id.0.starts_with("driver-owner-")
            && self.gate.driver_poll_panic.swap(false, Ordering::SeqCst)
        {
            self.gate.driver_poll_entered.notify_one();
            panic!("driver control Store future panicked");
        }
        if authority.target().id.0.starts_with("driver-owner-")
            && self.gate.driver_poll_hold.swap(false, Ordering::SeqCst)
        {
            self.gate.driver_poll_entered.notify_one();
            self.gate.driver_poll_release.notified().await;
            self.gate
                .driver_poll_completed
                .fetch_add(1, Ordering::SeqCst);
        }
        if !authority.target().id.0.starts_with("driver-owner-")
            && let Some(gate) = &self.gate.preparation_heartbeat
        {
            gate.pending().await;
        }
        self.inner.pending_session_owner_cancel(authority).await
    }
    async fn observe_session_owner_cancel(
        &self,
        request: crabber_session::session_owner::ObserveSessionOwnerCancel,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate
            .owner_observe_keys
            .lock()
            .unwrap()
            .push(request.id.clone());
        let result = self
            .inner
            .observe_session_owner_cancel(request, authority)
            .await?;
        if self
            .gate
            .owner_observe_ack_lost
            .swap(false, Ordering::SeqCst)
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    fn supports_session_owners(&self) -> bool {
        true
    }
    async fn reserve_session_owner(
        &self,
        request: crabber_session::session_owner::ReserveSessionOwner,
        parent: Option<crabber_session::session_owner::SessionOwnerParentAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let result = self.inner.reserve_session_owner(request, parent).await?;
        if self
            .gate
            .session_owner_ack_lost
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn mutate_session_owner(
        &self,
        request: crabber_session::session_owner::MutateSessionOwner,
        authority: Option<crabber_session::session_owner::SessionOwnerAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let kind = self
            .inner
            .get_session_owner(&request.session_id, &request.target.id)
            .await?
            .binding
            .kind;
        if matches!(
            request.command,
            crabber_session::session_owner::SessionOwnerCommand::Renew { .. }
        ) && matches!(
            kind,
            crabber_session::session_owner::SessionOwnerKind::Preparation
                | crabber_session::session_owner::SessionOwnerKind::RetainedCapacity
        ) && let Some(gate) = &self.gate.preparation_heartbeat
        {
            gate.renew().await;
        }
        let uncertain = matches!(
            request.command,
            crabber_session::session_owner::SessionOwnerCommand::MarkUncertain { .. }
        ) && kind
            == crabber_session::session_owner::SessionOwnerKind::RetainedCapacity;
        let uncertain_fault = if uncertain {
            self.gate
                .cleanup_uncertain_keys
                .lock()
                .unwrap()
                .push(request.id.clone());
            self.gate.cleanup_uncertain_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if uncertain_fault == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let detached = kind == crabber_session::session_owner::SessionOwnerKind::DetachedInvocation;
        let fault = match request.command {
            crabber_session::session_owner::SessionOwnerCommand::Begin => 2,
            crabber_session::session_owner::SessionOwnerCommand::Join { .. } => 3,
            _ => 0,
        };
        let scope = request.session_id.clone();
        let target = request.target.clone();
        let result = self.inner.mutate_session_owner(request, authority).await?;
        if uncertain_fault == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        if fault == 2 {
            self.cancel_before_begin_ack(scope, target, detached)
                .await?;
        }
        if detached
            && fault != 0
            && self
                .gate
                .session_owner_ack_lost
                .compare_exchange(fault, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn lookup_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerOperationId,
    ) -> Result<
        Option<crabber_session::session_owner::SessionOwnerReceipt>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_session_owner(session, id).await
    }
    async fn get_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerId,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerSnapshot,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.get_session_owner(session, id).await
    }

    fn supports_tool_continuations(&self) -> bool {
        true
    }
    async fn tool_continuation_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<
        Option<crabber_session::ToolContinuationSnapshot>,
        crabber_session::ToolContinuationError,
    > {
        self.inner.tool_continuation_snapshot(session, run).await
    }

    async fn original_tool_continuation_plan(
        &self,
        selected: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        target: &crabber_session::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crabber_session::ToolContinuationError> {
        self.inner
            .original_tool_continuation_plan(selected, run, target)
            .await
    }
    async fn lookup_tool_continuation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<
        Option<crabber_session::ToolContinuationReceipt>,
        crabber_session::ToolContinuationError,
    > {
        if self
            .gate
            .observation
            .as_ref()
            .is_some_and(|gate| gate.lose_ack)
        {
            return Err(crabber_session::ToolContinuationError::UnknownStoreOutcome);
        }
        self.inner.lookup_tool_continuation(session, id).await
    }
    fn supports_owned_attempts(&self) -> bool {
        true
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.inner.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.inner.supports_pre_effect_approvals()
    }
    async fn tool_phase(
        &self,
        run: &crabber_core::RunId,
        id: &crabber_core::PhaseId,
    ) -> Result<crabber_session::VersionedPhase, crabber_session::PhaseError> {
        self.inner.tool_phase(run, id).await
    }
    async fn mark_phase_uncertain(
        &self,
        request: crabber_session::PhaseUncertainty,
    ) -> Result<crabber_session::PhaseControlOutcome, crabber_session::PhaseError> {
        self.inner.mark_phase_uncertain(request).await
    }
    async fn record_phase_cleanup(
        &self,
        request: crabber_session::PhaseCleanup,
    ) -> Result<crabber_session::PhaseControlOutcome, crabber_session::PhaseError> {
        self.inner.record_phase_cleanup(request).await
    }
    async fn list_tool_phases(
        &self,
        request: crabber_session::PhasePageRequest,
    ) -> Result<crabber_session::PhasePage, crabber_session::PhaseError> {
        self.inner.list_tool_phases(request).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crabber_session::PhaseResolution,
    ) -> Result<crabber_session::PhaseResolutionOutcome, crabber_session::PhaseError> {
        self.inner.resolve_uncertain_phase(resolution).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::PhaseReceipt>, crabber_session::PhaseError> {
        self.inner.lookup_phase_mutation(session, id).await
    }
    async fn approval_snapshot(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
    ) -> Result<Option<crabber_session::ApprovalSnapshot>, crabber_session::ApprovalError> {
        self.inner.approval_snapshot(session, run).await
    }
    async fn original_approval_plan(
        &self,
        selected: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        target: &crabber_session::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crabber_session::ApprovalError> {
        self.inner
            .original_approval_plan(selected, run, target)
            .await
    }
    async fn lookup_approval(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::ApprovalReceipt>, crabber_session::ApprovalError> {
        self.inner.lookup_approval(session, id).await
    }
    async fn answer_approval(
        &self,
        answer: crabber_session::ApprovalAnswer,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.inner.answer_approval(answer).await
    }
    async fn terminate_approval(
        &self,
        request: crabber_session::ApprovalTermination,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.inner.terminate_approval(request).await
    }
    async fn reclaim_approval_continuation(
        &self,
        request: crabber_session::ApprovalContinuationRecovery,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        if self.gate.approval_recovery_gate.load(Ordering::SeqCst) {
            self.gate.approval_recovery_entered.notify_one();
            self.gate.approval_recovery_release.notified().await;
        }
        let outcome = self.inner.reclaim_approval_continuation(request).await?;
        if matches!(
            outcome,
            crabber_session::ApprovalClaimOutcome::Applied { .. }
        ) && self.gate.approval_recovery_ack_lost.load(Ordering::SeqCst)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    async fn claim_approval_continuation(
        &self,
        claim: crabber_session::ApprovalContinuationClaim,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        let outcome = self.inner.claim_approval_continuation(claim).await?;
        if matches!(
            outcome,
            crabber_session::ApprovalClaimOutcome::Applied { .. }
        ) && self.gate.approval_claim_ack_lost.load(Ordering::SeqCst)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }

    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::VersionedAttempt>, crabber_session::AttemptError> {
        self.inner.latest_execution_attempt(run).await
    }

    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        self.inner.session_revision(session).await
    }

    async fn admit_bound_run(
        &self,
        request: crabber_session::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        let admitted = self.inner.admit_bound_run(request).await?;
        if let Some(gate) = &self.gate.admission {
            gate.hold(&admitted.session.id, &admitted.run.id).await;
        }
        Ok(admitted)
    }
    async fn admit_bound_keyed_run(
        &self,
        request: crabber_session::BoundKeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        let outcome = self.inner.admit_bound_keyed_run(request).await?;
        if let (Some(gate), crabber_session::KeyedAdmitOutcome::Started { admitted, .. }) =
            (&self.gate.admission, &outcome)
        {
            gate.hold(&admitted.session.id, &admitted.run.id).await;
        }
        Ok(outcome)
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber_session::RetainedExecutionBinding>, StoreError> {
        self.inner.execution_binding(session, run).await
    }

    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        request: crabber_session::KeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        self.inner.admit_keyed_run(request).await
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &crabber_core::AdmissionKey,
    ) -> Result<
        Option<crabber_session::AdmissionExecutionRecord>,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn inspect_admission_recovery(
        &self,
        request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<
        crabber_session::session_owner::preparation::AdmissionRecoveryPreparation,
        crabber_session::AdmissionExecutionError,
    > {
        if self
            .gate
            .unsupported_recovery_inspection
            .load(Ordering::SeqCst)
        {
            return Err(crabber_session::AdmissionExecutionError::Unsupported);
        }
        self.inner.inspect_admission_recovery(request).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<crabber_session::ClaimedAdmission, crabber_session::AdmissionExecutionError> {
        let claimed = self.inner.claim_unstarted_admission(request).await?;
        if let Some(gate) = &self.gate.admission {
            gate.hold(&claimed.run.session_id, &claimed.run.id).await;
        }
        Ok(claimed)
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
        assert!(
            !self.gate.reject_run_reads.load(Ordering::SeqCst),
            "metadata replay attempted a full private Run read"
        );
        let mut run = self.inner.get_run(id).await?;
        if let Some(checkpoint) = self.gate.checkpoint_override.lock().unwrap().clone()
            && let Some(run) = &mut run
        {
            run.checkpoint = Some(checkpoint);
        }
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
        self.gate.unfinished_reads.fetch_add(1, Ordering::SeqCst);
        let mut calls = self.inner.list_unfinished_tool_calls(run).await?;
        if let Some(arguments) = self.gate.call_arguments_override.lock().unwrap().clone() {
            for call in &mut calls {
                call.arguments.clone_from(&arguments);
            }
        }
        Ok(calls)
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::AdmissionExecutionState>, StoreError> {
        self.inner.admission_execution_state(run).await
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
    async fn observe_run_controls(
        &self,
        turn: Option<&crabber_core::TurnId>,
    ) -> Result<crabber_session::RunControlBoundary, crabber_session::RunControlError> {
        if let Some(gate) = &self.gate.observation {
            return gate.observe(self.inner.as_ref(), turn).await;
        }
        self.inner.observe_run_controls(turn).await
    }

    async fn suspend_tool_continuation(
        &self,
        request: crabber_session::ToolContinuationSuspension,
        authority: crabber_session::ToolSuspensionAuthority,
    ) -> Result<crabber_session::ToolContinuationReceipt, crabber_session::ToolContinuationError>
    {
        tool_suspension_faults::suspend(self, request, authority).await
    }
    async fn mutate_phase(
        &self,
        request: crabber_session::PhaseMutation,
        authority: Option<crabber_session::PhaseWriteAuthority>,
    ) -> Result<crabber_session::PhaseMutationOutcome, crabber_session::PhaseError> {
        self.inner.mutate_phase(request, authority).await
    }
    async fn start_tool_phase(
        &self,
        start: crabber_session::PhaseStart,
        authority: Option<crabber_session::PhaseAuthority>,
    ) -> Result<crabber_session::PhaseMutationOutcome, crabber_session::PhaseError> {
        if self.gate.fail_claim.load(Ordering::SeqCst)
            || self.gate.phase_fault.load(Ordering::SeqCst) == 1
        {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        let result = self.inner.start_tool_phase(start, authority).await;
        if result.is_ok() && self.gate.phase_fault.load(Ordering::SeqCst) == 2 {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        result
    }
    async fn settle_tool_phase(
        &self,
        settlement: crabber_session::PhaseSettlement,
        authority: Option<crabber_session::PhaseAuthority>,
    ) -> Result<crabber_session::PhaseMutationOutcome, crabber_session::PhaseError> {
        self.gate.tool_settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.fail_settle.load(Ordering::SeqCst)
            || self.gate.phase_fault.load(Ordering::SeqCst) == 3
        {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        let result = self.inner.settle_tool_phase(settlement, authority).await;
        if result.is_ok() && self.gate.phase_fault.load(Ordering::SeqCst) == 4 {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        result
    }
    async fn suspend_for_approval(
        &self,
        batch: crabber_session::ApprovalSuspension,
        authority: crabber_session::AttemptAuthority,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        if self.gate.fail_permission_event.load(Ordering::SeqCst) {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
        }
        let result = self.inner.suspend_for_approval(batch, authority).await;
        if let Some(gate) = &self.gate.observation
            && result.is_ok()
        {
            gate.acknowledge().await;
            if gate.lose_ack {
                return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
            }
        }
        result
    }

    async fn mutate_attempt(
        &self,
        request: crabber_session::AttemptMutation,
        authority: Option<crabber_session::AttemptAuthority>,
    ) -> Result<crabber_session::AttemptMutationOutcome, crabber_session::AttemptError> {
        let begin = matches!(
            request.command,
            crabber_session::AttemptCommand::Begin { .. }
        );
        let reserve = matches!(
            &request.command,
            crabber_session::AttemptCommand::Reserve { .. }
                | crabber_session::AttemptCommand::ReserveBound { .. }
        );
        let result = self.inner.mutate_attempt(request, authority).await;
        if reserve
            && matches!(
                &result,
                Ok(crabber_session::AttemptMutationOutcome::Applied { .. })
            )
            && self.gate.reserve_ack_lost.load(Ordering::SeqCst)
        {
            return Err(crabber_session::AttemptError::UnknownStoreOutcome);
        }
        if begin && result.is_ok() && self.gate.begin_ack_lost.load(Ordering::SeqCst) {
            return Err(crabber_session::AttemptError::UnknownStoreOutcome);
        }
        result
    }
    async fn conclude_attempt(
        &self,
        conclusion: crabber_session::AttemptConclusion,
        authority: Option<crabber_session::AttemptAuthority>,
    ) -> Result<crabber_session::AttemptMutationOutcome, crabber_session::AttemptError> {
        use crabber_session::AttemptError;
        if matches!(
            &conclusion.boundary,
            crabber_session::RunBoundary::Suspended { .. }
        ) {
            let result = self.inner.conclude_attempt(conclusion, authority).await;
            if let Some(gate) = &self.gate.observation
                && result.is_ok()
            {
                gate.acknowledge().await;
                if gate.lose_ack {
                    return Err(AttemptError::UnknownStoreOutcome);
                }
            }
            return result;
        }
        if self.gate.observation.is_some() {
            // This fixture delays only suspension ACKs, never terminal publication.
            return self.inner.conclude_attempt(conclusion, authority).await;
        }
        self.gate.settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 1 {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        let completed = matches!(
            &conclusion.boundary,
            crabber_session::RunBoundary::Terminal {
                status: RunStatus::Completed,
                ..
            }
        );
        let result = self.inner.conclude_attempt(conclusion, authority).await;
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 2 && result.is_ok() {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        if completed && result.is_ok() {
            if let Some(sender) = self.gate.entered.lock().unwrap().take() {
                let _ = sender.send(());
            }
            self.gate.release.notified().await;
        }
        result
    }

    async fn begin_admission_execution(
        &self,
    ) -> Result<(), crabber_session::AdmissionExecutionError> {
        self.inner.begin_admission_execution().await
    }
    async fn renew_lease(&self, until: time::OffsetDateTime) -> Result<(), StoreError> {
        if let Some(gate) = &self.gate.observation {
            return gate.renew(self.inner.as_ref(), until).await;
        }
        self.inner.renew_lease(until).await
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        if message.role == Role::Assistant
            && let Some(gate) = &self.gate.append
            && gate.enabled.load(Ordering::SeqCst)
        {
            gate.entered.notify_one();
            gate.release.notified().await;
            if gate.conflict.load(Ordering::SeqCst) {
                return Err(StoreError::Conflict);
            }
        }
        self.inner.append_message(message).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.inner.append_part(part).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        if event.kind == EventKind::PermissionRequested
            && self.gate.fail_permission_event.load(Ordering::SeqCst)
        {
            return Err(StoreError::Validation("SECRET permission backend".into()));
        }
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
        if self.gate.fail_claim.load(Ordering::SeqCst) {
            return Err(StoreError::Validation("SECRET backend".into()));
        }
        self.inner.claim_tool_call(id, running_event).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.gate.tool_settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.fail_settle.load(Ordering::SeqCst) {
            return Err(StoreError::Conflict);
        }
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
        self.gate.settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 1 {
            return Err(StoreError::Validation(
                "terminal acknowledgement unavailable".into(),
            ));
        }
        let result = self.inner.settle_run(status, error, usage, event).await;
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 2 && result.is_ok() {
            return Err(StoreError::Validation(
                "terminal acknowledgement unavailable".into(),
            ));
        }
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
        ..SettlementGate::default()
    });
    let store = Arc::new(DelayedTerminalStore {
        inner: inner.clone(),
        gate: Arc::clone(&gate),
    });
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
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

#[derive(Default)]
struct AttemptObserver(Mutex<Vec<(RunId, Option<crabber_core::TraceContext>)>>);
impl Observer for AttemptObserver {
    fn emit(&self, event: &EventRecord) {
        self.emit_with_context(event, None);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&crabber_core::TraceContext>) {
        self.0
            .lock()
            .unwrap()
            .push((event.run_id.clone(), context.cloned()));
    }
    fn model_completed_with_context(
        &self,
        event: &EventRecord,
        context: Option<&crabber_core::TraceContext>,
    ) {
        self.emit_with_context(event, context);
    }
}
#[tokio::test]
async fn recovery_selects_current_attempt_per_run_without_ambient_contamination() {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let capture = Arc::new(AttemptObserver::default());
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .clock(clock.clone())
        .observer(capture.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"one"}"#),
            call_script(ToolCallId::new(), r#"{"text":"two"}"#),
            text_script("done"),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(executed.clone())))],
            Vec::new(),
        )))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let prior = crabber_core::TraceContext::new("1234567890abcdef", "1234567890abcdef").unwrap();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let handle = runtime
            .start_with_context(request(), Some(prior.clone()))
            .await
            .unwrap();
        ids.push(handle.run_id().clone());
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    }
    capture.0.lock().unwrap().clear();
    clock.set(now + time::Duration::seconds(31));
    let contexts = ["0000000000000001", "0000000000000002"].map(|trace| {
        crabber_core::TraceContext::new(trace, "1234567890abcdef")
            .unwrap()
            .linked_to(&prior)
            .unwrap()
    });
    let results = runtime
        .recover_with_context(|run| {
            Some(contexts[ids.iter().position(|id| id == &run.id).unwrap()].clone())
        })
        .await
        .unwrap()
        .recovered;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.status == RunStatus::Completed)
    );
    assert_eq!(executed.load(Ordering::SeqCst), 2);
    for (i, id) in ids.iter().enumerate() {
        let events = capture.0.lock().unwrap();
        assert!(events.iter().any(|(run, _)| run == id));
        assert!(
            events
                .iter()
                .filter(|(run, _)| run == id)
                .all(|(_, context)| context.as_ref() == Some(&contexts[i]))
        );
    }
}

#[tokio::test]
async fn linked_expired_legacy_recovery_cannot_grant_dispatch() {
    for running in [false, true] {
        let capture = Arc::new(AttemptObserver::default());
        let (runtime, store, executed, run_id) =
            crashed_run_with_observer(running, capture.clone()).await;
        let before = store.list_unfinished_tool_calls(&run_id).await.unwrap();
        let current =
            crabber_core::TraceContext::new("9876543210abcdef", "9876543210abcdef").unwrap();
        let report = runtime
            .recover_with_context(|_| Some(current.clone()))
            .await
            .unwrap();
        assert_eq!(report.recovered, Vec::new());
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(
            report.skipped[0].reason,
            crate::RecoverySkipReason::Attempt(crabber_session::AttemptError::InvalidTransition)
        );
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.list_unfinished_tool_calls(&run_id).await.unwrap(),
            before
        );
        assert!(capture.0.lock().unwrap().is_empty());
    }
}

#[derive(Default)]
struct OperationalCapture {
    records: Mutex<Vec<EventRecord>>,
    values: Mutex<Vec<crate::OperationalObservation>>,
    text: Notify,
}
impl Observer for OperationalCapture {
    fn emit(&self, event: &EventRecord) {
        self.records.lock().unwrap().push(event.clone());
        if event.kind == EventKind::TextDelta {
            self.text.notify_one();
        }
    }
    fn operational_completed(&self, value: &crate::OperationalObservation) {
        self.values.lock().unwrap().push(value.clone());
    }
}
#[derive(Default)]
struct OperationalClock(AtomicUsize);
impl crate::MonotonicClock for OperationalClock {
    fn now(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.0.load(Ordering::SeqCst) as u64)
    }
}
#[derive(Clone)]
struct OperationalStream {
    clock: Arc<OperationalClock>,
    startup_error: bool,
    script: Vec<(usize, StreamDelta)>,
    entered: Option<Arc<Notify>>,
    block_setup: bool,
    block_end: bool,
}
#[async_trait]
impl Resolver for OperationalStream {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber_core::ProviderBindingDescriptor, crabber_providers::ProviderBindingError>
    {
        crabber_core::ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "operational-fixture",
            "1",
            crabber_core::InputFingerprint::new(crabber_session::admission_config_hash(&json!([
                self.startup_error,
                self.block_setup,
                self.block_end
            ])))
            .unwrap(),
            crabber_core::ProviderBindingLimits::default(),
        )
        .map_err(|_| crabber_providers::ProviderBindingError::InvalidDescriptor)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for OperationalStream {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.clock.0.store(10, Ordering::SeqCst);
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        if self.block_setup {
            futures::future::pending::<()>().await;
        }
        if self.startup_error {
            return Err(operational_error("SECRET startup"));
        }
        let clock = self.clock.clone();
        let script = self.script.clone();
        let stream = futures::stream::iter(script).map(move |(at, delta)| {
            clock.0.store(at, Ordering::SeqCst);
            delta
        });
        if self.block_end {
            Ok(Box::pin(stream.chain(futures::stream::pending())))
        } else {
            Ok(Box::pin(stream))
        }
    }
}
fn operational_runtime(
    capture: Arc<OperationalCapture>,
    clock: Arc<OperationalClock>,
    stream: OperationalStream,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(stream))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(capture)
        .monotonic_clock(clock)
        .build()
        .unwrap()
}
fn operational_stream(clock: Arc<OperationalClock>) -> OperationalStream {
    OperationalStream {
        clock,
        startup_error: false,
        script: Vec::new(),
        entered: None,
        block_setup: false,
        block_end: false,
    }
}
#[tokio::test]
async fn operational_startup_midstream_success_and_first_text_boundaries() {
    use crate::{OperationKind, TerminalReason};
    for (startup_error, script, reason, first_token, elapsed) in [
        (true, vec![], TerminalReason::ProviderError, None, 10),
        (
            false,
            vec![
                (15, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
                (20, StreamDelta::Error(operational_error("SECRET error"))),
            ],
            TerminalReason::ProviderError,
            None,
            20,
        ),
        (
            false,
            vec![
                (15, StreamDelta::TextDelta(String::new())),
                (20, StreamDelta::TextDelta("SECRET token".into())),
                (30, StreamDelta::TextDelta("SECRET second".into())),
                (40, StreamDelta::Completed),
            ],
            TerminalReason::Success,
            Some(20),
            40,
        ),
        (
            false,
            vec![
                (20, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
                (40, StreamDelta::Completed),
            ],
            TerminalReason::Success,
            None,
            40,
        ),
    ] {
        let clock = Arc::new(OperationalClock::default());
        let capture = Arc::new(OperationalCapture::default());
        let mut stream = operational_stream(clock.clone());
        stream.startup_error = startup_error;
        stream.script = script;
        let runtime = operational_runtime(capture.clone(), clock, stream);
        let result = runtime.start(request()).await.unwrap().done().await;
        assert_eq!(result.is_ok(), reason == TerminalReason::Success);
        let values = capture.values.lock().unwrap().clone();
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0].kind, OperationKind::Model { .. }));
        assert_eq!(values[0].reason, reason);
        assert_eq!(values[0].elapsed, std::time::Duration::from_millis(elapsed));
        assert_eq!(
            values[0].first_token,
            first_token.map(std::time::Duration::from_millis)
        );
        assert_eq!(values[1].kind, OperationKind::Run);
        assert_eq!(values[1].reason, reason);
        assert_eq!(values[1].elapsed, std::time::Duration::from_millis(elapsed));
        assert!(!format!("{values:?}").contains("SECRET"));
    }
}
#[tokio::test]
async fn operational_cancellation_before_and_after_stream_setup() {
    for block_setup in [true, false] {
        let clock = Arc::new(OperationalClock::default());
        let capture = Arc::new(OperationalCapture::default());
        let entered = Arc::new(Notify::new());
        let mut stream = operational_stream(clock.clone());
        stream.entered = Some(entered.clone());
        stream.block_setup = block_setup;
        stream.block_end = true;
        stream.script = vec![(20, StreamDelta::TextDelta("partial".into()))];
        let runtime = operational_runtime(capture.clone(), clock, stream);
        let handle = runtime.start(request()).await.unwrap();
        if block_setup {
            entered.notified().await;
        } else {
            capture.text.notified().await;
        }
        handle.interrupt();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
        let values = capture.values.lock().unwrap().clone();
        assert_eq!(values.len(), 2);
        assert!(
            values
                .iter()
                .all(|value| value.reason == crate::TerminalReason::Cancelled)
        );
        assert_eq!(values[0].first_token.is_some(), !block_setup);
    }
}
#[tokio::test(start_paused = true)]
async fn operational_lease_loss_is_local_once_and_does_not_settle_replacement() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let clock = Arc::new(OperationalClock::default());
    let capture = Arc::new(OperationalCapture::default());
    let entered = Arc::new(Notify::new());
    let mut stream = operational_stream(clock.clone());
    stream.entered = Some(entered.clone());
    stream.block_setup = true;
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .clock(wall.clone())
        .monotonic_clock(clock)
        .resolver(Arc::new(stream))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(capture.clone())
        .heartbeat_interval(std::time::Duration::from_secs(1))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    entered.notified().await;
    wall.set(now + time::Duration::seconds(31));
    let replacement = store.claim_expired_run(&run, "replacement").await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert!(matches!(handle.done().await, Err(RuntimeError::LeaseLost)));
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e.kind, EventKind::MessageCommitted | EventKind::RunSettled))
    );
    let values = capture.values.lock().unwrap().clone();
    assert_eq!(values.len(), 2);
    assert!(
        values
            .iter()
            .all(|value| value.reason == crate::TerminalReason::LeaseLost)
    );
    assert!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .all(|event| event.kind != EventKind::RunSettled)
    );
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().claim_token,
        replacement.claim_token
    );
}
#[tokio::test]
async fn operational_tool_failure_remains_distinct_when_run_succeeds() {
    let capture = Arc::new(OperationalCapture::default());
    let clock = Arc::new(OperationalClock::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"SECRET argument"}"#),
            text_script("after"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(OperationalFailingTool(clock.clone())))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .monotonic_clock(clock)
        .observer(capture.clone())
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
    let values = capture.values.lock().unwrap().clone();
    assert_eq!(values.len(), 4);
    assert_eq!(values[1].reason, crate::TerminalReason::ToolError);
    assert_eq!(values[1].elapsed, std::time::Duration::from_millis(15));
    assert!(matches!(values[1].kind, crate::OperationKind::Tool { .. }));
    assert_eq!(values[3].reason, crate::TerminalReason::Success);
}

fn operational_error(message: &str) -> ProviderError {
    ProviderError {
        kind: crabber_providers::ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}

struct OperationalFailingTool(Arc<OperationalClock>);
#[async_trait]
impl JsonToolExecutor for OperationalFailingTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.0.store(15, Ordering::SeqCst);
        Err(ExtensionError::Plan("SECRET tool error".into()))
    }
}

#[tokio::test]
async fn operational_regressing_clock_saturates_to_zero() {
    let clock = Arc::new(OperationalClock::default());
    clock.0.store(100, Ordering::SeqCst);
    let capture = Arc::new(OperationalCapture::default());
    let mut stream = operational_stream(clock.clone());
    stream.script = vec![
        (20, StreamDelta::TextDelta("text".into())),
        (30, StreamDelta::Completed),
    ];
    let runtime = operational_runtime(capture.clone(), clock, stream);
    runtime
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let values = capture.values.lock().unwrap();
    assert!(
        values
            .iter()
            .all(|value| value.elapsed == std::time::Duration::ZERO)
    );
    assert_eq!(values[0].first_token, Some(std::time::Duration::ZERO));
}

#[derive(Default)]
struct OperationalAppendGate {
    enabled: std::sync::atomic::AtomicBool,
    conflict: std::sync::atomic::AtomicBool,
    entered: Notify,
    release: Notify,
}
#[tokio::test(start_paused = true)]
async fn operational_store_first_takeover_skips_settlement_initial_and_resume() {
    for resume in [false, true] {
        let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let wall = Arc::new(ManualClock::new(now));
        let inner = Arc::new(MemoryStore::with_clock(wall.clone()));
        let append = Arc::new(OperationalAppendGate::default());
        let gate = Arc::new(SettlementGate {
            append: Some(append.clone()),
            ..SettlementGate::default()
        });
        let store = Arc::new(DelayedTerminalStore {
            inner: inner.clone(),
            gate: gate.clone(),
        });
        let capture = Arc::new(OperationalCapture::default());
        let scripts = if resume {
            vec![
                call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
                text_script("resumed"),
            ]
        } else {
            vec![text_script("initial")]
        };
        let mut builder = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store)
            .clock(wall.clone())
            .observer(capture.clone())
            .resolver(Arc::new(FakeProvider::scripted(scripts)))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
                Vec::new(),
            )));
        if resume {
            builder = builder.policy(Arc::new(PausePolicy));
        }
        let runtime = builder.build().unwrap();
        append.enabled.store(!resume, Ordering::SeqCst);
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        let session = handle.session_id().clone();
        let done = if resume {
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
            capture.values.lock().unwrap().clear();
            append.enabled.store(true, Ordering::SeqCst);
            let runtime = runtime.clone();
            let run_id = run_id.clone();
            tokio::spawn(async move { runtime.resume(&run_id).await })
        } else {
            tokio::spawn(handle.done())
        };
        append.entered.notified().await;
        gate.settlements.store(0, Ordering::SeqCst);
        gate.tool_settlements.store(0, Ordering::SeqCst);
        gate.unfinished_reads.store(0, Ordering::SeqCst);
        wall.set(now + time::Duration::seconds(31));
        let replacement = inner
            .claim_expired_run(&run_id, "replacement")
            .await
            .unwrap();
        append.release.notify_one();
        assert!(matches!(
            done.await.unwrap(),
            Err(RuntimeError::Store(StoreError::Conflict))
        ));
        assert_eq!(gate.settlements.load(Ordering::SeqCst), 0);
        assert_eq!(gate.tool_settlements.load(Ordering::SeqCst), 0);
        assert_eq!(gate.unfinished_reads.load(Ordering::SeqCst), 0);
        let values = capture.values.lock().unwrap().clone();
        let runs: Vec<_> = values
            .iter()
            .filter(|sample| sample.kind == crate::OperationKind::Run)
            .collect();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].reason, crate::TerminalReason::LeaseLost);
        assert!(
            inner
                .list_events(&session, None, 100)
                .await
                .unwrap()
                .iter()
                .all(|event| event.kind != EventKind::RunSettled)
        );
        assert_eq!(
            inner.get_run(&run_id).await.unwrap().unwrap().claim_token,
            replacement.claim_token
        );
    }
}
#[tokio::test(start_paused = true)]
async fn operational_ordinary_store_conflict_keeps_runtime_error_and_settlement() {
    let append = Arc::new(OperationalAppendGate::default());
    append.enabled.store(true, Ordering::SeqCst);
    append.conflict.store(true, Ordering::SeqCst);
    let gate = Arc::new(SettlementGate {
        append: Some(append.clone()),
        ..SettlementGate::default()
    });
    let capture = Arc::new(OperationalCapture::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(DelayedTerminalStore {
            inner: Arc::new(MemoryStore::new()),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script("text")])))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    append.entered.notified().await;
    append.release.notify_one();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Store(StoreError::Conflict))
    ));
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.kind == EventKind::MessageCommitted)
    );
    assert_eq!(gate.settlements.load(Ordering::SeqCst), 1);
    let values = capture.values.lock().unwrap();
    assert_eq!(
        values.last().unwrap().reason,
        crate::TerminalReason::RuntimeError
    );
}
#[tokio::test]
async fn operational_tool_store_setup_error_is_not_tool_error() {
    for permission_event in [false, true] {
        let gate = Arc::new(SettlementGate::default());
        gate.fail_claim.store(!permission_event, Ordering::SeqCst);
        gate.fail_permission_event
            .store(permission_event, Ordering::SeqCst);
        let capture = Arc::new(OperationalCapture::default());
        let executed = Arc::new(AtomicUsize::new(0));
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(Arc::new(DelayedTerminalStore {
                inner: Arc::new(MemoryStore::new()),
                gate,
            }))
            .resolver(Arc::new(FakeProvider::scripted(vec![call_script(
                ToolCallId::new(),
                r#"{"text":"ok"}"#,
            )])))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(EchoTool(executed.clone())))],
                Vec::new(),
            )))
            .policy(Arc::new(StaticPolicy::new(if permission_event {
                PermissionDecision::Ask
            } else {
                PermissionDecision::Allow
            })))
            .observer(capture.clone())
            .build()
            .unwrap();
        let error = runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap_err();
        if permission_event {
            assert!(matches!(
                error,
                RuntimeError::ApprovalBoundaryUnknown { .. }
            ));
        } else {
            assert!(matches!(error, RuntimeError::PhaseOutcomeUnknown { .. }));
        }
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        let values = capture.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].reason, crate::TerminalReason::Success);
        assert_eq!(values[1].reason, crate::TerminalReason::RuntimeError);
        assert!(
            !values
                .iter()
                .any(|value| value.reason == crate::TerminalReason::ToolError)
        );
    }
}

struct OperationalSetupExtension(bool);
#[async_trait]
impl crabber_extension::Extension for OperationalSetupExtension {
    fn id(&self) -> &'static str {
        "operational-setup"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.0.to_string()
    }
    async fn install(
        &self,
        registrar: &mut crabber_extension::Registrar,
    ) -> Result<(), ExtensionError> {
        use crabber_extension::Point;
        let reject = self.0;
        registrar.on_around(
            crabber_extension::ModelStream::ID,
            0,
            "setup",
            Arc::new(move |_, _| {
                Box::pin(async move {
                    if reject {
                        Err(ExtensionError::Plan("SECRET extension".into()))
                    } else {
                        Ok(Value::Null)
                    }
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn operational_extension_setup_rejection_and_omission_are_runtime_errors() {
    for reject in [true, false] {
        let registry = crabber_extension::Registry::new();
        registry
            .mount(
                Arc::new(OperationalSetupExtension(reject)),
                crabber_extension::Scope::Global,
            )
            .await
            .unwrap();
        let fake = FakeProvider::scripted(vec![text_script("never")]);
        let capture = Arc::new(OperationalCapture::default());
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(Arc::new(MemoryStore::new()))
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(registry))
            .observer(capture.clone())
            .build()
            .unwrap();
        assert!(matches!(
            runtime.start(request()).await.unwrap().done().await,
            Err(RuntimeError::Extension(_))
        ));
        assert_eq!(fake.requests(), [] as [crabber_providers::ModelRequest; 0]);
        let values = capture.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0].kind, crate::OperationKind::Model { .. }));
        assert_eq!(values[1].kind, crate::OperationKind::Run);
        assert!(
            values
                .iter()
                .all(|value| value.reason == crate::TerminalReason::RuntimeError
                    && value.first_token.is_none())
        );
        assert!(!format!("{values:?}").contains("SECRET"));
    }
}

mod source_contract;
mod workspace_context;

#[tokio::test]
async fn admission_recovery_verifies_turn_limit_before_terminal_replay() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![text_script("done")]));
    let plan = Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new()));
    let make = |max_turns| {
        Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store.clone())
            .resolver(provider.clone())
            .plan_provider(plan.clone())
            .max_turns(max_turns)
            .build()
            .unwrap()
    };
    let options = crabber_core::AdmissionOptions {
        key: crabber_core::AdmissionKey::new("turn-limit").unwrap(),
        fingerprint: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
    };
    let mut input = request();
    input.session_id = Some(SessionId::new());
    let crate::Admission::Started { handle, receipt } = make(64)
        .start_keyed(input.clone(), options.clone())
        .await
        .unwrap()
    else {
        panic!()
    };
    handle.done().await.unwrap();
    let before = store.get_run(&receipt.run_id).await.unwrap();
    assert!(matches!(
        make(2).recover_admission(input, options).await,
        Err(RuntimeError::Store(StoreError::AdmissionConflict))
    ));
    assert_eq!(store.get_run(&receipt.run_id).await.unwrap(), before);
    assert_eq!(provider.requests().len(), 1);
}

struct PipelineCleanup(Arc<Notify>);
#[async_trait]
impl ToolPipeline for PipelineCleanup {
    async fn prepare(
        &self,
        _tool: &crabber_core::ToolInfo,
        arguments: Value,
    ) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        context: &crabber_extension::ToolResultContext,
        _tool: &crabber_core::ToolInfo,
        result: crabber_core::ToolOutput,
    ) -> Result<crabber_extension::TransformOutput, String> {
        let result = result
            .json_value()
            .map_err(|_| "JSON fixture result required".to_owned())?;
        let closing = context.cleanup().closing();
        let reaped = self.0.clone();
        context.cleanup().spawn(async move {
            closing.await;
            reaped.notify_one();
        });
        Ok(crabber_extension::TransformOutput::new(
            crabber_core::ToolOutput::from_json(&(result)).unwrap(),
        ))
    }
}

#[tokio::test]
async fn pipeline_cleanup_is_joined_before_native_phase_settlement() {
    let store = Arc::new(MemoryStore::new());
    let completed = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool_pipeline(Arc::new(PipelineCleanup(completed.clone())))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .unwrap();
    let report = runtime
        .drain_execution_tasks(std::time::Duration::from_secs(1))
        .await;
    assert_eq!(report.resources.len(), 1);
    assert_eq!(report.resources[0].resources.pending, 0);
    assert_eq!(report.resources[0].resources.joined, 1);
    let phase = report.resources[0].phase.as_ref().unwrap();
    let stored = store.tool_phase(&run, &phase.id).await.unwrap().phase;
    assert_eq!(stored.state, crabber_session::PhaseState::Completed);
    assert_eq!(
        stored.quiescence,
        crabber_session::PhaseQuiescence::Complete
    );
}

#[tokio::test]
async fn zero_output_cap_is_rejected_before_admission() {
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![])))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let mut input = request();
    let session = SessionId::new();
    input.session_id = Some(session.clone());
    input.max_output_tokens = Some(0);
    assert!(matches!(
        runtime.start(input).await,
        Err(RuntimeError::InvalidConfiguration(_))
    ));
    assert_eq!(store.get_session(&session).await.unwrap(), None);
    assert_eq!(
        store.list_unfinished_runs().await.unwrap(),
        Vec::<Run>::new()
    );
}

#[tokio::test(start_paused = true)]
async fn interruption_during_provider_backoff_is_prompt_and_never_retries() {
    struct BackoffSignal(Mutex<Option<oneshot::Sender<()>>>);
    impl Observer for BackoffSignal {
        fn emit(&self, _: &EventRecord) {}
        fn operational_completed(&self, observation: &crate::OperationalObservation) {
            if observation.reason == crate::TerminalReason::ProviderError
                && matches!(
                    observation.kind,
                    crate::OperationKind::Model {
                        purpose: crate::ModelPurpose::Turn,
                        ..
                    }
                )
                && let Some(sender) = self.0.lock().unwrap().take()
            {
                let _ = sender.send(());
            }
        }
    }
    let (sender, first_error) = oneshot::channel();
    let fake = FakeProvider::scripted(vec![
        vec![StreamDelta::Error(ProviderError {
            kind: crabber_providers::ProviderErrorKind::Server,
            message: "temporary".into(),
            retryable: true,
        })],
        text_script("must not request"),
    ]);
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_version("retry-cancellation-test.v1")
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .observer(Arc::new(BackoffSignal(Mutex::new(Some(sender)))))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    first_error.await.unwrap();
    handle.interrupt();
    let result = tokio::time::timeout(std::time::Duration::from_millis(25), handle.done())
        .await
        .expect("cancellation must not wait for the 100ms retry backoff")
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(fake.requests().len(), 1);
}

impl DelayedTerminalStore {
    async fn cancel_before_begin_ack(
        &self,
        session: SessionId,
        target: crabber_session::session_owner::SessionOwnerTarget,
        detached: bool,
    ) -> Result<(), crabber_session::session_owner::SessionOwnerError> {
        use crabber_session::session_owner::{
            RequestSessionOwnerCancel, SessionOwnerError, SessionOwnerResolver,
        };
        let mode = if detached { 2 } else { 1 };
        if self
            .gate
            .session_owner_cancel_on_begin
            .compare_exchange(mode, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        let expected_revision = if self
            .inner
            .get_session(&session)
            .await
            .map_err(|_| SessionOwnerError::UnknownStoreOutcome)?
            .is_some()
        {
            Some(
                self.inner
                    .session_revision(&session)
                    .await
                    .map_err(|_| SessionOwnerError::UnknownStoreOutcome)?,
            )
        } else {
            None
        };
        self.inner
            .request_session_owner_cancel(RequestSessionOwnerCancel {
                id: crabber_core::SessionOwnerOperationId::new(),
                session_id: session,
                target,
                expected_revision,
                claim: SessionOwnerResolver {
                    resolver_id: "test controller".into(),
                    authority_claim: "cancel before Begin ACK".into(),
                },
            })
            .await?;
        Ok(())
    }
}
