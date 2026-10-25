//! Existing private fixture/suite, preserving actual native cuts and joins.
use super::*;

#[tokio::test]
async fn unsafe_summary_finish_reason_never_installs_compaction_history() {
    use crabber_core::ModelFinishReason;
    for reason in [
        ModelFinishReason::Length,
        ModelFinishReason::ContentFilter,
        ModelFinishReason::Other,
    ] {
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![
            vec![StreamDelta::Error(ProviderError {
                kind: crabber_providers::ProviderErrorKind::ContextOverflow,
                message: "full".into(),
                retryable: false,
            })],
            vec![
                StreamDelta::TextDelta("incomplete summary".into()),
                StreamDelta::FinishReason(reason),
                StreamDelta::Usage(Usage {
                    input_tokens: 2,
                    output_tokens: 40,
                }),
                StreamDelta::Completed,
            ],
            text_script("must not retry"),
        ]);
        let runtime = orchestrator(
            store.clone(),
            fake.clone(),
            Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
            Arc::new(RecordingObserver::default()),
        );
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let error = handle.done().await.unwrap_err();
        match reason {
            ModelFinishReason::Length => assert!(matches!(
                error,
                RuntimeError::OutputLimit { tool_call_count: 0 }
            )),
            _ => assert!(
                matches!(error, RuntimeError::ModelResponseRejected(actual) if actual == reason)
            ),
        }
        assert_eq!(fake.requests().len(), 2);
        assert!(
            !store
                .list_messages(&session, None)
                .await
                .unwrap()
                .iter()
                .flat_map(|message| &message.parts)
                .any(|part| part.kind == crabber_core::PartKind::CompactionSummary)
        );
        assert!(
            !store
                .list_events(&session, None, 100)
                .await
                .unwrap()
                .iter()
                .any(|event| event.kind == EventKind::ContextEpochFinished)
        );
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
