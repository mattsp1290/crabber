use super::*;
use crabber_core::{ModelFinishReason, event_payload::ModelResponseCompleted};

async fn completion_events(store: &MemoryStore, session: &SessionId) -> Vec<EventRecord> {
    store
        .list_events(session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::ModelResponseCompleted)
        .collect()
}

#[tokio::test]
async fn reasoning_only_completion_has_durable_safe_metadata() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![vec![
        StreamDelta::ReasoningDelta("PRIVATE_REASONING_SENTINEL".into()),
        StreamDelta::Usage(Usage {
            input_tokens: 4,
            output_tokens: 32,
        }),
        StreamDelta::Completed,
    ]]);
    let runtime = orchestrator(
        store.clone(),
        fake.clone(),
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(fake.requests().len(), 1);
    let events = store.list_events(&session, None, 100).await.unwrap();
    let event = events
        .iter()
        .find(|event| serde_json::to_value(&event.kind).unwrap() == "model_response_completed")
        .expect("durable model completion metadata");
    assert!(!event.live_only);
    assert!(event.turn_id.is_some());
    assert_eq!(
        event.payload,
        json!({
            "finish_reason": null, "usage":{"input_tokens":4,"output_tokens":32},
            "has_visible_text":false,"tool_call_count":0,"stream_completed":true
        })
    );
    assert!(
        !serde_json::to_string(&event)
            .unwrap()
            .contains("PRIVATE_REASONING_SENTINEL")
    );
}

#[tokio::test]
async fn unsafe_finish_rejects_valid_tool_arguments_before_commit_prepare_or_execution() {
    for reason in [
        ModelFinishReason::Length,
        ModelFinishReason::ContentFilter,
        ModelFinishReason::Other,
    ] {
        for arguments_completed in [false, true] {
            let store = Arc::new(MemoryStore::new());
            let executed = Arc::new(AtomicUsize::new(0));
            let prepared = Arc::new(AtomicUsize::new(0));
            let mut script = call_script(ToolCallId::new(), r#"{"text":"valid json"}"#);
            script.pop();
            if !arguments_completed {
                script.pop();
            }
            script.extend([
                StreamDelta::FinishReason(reason),
                StreamDelta::Usage(Usage {
                    input_tokens: 7,
                    output_tokens: 64,
                }),
                StreamDelta::Completed,
            ]);
            let fake = FakeProvider::scripted(vec![script, text_script("must not retry")]);
            let runtime = Orchestrator::builder()
                .behavior_version("model-completion.v1")
                .store(store.clone())
                .resolver(Arc::new(fake.clone()))
                .plan_provider(Arc::new(StaticPlanProvider::new(
                    vec![tool(Arc::new(EchoTool(executed.clone())))],
                    vec![],
                )))
                .tool_pipeline(Arc::new(CountingPipeline(prepared.clone())))
                .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
                .build()
                .unwrap();
            let handle = runtime.start(request()).await.unwrap();
            let session = handle.session_id().clone();
            let error = handle.done().await.unwrap_err();
            match reason {
                ModelFinishReason::Length => assert!(matches!(
                    error,
                    RuntimeError::OutputLimit { tool_call_count: 1 }
                )),
                _ => assert!(
                    matches!(error, RuntimeError::ModelResponseRejected(actual) if actual == reason)
                ),
            }
            assert_eq!(executed.load(Ordering::SeqCst), 0);
            assert_eq!(prepared.load(Ordering::SeqCst), 0);
            assert_eq!(fake.requests().len(), 1);
            assert!(
                !store
                    .list_messages(&session, None)
                    .await
                    .unwrap()
                    .iter()
                    .any(|message| message.role == Role::Assistant)
            );
            let events = store.list_events(&session, None, 100).await.unwrap();
            assert!(!events.iter().any(|event| matches!(
                event.kind,
                EventKind::ToolCallPending
                    | EventKind::ToolCallRunning
                    | EventKind::ToolCallSettled
            )));
            let completions = completion_events(&store, &session).await;
            assert_eq!(completions.len(), 1);
            let completion: ModelResponseCompleted =
                serde_json::from_value(completions[0].payload.clone()).unwrap();
            assert_eq!(completion.finish_reason, Some(reason));
            assert_eq!(
                completion.usage,
                Usage {
                    input_tokens: 7,
                    output_tokens: 64
                }
            );
            assert_eq!(completion.tool_call_count, 1);
            assert!(completion.has_visible_text && completion.stream_completed);
        }
    }
}

#[tokio::test]
async fn length_after_private_reasoning_preserves_late_usage_and_reports_no_tool_calls() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![vec![
        StreamDelta::ReasoningDelta("PRIVATE_REASONING_SENTINEL".into()),
        StreamDelta::FinishReason(ModelFinishReason::Length),
        StreamDelta::Usage(Usage {
            input_tokens: 9,
            output_tokens: 128,
        }),
        StreamDelta::Completed,
    ]]);
    let runtime = orchestrator(
        store.clone(),
        fake,
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::OutputLimit { tool_call_count: 0 })
    ));
    let completions = completion_events(&store, &session).await;
    assert_eq!(completions.len(), 1);
    assert_eq!(
        completions[0].payload,
        json!({"finish_reason":"length","usage":{"input_tokens":9,"output_tokens":128},"has_visible_text":false,"tool_call_count":0,"stream_completed":true})
    );
    assert!(
        !serde_json::to_string(&completions)
            .unwrap()
            .contains("PRIVATE_REASONING_SENTINEL")
    );
}

#[tokio::test]
async fn incomplete_transport_never_emits_model_response_completed() {
    let store = Arc::new(MemoryStore::new());
    let runtime = orchestrator(
        store.clone(),
        FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("partial".into()),
            StreamDelta::FinishReason(ModelFinishReason::Stop),
        ]]),
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert!(handle.done().await.is_err());
    assert!(completion_events(&store, &session).await.is_empty());
}

#[tokio::test]
async fn safe_terminal_reasons_preserve_normal_text_and_tool_execution() {
    let store = Arc::new(MemoryStore::new());
    let executed = Arc::new(AtomicUsize::new(0));
    let mut tool_script = call_script(ToolCallId::new(), r#"{"text":"normal"}"#);
    tool_script.insert(
        tool_script.len() - 1,
        StreamDelta::FinishReason(ModelFinishReason::ToolCalls),
    );
    let runtime = orchestrator(
        store.clone(),
        FakeProvider::scripted(vec![
            tool_script,
            vec![
                StreamDelta::TextDelta("answer".into()),
                StreamDelta::FinishReason(ModelFinishReason::Stop),
                StreamDelta::Completed,
            ],
        ]),
        Arc::new(EchoTool(executed.clone())),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    let completions = completion_events(&store, &session).await;
    assert_eq!(completions.len(), 2);
    assert_eq!(completions[0].payload["finish_reason"], "tool_calls");
    assert_eq!(completions[0].payload["tool_call_count"], 1);
    assert_eq!(completions[1].payload["finish_reason"], "stop");
    assert!(contains_text(
        &store.list_messages(&session, None).await.unwrap(),
        "answer"
    ));
}
