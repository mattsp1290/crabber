use super::*;

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
