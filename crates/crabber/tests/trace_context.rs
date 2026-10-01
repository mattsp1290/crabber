#[path = "../../../examples/host-trace/src/journey.rs"]
mod journey;

use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, EventRecord, FakeProvider,
    InputFingerprint, Observer, Selection, SessionId, StreamDelta, TraceContext,
    session::{MemoryStore, Store},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn context(trace: &str) -> TraceContext {
    TraceContext::new(trace, "1234567890abcdef").unwrap()
}
fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("trace-key").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
fn build(capture: Arc<journey::Capture>, store: Arc<MemoryStore>) -> Agent {
    Agent::builder()
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(vec![
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ];
            3
        ])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .observer(capture)
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_observes_model_parallel_tools_and_lifecycle_without_global_subscriber() {
    tokio::time::timeout(std::time::Duration::from_secs(5), journey::journey())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_contexts_and_context_free_execution_are_isolated() {
    let capture = Arc::new(journey::Capture::default());
    let agent = Arc::new(build(capture.clone(), Arc::new(MemoryStore::new())));
    let contexts = [
        Some(context("1234567890abcdef")),
        Some(context("1234567890abcdef9876543210abcdef")),
        None,
    ];
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for context in &contexts {
        let agent = agent.clone();
        let context = context.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let run = agent
                .prompt_with_context(None, "same prompt", context.clone())
                .await
                .unwrap();
            (run.done().await.unwrap().run_id, context)
        }));
    }
    let mut identities = Vec::new();
    for task in tasks {
        identities.push(task.await.unwrap());
    }
    for (run, context) in identities {
        let events = capture.events.lock().unwrap();
        let models = capture.models.lock().unwrap();
        assert!(events.iter().any(|(event, _)| event.run_id == run));
        assert_eq!(
            models
                .iter()
                .filter(|(event, _)| event.run_id == run)
                .count(),
            1
        );
        for (event, observed) in events
            .iter()
            .chain(models.iter())
            .filter(|(event, _)| event.run_id == run)
        {
            assert_eq!(observed, &context, "wrong context for {}", event.run_id);
        }
    }
}

#[tokio::test]
async fn replay_ignores_replacement_context_without_receipt_or_execution_mutation() {
    let capture = Arc::new(journey::Capture::default());
    let store = Arc::new(MemoryStore::new());
    let agent = build(capture.clone(), store.clone());
    let session = SessionId::new();
    let original = context("1234567890abcdef");
    let started = agent
        .prompt_keyed_with_context(
            session.clone(),
            "same prompt",
            options(),
            Some(original.clone()),
        )
        .await
        .unwrap();
    let receipt = started.receipt().clone();
    let Admission::Started { handle, .. } = started else {
        panic!("first admission must start")
    };
    handle.done().await.unwrap();
    let messages = store.list_all_messages(&session).await.unwrap();
    let count = capture.events.lock().unwrap().len();
    let replay = agent
        .prompt_keyed_with_context(
            session.clone(),
            "same prompt",
            options(),
            Some(context("9876543210abcdef9876543210abcdef")),
        )
        .await
        .unwrap();
    assert!(matches!(replay, Admission::Replayed(_)));
    assert_eq!(replay.receipt(), &receipt);
    assert_eq!(
        agent
            .lookup_admission(&session, &options().key)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
    assert_eq!(capture.events.lock().unwrap().len(), count);
    assert_eq!(capture.models.lock().unwrap().len(), 1);
    assert!(
        capture
            .events
            .lock()
            .unwrap()
            .iter()
            .all(|(_, observed)| observed.as_ref() == Some(&original))
    );
}

struct Legacy(AtomicUsize, AtomicUsize);
impl Observer for Legacy {
    fn emit(&self, _: &EventRecord) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn model_completed(&self, _: &EventRecord) {
        self.1.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn legacy_observers_and_multiple_host_observers_receive_each_callback_once() {
    let first = Arc::new(Legacy(AtomicUsize::new(0), AtomicUsize::new(0)));
    let second = Arc::new(Legacy(AtomicUsize::new(0), AtomicUsize::new(0)));
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ]])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .observer(first.clone())
        .observer(second.clone())
        .build()
        .unwrap();
    let mut run = agent
        .prompt_with_context(None, "safe", Some(context("1234567890abcdef")))
        .await
        .unwrap();
    let mut broadcast = run.events();
    let mut count = 0;
    while broadcast.recv().await.unwrap().is_some() {
        count += 1;
    }
    run.done().await.unwrap();
    for observer in [first, second] {
        assert_eq!(observer.0.load(Ordering::SeqCst), count);
        assert_eq!(observer.1.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn malformed_transport_context_is_rejected_before_admission() {
    let capture = Arc::new(journey::Capture::default());
    let store = Arc::new(MemoryStore::new());
    let agent = build(capture.clone(), store.clone());
    let session = SessionId::new();
    for serialized in [
        r#"{"trace_id":"0000000000000000","span_id":"1234567890abcdef"}"#,
        r#"{"trace_id":"1234567890abcdef1234567890abcdef0","span_id":"1234567890abcdef"}"#,
        r#"{"trace_id":"1234567890abcdef","span_id":"secret-sentinel"}"#,
        r#"{"trace_id":"1234567890abcdef","span_id":"1234567890abcdef","secret-field":"secret-sentinel"}"#,
        r#"{"trace_id":"1234567890abcdef","trace_id":"9876543210abcdef","span_id":"1234567890abcdef"}"#,
        r#""secret-sentinel""#,
    ] {
        let parsed = serde_json::from_str::<TraceContext>(serialized);
        match parsed {
            Ok(context) => {
                agent
                    .prompt_keyed_with_context(session.clone(), "safe", options(), Some(context))
                    .await
                    .unwrap();
                panic!("invalid identity admitted")
            }
            Err(error) => {
                assert!(!error.to_string().contains("secret"));
            }
        }
        assert!(store.get_session(&session).await.unwrap().is_none());
        assert!(
            agent
                .lookup_admission(&session, &options().key)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(capture.events.lock().unwrap().is_empty());
    assert!(capture.models.lock().unwrap().is_empty());
}
