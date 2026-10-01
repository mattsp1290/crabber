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

#[path = "../../../examples/host-trace/src/durable.rs"]
mod durable;

#[tokio::test]
async fn durable_memory_paused_resume_and_duplicate_delivery() {
    let dir = std::env::temp_dir().join(format!("crabber-trace-{}", SessionId::new()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("queue.json");
    durable::enqueue(&path, &durable::Envelope::demo());
    durable::memory(&path).await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn predecessor_is_validated_bounded_and_requires_a_new_numeric_trace() {
    let first = context("0000000000000001");
    assert!(
        context("00000000000000000000000000000001")
            .linked_to(&first)
            .is_err()
    );
    let second = context("0000000000000002").linked_to(&first).unwrap();
    let third = context("0000000000000003").linked_to(&second).unwrap();
    assert_eq!(third.predecessor().unwrap().trace_id(), second.trace_id());
    let serialized = serde_json::to_string(&third).unwrap();
    assert_eq!(serialized.matches("predecessor").count(), 1);
    assert_eq!(
        serde_json::from_str::<TraceContext>(&serialized).unwrap(),
        third
    );
    assert!(!format!("{third:?} {:?}", third.predecessor()).contains("000000000000000"));
    for invalid in [
        r#"{"trace_id":"0000000000000003","span_id":"1234567890abcdef","predecessor":{"trace_id":"0000000000000000","span_id":"1234567890abcdef"}}"#,
        r#"{"trace_id":"0000000000000003","span_id":"1234567890abcdef","predecessor":{"trace_id":"0000000000000002","span_id":"1234567890abcdef","predecessor":"secret"}}"#,
        r#"{"trace_id":"0000000000000003","span_id":"1234567890abcdef","predecessor":{"trace_id":"0000000000000002","trace_id":"0000000000000001","span_id":"1234567890abcdef"}}"#,
        r#"{"trace_id":"0000000000000003","span_id":"1234567890abcdef","predecessor":{"trace_id":"0000000000000002","span_id":"1234567890abcdef"},"predecessor":null}"#,
        r#"{"trace_id":"0000000000000003","span_id":"1234567890abcdef","predecessor":{"trace_id":"0000000000000003","span_id":"1234567890abcdef"}}"#,
    ] {
        let error = serde_json::from_str::<TraceContext>(invalid).unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
}

#[cfg(feature = "postgres")]
mod durable_process {
    use super::*;
    use crabber::session::PostgresStore;
    use std::{path::Path, process::Command};

    async fn spawn(path: &Path, mode: &str) {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "durable_process::worker_child", "--nocapture"])
            .env("CRABBER_TRACE_WORKER", mode)
            .env("CRABBER_TRACE_QUEUE", path)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if std::time::Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("worker timeout");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    #[tokio::test]
    async fn worker_child() {
        let Ok(mode) = std::env::var("CRABBER_TRACE_WORKER") else {
            return;
        };
        let url = std::env::var("CRABBER_TEST_POSTGRES_URL").unwrap();
        let store = Arc::new(PostgresStore::connect(&url).await.unwrap());
        let path = std::path::PathBuf::from(std::env::var_os("CRABBER_TRACE_QUEUE").unwrap());
        durable::worker(store, &path, &mode, "postgres").await;
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .unwrap();
        assert!(output.status.success());
        println!(
            "worker_source={}",
            String::from_utf8(output.stdout).unwrap().trim()
        );
    }
    #[tokio::test]
    async fn postgres_fresh_workers_preserve_schema3_receipts_sessions_and_cursors() {
        let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
            assert!(
                std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() != Ok("1"),
                "CRABBER_TEST_POSTGRES_URL required"
            );
            return;
        };
        PostgresStore::migrate(&url).await.unwrap();
        let store = PostgresStore::connect(&url).await.unwrap();
        let envelope = durable::Envelope::demo();
        let dir = std::env::temp_dir().join(format!("crabber-trace-{}", envelope.session));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("queue.json");
        durable::enqueue(&path, &envelope);
        assert!(
            store
                .get_session(&envelope.session)
                .await
                .unwrap()
                .is_none(),
            "queue admission precedes Crabber admission"
        );
        spawn(&path, "pause").await;
        let receipt = store
            .lookup_admission(&envelope.session, &envelope.key)
            .await
            .unwrap()
            .unwrap();
        let old_messages = store.list_all_messages(&envelope.session).await.unwrap();
        let old_events = store
            .list_events(&envelope.session, None, 100)
            .await
            .unwrap();
        let session = store.get_session(&envelope.session).await.unwrap().unwrap();
        spawn(&path, "resume").await;
        // An already issued schema3 continuation survives a completely fresh
        // worker pool and keyed replay, with its original inclusive event cursor.
        let mut request = crabber::SnapshotRequest {
            session_id: envelope.session.clone(),
            continuation: None,
            limits: crabber::SnapshotLimits {
                messages: 1,
                tool_calls: 10,
                parts: 100,
                text_bytes: 10_000,
                encoded_bytes: 100_000,
            },
        };
        let crabber::SnapshotOutcome::Page(first) = store.snapshot(request.clone()).await.unwrap()
        else {
            panic!("snapshot page required");
        };
        assert!(first.continuation.is_some());
        request.continuation = first.continuation;
        let expected = store.snapshot(request.clone()).await.unwrap();
        std::fs::write(
            dir.join("snapshot.json"),
            serde_json::to_vec(&serde_json::json!({"request":request,"outcome":expected})).unwrap(),
        )
        .unwrap();
        let journal = std::fs::read(dir.join("attempt.json")).unwrap();
        // Retry the original queue delivery mode, not only a special replay mode.
        spawn(&path, "pause").await;
        spawn(&path, "duplicate").await;
        assert_eq!(std::fs::read(dir.join("attempt.json")).unwrap(), journal);
        assert_eq!(
            store
                .lookup_admission(&envelope.session, &envelope.key)
                .await
                .unwrap(),
            Some(receipt.clone())
        );
        let messages = store.list_all_messages(&envelope.session).await.unwrap();
        assert!(old_messages.iter().all(|old| messages.contains(old)));
        assert_eq!(
            messages
                .iter()
                .filter(|m| m.role == crabber::core::Role::User)
                .count(),
            1
        );
        let events = store
            .list_events(&envelope.session, None, 100)
            .await
            .unwrap();
        assert_eq!(&events[..old_events.len()], old_events.as_slice());
        assert_eq!(
            store
                .get_session(&envelope.session)
                .await
                .unwrap()
                .unwrap()
                .id,
            session.id
        );
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let version: i32 = sqlx::query_scalar("SELECT max(version) FROM schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 3);
        pool.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}
