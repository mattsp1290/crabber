//! A selected native call obeys the raw-history allowance before private decode.
use super::*;
async fn journey(store: Arc<dyn Store>) {
    let mut source_call = call(&format!("large-fork-{}", SessionId::new().0));
    for delta in &mut source_call {
        if let StreamDelta::ToolCallArgsDelta { text, .. } = delta {
            *text = json!({"payload":"x".repeat(1_048_576)}).to_string();
        }
    }
    let provider = FakeProvider::scripted(vec![source_call, done()]);
    let effects = Arc::new(AtomicUsize::new(0));
    let agent = worker(store.clone(), provider.clone(), effects.clone());
    let source = SessionId::new();
    let execution = context(&source.0, "fork-workspace", "/fork-workspace", "scripted");
    let Admission::Started { handle, .. } = agent
        .prompt_input_keyed_in(
            &execution,
            InputEnvelope::text("large source", SessionRevision::INITIAL).unwrap(),
            AdmissionOptions {
                key: AdmissionKey::new("large-input").unwrap(),
                fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
                behavior_fingerprint: agent.behavior_fingerprint().clone(),
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("source")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let messages = store.list_all_messages(&source).await.unwrap();
    let prefix_bytes: usize = messages
        .iter()
        .map(|message| serde_json::to_vec(message).unwrap().len())
        .sum();
    let mut request = fork_request(&agent, store.as_ref(), &source).await;
    request.limits.encoded_bytes = prefix_bytes + 128;
    assert!(matches!(
        agent.fork_session(request.clone()).await.unwrap(),
        ForkOutcome::Limited(crabber::session::SnapshotLimit::EncodedBytes)
    ));
    assert!(
        store
            .get_session(&request.target_session_id)
            .await
            .unwrap()
            .is_none()
    );
    request.limits.encoded_bytes = 4_194_304;
    assert!(
        matches!(agent.fork_session(request).await.unwrap(),ForkOutcome::Applied(receipt) if receipt.imported_calls==1)
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn memory_native_call_raw_budget() {
    Box::pin(journey(Arc::new(MemoryStore::new()))).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_native_call_raw_budget() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    Box::pin(journey(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    )))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_native_call_raw_budget() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none());
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    Box::pin(journey(Arc::new(
        crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap(),
    )))
    .await;
}
