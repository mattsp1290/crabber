use super::*;
async fn altered_grant(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    recovery: bool,
    fault: usize,
) {
    let (h, original, effects) = fixture::paused(store, clock.clone(), frontend, false).await;
    let choice = if recovery {
        h.gate.preparation_fault.store(2, Ordering::SeqCst);
        assert!(original.invoke(&h.runtime).await.is_err());
        let desc = original.descriptor(&h).await;
        h.runtime.cleanup_retained_preparation(&desc).await.unwrap();
        h.finish_cleanup_state(&desc, true).await;
        clock.set(clock.now() + time::Duration::seconds(31));
        original.recovery(&h).await
    } else {
        original
    };
    h.gate
        .initial_task_diagnostics
        .store(true, Ordering::SeqCst);
    h.gate.initial_task_ids.lock().unwrap().clear();
    h.gate.preparation_fault.store(fault, Ordering::SeqCst);
    let result = choice.invoke(&h.runtime).await;
    let accepted = result.is_ok();
    if let Ok(crate::ToolContinuation::Started { handle, .. }) = result {
        handle.done().await.unwrap();
    }
    let resumed = effects.resumes.load(Ordering::SeqCst);
    let protected = h.gate.grant_payload_protections.load(Ordering::SeqCst);
    let registered = h.gate.successor_request.lock().unwrap().is_some();
    let began = h
        .gate
        .initial_task_ids
        .lock()
        .unwrap()
        .iter()
        .any(|(stage, _)| *stage == InitialTaskStage::BeginAttempt);
    let desc = choice.descriptor(&h).await;
    if accepted {
        h.finish_cleanup(&desc).await;
    } else {
        h.runtime.cleanup_retained_preparation(&desc).await.unwrap();
        h.finish_cleanup_state(&desc, true).await;
    }
    assert_eq!(
        resumed, 0,
        "changed public ACK payload reached resumable tool"
    );
    assert_eq!(
        protected, 0,
        "changed public ACK payload reached result protection"
    );
    assert!(
        !registered,
        "changed public ACK payload registered a successor"
    );
    assert!(!began, "changed public ACK payload began native work");
    assert!(!accepted, "changed public ACK payload accepted");
}
async fn contract(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    faults: &[usize],
) {
    for recovery in [false, true] {
        for &fault in faults {
            Box::pin(altered_grant(
                store.clone(),
                clock.clone(),
                frontend,
                recovery,
                fault,
            ))
            .await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_pending_tool_grant_payload_is_authenticated() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(MemoryStore::with_clock(clock.clone())),
        clock,
        false,
        &[8, 10, 11, 12],
    ))
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_pending_frontend_grant_payload_is_authenticated() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(MemoryStore::with_clock(clock.clone())),
        clock,
        true,
        &[8, 10, 11, 12],
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_pending_tool_and_frontend_grant_payload_is_authenticated() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("payload.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(
        crabber_session::SqliteStore::connect(path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    for frontend in [false, true] {
        Box::pin(contract(store.clone(), clock.clone(), frontend, &[8])).await;
    }
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_pending_tool_and_frontend_grant_payload_is_authenticated() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = Arc::new(
            crabber_session::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        );
        for frontend in [false, true] {
            Box::pin(contract(store.clone(), clock.clone(), frontend, &[8])).await;
        }
    })
    .await
    .unwrap();
}
