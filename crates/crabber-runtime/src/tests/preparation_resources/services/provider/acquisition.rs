//! Abandoning the response waiter must not detach the actual constructor.
use super::*;
async fn contract(store: Arc<dyn Store>) {
    let h = BorrowHarness::new(&store);
    h.resolver.gate.hold.store(true, Ordering::SeqCst);
    let runtime = h.services.runtime.clone();
    let input = h.services.a.clone();
    let start = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    tokio::time::timeout(Duration::from_secs(10), h.resolver.gate.entered.notified())
        .await
        .unwrap();
    h.resolver.gate.abort.notify_one();
    assert!(start.await.unwrap().is_err());
    assert_eq!(h.resolver.constructors.load(Ordering::SeqCst), 1);
    assert_eq!(h.resolver.gate.finished.load(Ordering::SeqCst), 0);
    assert!(
        h.close(&h.services.a, Duration::from_millis(30))
            .await
            .is_err()
    );
    // The actual acquisition still exists despite the discarded waiter and
    // failed preparation. Only its real completion can release the member.
    h.resolver.gate.release.notify_one();
    h.close(&h.services.a, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(h.resolver.gate.finished.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.services.fake.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    let escaped = h.resolver.contexts.lock().unwrap()[0].clone();
    let source = h.resolver.source.lock().unwrap().clone().unwrap();
    let count = h.resolver.constructors.clone();
    let result = escaped
        .acquire_service_binding(source, move || async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(ProviderBindingError::VersionConflict)
        })
        .await;
    assert!(result.is_err());
    assert_eq!(
        h.resolver.constructors.load(Ordering::SeqCst),
        1,
        "sealed original preparation cannot reopen constructor ingress"
    );
    h.services
        .provider
        .inner
        .close_all(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(h.services.seen.shutdown.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn memory_provider_borrow_caller_drop_retains_actual_constructor() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_provider_borrow_caller_drop_retains_actual_constructor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider-acquisition.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_provider_borrow_caller_drop_retains_actual_constructor() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    contract(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
