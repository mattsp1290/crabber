//! A store panic cannot detach an already dispatched dependency or its children.
use super::*;
async fn contract(store: Arc<dyn Store>) {
    let (h, gate) = faults::harness(store.clone());
    h.seen.resource_wait.store(true, Ordering::SeqCst);
    faults::started(&h).await;
    let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
    gate.service_access_panic.store(true, Ordering::SeqCst);
    let mut later = options();
    later.key = AdmissionKey::new("panic-service-access").unwrap();
    assert!(h.runtime.start_keyed(h.a.clone(), later).await.is_err());
    assert_eq!(h.seen.created.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 1);
    assert!(
        h.provider
            .inner
            .close_session(&faults::context(&h), Duration::from_millis(100))
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.seen.resource_started.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 0);
    assert_eq!(gate.service_join_calls.load(Ordering::SeqCst), 0);
    states(
        &*store,
        h.a.session_id.as_ref().unwrap(),
        &owned,
        SessionOwnerState::Active,
    )
    .await;
    // The first bounded close was cancelled while real children were blocked.
    // Their retained handles, and the original task, must survive that timeout.
    h.seen.resource_release.add_permits(2);
    h.provider
        .inner
        .close_session(&faults::context(&h), Duration::from_secs(3))
        .await
        .unwrap();
    states(
        &*store,
        h.a.session_id.as_ref().unwrap(),
        &owned,
        SessionOwnerState::Joined,
    )
    .await;
    assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 2);
    assert_eq!(h.seen.created.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 1);
    h.provider
        .inner
        .close_all(Duration::from_secs(2))
        .await
        .unwrap();
}
#[tokio::test]
async fn memory_store_panic_keeps_actual_service_scope() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_store_panic_keeps_actual_service_scope() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("service-ack.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_store_panic_keeps_actual_service_scope() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        contract(Arc::new(
            crabber_session::PostgresStore::connect(&url).await.unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}
