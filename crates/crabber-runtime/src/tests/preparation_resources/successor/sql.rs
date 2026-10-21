//! Run the actual Runtime bridge, retained observations and cold retry on SQL.
use super::*;
async fn all(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    contract(store.clone(), clock.clone()).await;
    retention::contract(store.clone(), clock.clone()).await;
    recovery::contract(store.clone(), clock.clone()).await;
    pending::contract(store, clock).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn sqlite_runtime_successor_complete_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("successor-lifecycle.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crabber_session::SqliteStore::connect(path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    all(Arc::new(store), clock).await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn postgres_runtime_successor_complete_lifecycle() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = crabber_session::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        all(Arc::new(store), clock).await;
    })
    .await
    .unwrap();
}
