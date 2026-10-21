//! Actual Agent dispatch, binding, defaults and unknown-ACK qualification.
#[path = "future_queue/contract.rs"]
mod contract;
#[path = "session_defaults/fixture.rs"]
mod fixture;
#[path = "session_defaults/store.rs"]
mod store;
use std::sync::Arc;
fn clock() -> Arc<crabber::core::ManualClock> {
    Arc::new(crabber::core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ))
}
#[tokio::test]
async fn memory_queue_dispatch() {
    let clock = clock();
    Box::pin(contract::run(
        Arc::new(crabber::session::MemoryStore::with_clock(clock.clone())),
        clock,
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_queue_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queue.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let clock = clock();
    Box::pin(contract::run(
        Arc::new(
            crabber::session::SqliteStore::connect(path)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        ),
        clock,
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[test]
fn postgres_queue_dispatch() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_with_runtime(
        DatabaseMode::Current,
        tokio::runtime::Builder::new_current_thread(),
        async move |url| {
            crabber::session::PostgresStore::migrate(&url)
                .await
                .unwrap();
            let clock = clock();
            Box::pin(contract::run(
                Arc::new(
                    crabber::session::PostgresStore::connect(&url)
                        .await
                        .unwrap()
                        .with_clock(clock.clone()),
                ),
                clock,
            ))
            .await;
        },
    )
    .unwrap();
}
