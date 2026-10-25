//! Exact durable controls across controllers, closed hosts and worker boundaries.
#[path = "server_host/support.rs"]
pub mod support;
pub use support::*;
#[path = "server_host/attempt_control.rs"]
mod attempt_control;
#[path = "run_controls/contract.rs"]
mod contract;
#[path = "run_controls/faults.rs"]
mod faults;
#[path = "run_controls/pause.rs"]
mod pause;
#[path = "run_controls/provider.rs"]
mod provider;
#[path = "session_defaults/store.rs"]
mod store;
#[path = "run_controls/tools.rs"]
mod tools;
#[tokio::test]
async fn memory_durable_controls() {
    contract::run(Arc::new(crabber::session::MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_durable_controls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("controls.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    contract::run(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_durable_controls() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        contract::run(Arc::new(
            crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}

#[path = "run_controls/unstarted.rs"]
mod unstarted;
#[tokio::test(flavor = "current_thread")]
async fn memory_cancel_before_first_attempt() {
    unstarted::cancel_before_first_attempt(Arc::new(crabber::session::MemoryStore::new())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_cancel_before_first_attempt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unstarted.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    unstarted::cancel_before_first_attempt(Arc::new(
        crabber::session::SqliteStore::connect(path).await.unwrap(),
    ))
    .await;
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_cancel_before_first_attempt() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    TestDatabase::run_with_runtime(
        DatabaseMode::Current,
        tokio::runtime::Builder::new_current_thread(),
        async move |url| {
            crabber::session::PostgresStore::migrate(&url)
                .await
                .unwrap();
            unstarted::cancel_before_first_attempt(Arc::new(
                crabber::session::PostgresStore::connect(&url)
                    .await
                    .unwrap(),
            ))
            .await;
        },
    )
    .unwrap();
}
