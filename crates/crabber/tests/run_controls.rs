//! Actual Agent controls cross controller boundaries and enter provider context once.
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
use std::sync::Arc;
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
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none());
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    contract::run(Arc::new(
        crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap(),
    ))
    .await;
}
