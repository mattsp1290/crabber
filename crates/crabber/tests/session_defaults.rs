//! Managed provider/model defaults are captured once per ordinary admission.
#[path = "session_defaults/contexts.rs"]
mod contexts;
#[path = "session_defaults/contract.rs"]
mod contract;
#[path = "session_defaults/fixture.rs"]
mod fixture;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[path = "session_defaults/process.rs"]
mod process;
#[path = "session_defaults/races.rs"]
mod races;
#[path = "session_defaults/recovery.rs"]
mod recovery;
#[path = "session_defaults/resume.rs"]
mod resume;
#[path = "session_defaults/store.rs"]
mod store;
use crabber::session::MemoryStore;
use std::sync::Arc;
#[tokio::test]
async fn memory_default_selection_contract() {
    Box::pin(contract::run(Arc::new(MemoryStore::new()))).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_default_selection_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("defaults.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let native = crabber::session::SqliteStore::connect(&path).await.unwrap();
    let native = Arc::new(native);
    Box::pin(contract::run(native.clone())).await;
    process::run(native, crabber_test_support::ChildDatabase::Sqlite(&path)).await;
}
#[cfg(feature = "postgres")]
#[test]
fn postgres_default_selection_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_with_runtime(
        DatabaseMode::Current,
        tokio::runtime::Builder::new_current_thread(),
        async move |url| {
            crabber::session::PostgresStore::migrate(&url)
                .await
                .unwrap();
            let native = crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap();
            let native = Arc::new(native);
            Box::pin(contract::run(native.clone())).await;
            process::run(native, crabber_test_support::ChildDatabase::Postgres(&url)).await;
        },
    )
    .unwrap();
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[tokio::test]
async fn default_selection_process_helper() {
    process::child().await;
}
