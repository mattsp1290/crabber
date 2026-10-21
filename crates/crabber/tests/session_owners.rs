//! Public Store capability and pre-session ownership contracts on all adapters.
#[tokio::test]
async fn memory_session_owners() {
    crabber::session::session_owner_contract::run_contract(|clock| {
        crabber::session::MemoryStore::with_clock(clock)
    })
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_session_owners() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owners.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber::session::SqliteStore::connect(&path).await.unwrap();
    crabber::session::session_owner_contract::run_contract(|clock| store.with_clock(clock)).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_session_owners() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let store = crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap();
        crabber::session::session_owner_contract::run_contract(|clock| store.with_clock(clock))
            .await;
    })
    .await
    .unwrap();
}
