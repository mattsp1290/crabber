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
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none());
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    let store = crabber::session::PostgresStore::connect(&url)
        .await
        .unwrap();
    crabber::session::session_owner_contract::run_contract(|clock| store.with_clock(clock)).await;
}
