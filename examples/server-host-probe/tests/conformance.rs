//! An out-of-crate custom adapter runs the actual published transactional suites.
use crabber::{
    core::ManualClock,
    session::{
        Store, attempt_contract, control_contract, interaction_contract, phase_contract,
        public_state_contract, revision_contract, session_management_contract,
        typed_admission_contract,
    },
};
use server_host_probe::store::AuditedStore;
use std::sync::Arc;

async fn contracts(factory: impl Fn(Arc<ManualClock>) -> Arc<dyn Store>) {
    let management = AuditedStore::new(factory(Arc::new(ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ))));
    session_management_contract::run_contract(&management).await;
    assert!(management.calls() > 0);
    control_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
    interaction_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
    attempt_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
    phase_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
    revision_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
    public_state_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
    typed_admission_contract::run_contract(|clock| AuditedStore::new(factory(clock))).await;
}
#[tokio::test]
async fn memory_custom_adapter_conformance() {
    contracts(|clock| Arc::new(crabber::session::MemoryStore::with_clock(clock))).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_custom_adapter_conformance() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("conformance.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber::session::SqliteStore::connect(path).await.unwrap();
    contracts(|clock| Arc::new(store.clone().with_clock(clock))).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_custom_adapter_conformance() {
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
    contracts(|clock| Arc::new(store.clone().with_clock(clock))).await;
}
