//! Run the actual public Runtime scoped acquisition/worker/resource path on SQL.
use super::*;

async fn journey(store: Arc<dyn Store>) {
    let h = Harness::on_store(store.clone(), store, Arc::new(SettlementGate::default()));
    let crate::Admission::Started { handle, receipt } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh scoped SQL admission")
    };
    let descriptor = h.descriptor().await;
    let original = h
        .memory
        .execution_binding(&receipt.session_id, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crabber_session::session_owner::preparation::resource_contract(
            &original.capsule.runtime_semantics
        )
        .unwrap()
        .is_some()
    );
    handle.done().await.unwrap();
    h.finish_cleanup(&descriptor).await;
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert!(
        matches!(h.runtime.start_keyed(h.input.clone(), options()).await.unwrap(), crate::Admission::Replayed(saved) if saved == receipt)
    );
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_scoped_factory_and_actual_cleanup_journey() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owned-scope.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber_session::SqliteStore::connect(&path).await.unwrap();
    journey(Arc::new(store)).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_scoped_factory_and_actual_cleanup_journey() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "required PostgreSQL test URL missing"
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    let store = crabber_session::PostgresStore::connect(&url).await.unwrap();
    journey(Arc::new(store)).await;
}
