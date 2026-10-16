use std::sync::Arc;
#[tokio::test]
async fn sql_initial_driver_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let store = Arc::new(store);
    crate::driver_contract::run(store.clone()).await;
    let fault = store.session_owner_fault.clone();
    crate::driver_contract::faults(store.clone(), move |value| {
        fault.store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    protection::run(store.as_ref()).await;
    let original = crate::driver_contract::concurrent(store.clone()).await;
    let cold = crate::PostgresStore::connect(&url).await.unwrap();
    assert!(matches!(
        crate::Store::register_execution_driver(&cold, original)
            .await
            .unwrap(),
        crate::session_owner::driver::ExecutionDriverOutcome::Replayed(_)
    ));
}

mod protection;
