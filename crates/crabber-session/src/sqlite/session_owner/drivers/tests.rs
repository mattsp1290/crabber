use std::sync::Arc;
#[tokio::test]
async fn sql_initial_driver_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drivers.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let store = Arc::new(store);
    crate::driver_contract::run(store.clone()).await;
    let fault = store.session_owner_fault.clone();
    crate::driver_contract::faults(store.clone(), move |value| {
        fault.store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    protection::run(store.as_ref()).await;
    let original = crate::driver_contract::concurrent(store.clone()).await;
    let cold = crate::SqliteStore::connect(&path).await.unwrap();
    assert!(matches!(
        crate::Store::register_execution_driver(&cold, original)
            .await
            .unwrap(),
        crate::session_owner::driver::ExecutionDriverOutcome::Replayed(_)
    ));
}
#[tokio::test]
async fn sql_unkeyed_driver_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drivers.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let store = Arc::new(store);
    crate::driver_contract::run_unkeyed(store.clone()).await;
    let fault = store.session_owner_fault.clone();
    crate::driver_contract::faults_unkeyed(store.clone(), move |value| {
        fault.store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    protection::unkeyed_contract(store.as_ref()).await;
    let original = crate::driver_contract::concurrent_unkeyed(store.clone()).await;
    let cold = crate::SqliteStore::connect(&path).await.unwrap();
    assert!(matches!(
        crate::Store::register_execution_driver(&cold, original)
            .await
            .unwrap(),
        crate::session_owner::driver::ExecutionDriverOutcome::Replayed(_)
    ));
}

mod protection;

mod source_budget;

mod publications;
