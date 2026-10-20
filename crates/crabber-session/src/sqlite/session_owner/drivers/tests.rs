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

#[tokio::test]
async fn successor_driver_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("successor.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = Arc::new(
        crate::SqliteStore::connect(&path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    crate::successor_driver_contract::run(store.clone(), &clock).await;
    let fault = store.session_owner_fault.clone();
    crate::successor_driver_contract::faults(store.clone(), &clock, move |value| {
        fault.store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    successor_budget::run(store.as_ref(), &clock).await;
}

mod successor_budget;

mod successor_process;

#[tokio::test]
async fn approval_successor_driver_contract() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("approval-successors.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let store = Arc::new(store);
    Box::pin(crate::successor_driver_contract::approval::run(
        store.as_ref(),
        &clock,
    ))
    .await;
    Box::pin(super::successor::approval_tests::run(
        store.as_ref(),
        &clock,
    ))
    .await;
    let fault = store.session_owner_fault.clone();
    Box::pin(crate::successor_driver_contract::approval::faults(
        store,
        &clock,
        move |value| {
            fault.store(value, std::sync::atomic::Ordering::SeqCst);
        },
    ))
    .await;
}
