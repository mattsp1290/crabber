use crate::Store;
#[tokio::test]
async fn quiet_deletion_run() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordinator.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    crate::deletion::coordinator_contract::run(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_faults() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordinator.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    crate::deletion::coordinator_contract::faults(
        &store,
        async |request| store.delete_session(request).await,
        |point| {
            store
                .deletion_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
}
#[tokio::test]
async fn quiet_deletion_races() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordinator.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    crate::deletion::coordinator_contract::races(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_mutation_races() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordinator.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    crate::deletion::coordinator_contract::mutation_races(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_forks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordinator.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    crate::deletion::coordinator_contract::forks(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}

mod process_tests;
