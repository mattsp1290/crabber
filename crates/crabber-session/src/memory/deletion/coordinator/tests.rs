use crate::Store;
#[tokio::test]
async fn quiet_deletion_run() {
    let store = crate::MemoryStore::new();
    crate::deletion::coordinator_contract::run(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_faults() {
    let store = crate::MemoryStore::new();
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
    let store = crate::MemoryStore::new();
    crate::deletion::coordinator_contract::races(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_mutation_races() {
    let store = crate::MemoryStore::new();
    crate::deletion::coordinator_contract::mutation_races(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[tokio::test]
async fn quiet_deletion_forks() {
    let store = crate::MemoryStore::new();
    crate::deletion::coordinator_contract::forks(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
