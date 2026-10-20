use crate::Store;
#[tokio::test]
async fn quiet_deletion_run() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::deletion::coordinator_contract::run(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_faults() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::deletion::coordinator_contract::races(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_mutation_races() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::deletion::coordinator_contract::mutation_races(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}
#[tokio::test]
async fn quiet_deletion_forks() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::deletion::coordinator_contract::forks(&store, async |request| {
        store.delete_session(request).await
    })
    .await;
}

mod process_tests;
