use crate::Store;
#[tokio::test]
async fn quiet_deletion_run() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        crate::deletion::coordinator_contract::run(&store, async |request| {
            store.delete_session(request).await
        })
        .await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn quiet_deletion_faults() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
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
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn quiet_deletion_races() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        crate::deletion::coordinator_contract::races(&store, async |request| {
            store.delete_session(request).await
        })
        .await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn quiet_deletion_mutation_races() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        crate::deletion::coordinator_contract::mutation_races(&store, async |request| {
            store.delete_session(request).await
        })
        .await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn quiet_deletion_forks() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        crate::deletion::coordinator_contract::forks(&store, async |request| {
            store.delete_session(request).await
        })
        .await;
    })
    .await
    .unwrap();
}

mod process_tests;
