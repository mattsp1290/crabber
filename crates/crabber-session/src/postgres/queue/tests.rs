#[tokio::test]
async fn queue_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::queue_contract::run(&store, clock).await;
        crate::queue_contract::domains_archive_and_reserved_keys(&store).await;
        crate::queue_contract::lazy_empty_scope(&store).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn queue_faults() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::queue_contract::faults(&store, |point| {
            store
                .queue_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn queue_bounds() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::queue_contract::bounds(&store).await;
        crate::queue_contract::byte_bound(&store).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn owned_queue_original_publication_inspection() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        crate::queue_contract::publications::owned_inspection(&store).await;
    })
    .await
    .unwrap();
}
