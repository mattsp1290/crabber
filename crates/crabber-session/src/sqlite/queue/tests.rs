#[tokio::test]
async fn queue_contract() {
    let (native, _, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = native.with_clock(clock.clone());
    crate::queue_contract::run(&store, clock).await;
    crate::queue_contract::domains_archive_and_reserved_keys(&store).await;
    crate::queue_contract::lazy_empty_scope(&store).await;
}
#[tokio::test]
async fn queue_faults() {
    let (native, _, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = native.with_clock(clock.clone());
    crate::queue_contract::faults(&store, |point| {
        store
            .queue_fault
            .store(point, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}
#[tokio::test]
async fn queue_bounds() {
    let (native, _, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = native.with_clock(clock.clone());
    crate::queue_contract::bounds(&store).await;
    crate::queue_contract::byte_bound(&store).await;
}
