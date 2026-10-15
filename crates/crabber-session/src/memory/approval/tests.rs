#[tokio::test]
async fn control_faults_keep_attempt_barrier_answers_and_receipts_atomic() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::approval_contract::run_fault_contract(&store, &clock, |point| {
        store
            .approval_fault
            .store(point as u8, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}
