#[tokio::test]
async fn whole_batch_approval_and_single_continuation_owner() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    crate::approval_contract::run_contract(|clock| store.with_clock(clock)).await;
}
#[tokio::test]
async fn suspend_answer_and_claim_faults_are_atomic() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    crate::approval_contract::run_fault_contract(
        &store,
        &clock,
        crate::binding_fault::setter(
            &store.approval_fault,
            |point: crate::public_state_contract::FaultPoint| point as u8,
        ),
    )
    .await;
}
