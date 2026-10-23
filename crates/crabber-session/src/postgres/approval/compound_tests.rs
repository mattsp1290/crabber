#[tokio::test]
async fn whole_batch_approval_and_single_continuation_owner() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url).await.unwrap();
        crate::approval_contract::run_contract(|clock| store.with_clock(clock)).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn suspend_answer_and_claim_faults_are_atomic() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url).await.unwrap();
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
    })
    .await
    .unwrap();
}
