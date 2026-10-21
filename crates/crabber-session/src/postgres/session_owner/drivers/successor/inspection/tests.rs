use super::*;
use crate::successor_driver_contract::approval::pending::{commit_faults, inspected};
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn pending_approval_commit_faults_exact_budget_and_cold_metadata() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        commit_faults(&store, &clock, |value| {
            store.session_owner_fault.store(value, Ordering::SeqCst);
        })
        .await;
        for recovery in [false, true] {
            let request = inspected(&store, &clock, recovery).await;
            let mut tx = crate::postgres::transaction::begin(&store.pool)
                .await
                .unwrap();
            let mut proof = Proof::bounded(32 << 20);
            let result = inspect(&mut tx, &request, &mut proof).await.unwrap();
            assert!(proof.used() > 1000);
            assert!(
                inspect(&mut tx, &request, &mut Proof::bounded(proof.used() - 1))
                    .await
                    .is_err()
            );
            assert_eq!(
                result,
                inspect(&mut tx, &request, &mut Proof::bounded(proof.used()))
                    .await
                    .unwrap()
            );
            tx.commit().await.unwrap();
            let cold = crate::PostgresStore::connect(&url).await.unwrap();
            assert_eq!(
                store.inspect_approval_driver(&request).await.unwrap(),
                cold.inspect_approval_driver(&request).await.unwrap()
            );
        }
    })
    .await
    .unwrap();
}
