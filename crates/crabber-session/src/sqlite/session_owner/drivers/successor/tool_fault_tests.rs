//! Fresh effects are gated through original commit/activation ACK uncertainty.
use crate::successor_driver_contract::tools::{fixture, fresh};
use std::sync::Arc;
#[tokio::test]
async fn tool_fresh_faults_keep_original_grants_and_effect_counts() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tool-faults.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let fault = |value| {
        store
            .session_owner_fault
            .store(value, std::sync::atomic::Ordering::SeqCst);
    };
    for frontend in [false, true] {
        for recovery in [false, true] {
            for boundary in 1..=7 {
                let f = Box::pin(fixture(&store, &clock, frontend, recovery, false)).await;
                let mut tx = store.begin_write().await.unwrap();
                let request = super::tool_guards_tests::request(&mut tx, &f).await;
                tx.rollback().await.unwrap();
                if boundary <= 2 {
                    Box::pin(fresh::faults(&store, request, boundary, &fault)).await;
                } else if boundary == 3 {
                    Box::pin(fresh::activation_fault(&store, request, &fault)).await;
                } else {
                    Box::pin(fresh::refusals(&store, request, &clock, boundary)).await;
                }
            }
        }
    }
}
