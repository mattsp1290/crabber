//! Distinct commit/activation faults and source-origin wiring, without a Cartesian product.
use crate::successor_driver_contract::tools::{fixture, fresh};
use std::sync::Arc;

macro_rules! case {
    ($name:ident, $frontend:literal, $recovery:literal, $boundary:literal) => {
        #[tokio::test]
        async fn $name() {
            use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
            TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
                let clock = Arc::new(crabber_core::ManualClock::new(
                    time::OffsetDateTime::UNIX_EPOCH,
                ));
                let store = crate::PostgresStore::connect(&url)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                history(&store, &clock, $frontend, $recovery, $boundary).await;
            })
            .await
            .unwrap();
        }
    };
}

case!(tool_fresh_fault_before_commit, false, false, 1);
case!(tool_fresh_fault_committed_ack, false, false, 2);
case!(tool_fresh_fault_activation_ack, false, false, 3);
case!(tool_fresh_fault_cancelled, false, false, 4);
case!(tool_fresh_fault_expired, false, false, 5);
case!(tool_fresh_fault_changed_public_state, false, false, 6);
case!(tool_fresh_fault_changed_before_register, false, false, 7);
case!(tool_fresh_fault_tool_recovery_before_commit, false, true, 1);
case!(
    tool_fresh_fault_frontend_claim_committed_ack,
    true,
    false,
    2
);
case!(
    tool_fresh_fault_frontend_recovery_activation_ack,
    true,
    true,
    3
);

async fn history(
    store: &crate::PostgresStore,
    clock: &crabber_core::ManualClock,
    frontend: bool,
    recovery: bool,
    boundary: u8,
) {
    let fault = |value| {
        store
            .session_owner_fault
            .store(value, std::sync::atomic::Ordering::SeqCst);
    };
    let f = Box::pin(fixture(store, clock, frontend, recovery, false)).await;
    let mut tx = store
        .session_transaction(f.context.session_id())
        .await
        .unwrap();
    let request = super::tool_guards_tests::request(&mut tx, &f).await;
    tx.rollback().await.unwrap();
    if boundary <= 2 {
        Box::pin(fresh::faults(store, request, boundary, &fault)).await;
    } else if boundary == 3 {
        Box::pin(fresh::activation_fault(store, request, &fault)).await;
    } else {
        Box::pin(fresh::refusals(store, request, clock, boundary)).await;
    }
}
