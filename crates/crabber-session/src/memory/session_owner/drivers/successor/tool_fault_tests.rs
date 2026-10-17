//! Fresh effects are gated through original commit/activation ACK uncertainty.
use super::*;
use crate::successor_driver_contract::tools::{fixture, fresh};
use std::sync::Arc;
#[tokio::test]
async fn tool_fresh_faults_keep_original_grants_and_effect_counts() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let fault = |value| {
        store
            .session_owner_fault
            .store(value, std::sync::atomic::Ordering::SeqCst);
    };
    for frontend in [false, true] {
        for recovery in [false, true] {
            for boundary in 1..=7 {
                let f = Box::pin(fixture(&store, &clock, frontend, recovery, false)).await;
                let request = super::tool_source_tests::request(&store, &f);
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
