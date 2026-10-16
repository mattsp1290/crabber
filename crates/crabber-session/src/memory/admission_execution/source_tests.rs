//! Genuine source guards remain closed after private carrier corruption.
use crate::admission_execution_contract::inspection::{
    corrupted_carrier_refuses, reserved_fixture,
};
use crate::{AdmissionExecutionError, Store};
#[tokio::test]
async fn admission_recovery_original_reserve_still_blocks_after_latest_origin_erasure() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let (source, grant, _) = reserved_fixture(&store, &clock).await;
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .erase_latest_preparation_for_test(
            &source.request().session_id,
            &grant
                .original_reservation()
                .reconciliation()
                .unwrap()
                .begin_id,
        );
    assert_eq!(
        store
            .claim_unstarted_admission(source.request().clone())
            .await
            .unwrap_err(),
        AdmissionExecutionError::PreparationBusy
    );
    assert_eq!(
        store
            .get_run(&source.request().expected_fence.run_id)
            .await
            .unwrap()
            .unwrap()
            .claim_token,
        source.request().expected_fence.claim_token
    );
}
#[tokio::test]
async fn admission_recovery_rejects_oversized_or_executed_private_run_before_claim() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    for case in 0..3 {
        let (source, grant, bound) = reserved_fixture(&store, &clock).await;
        let mut run = store
            .get_run(&source.request().expected_fence.run_id)
            .await
            .unwrap()
            .unwrap();
        match case {
            0 => run.checkpoint = Some(serde_json::json!({"unexpected":"executed state"})),
            1 => run.error = Some("oversized-private-error".repeat(65536)),
            _ => run.owner = "oversized-private-owner".repeat(65536),
        }
        store.state.lock().unwrap().runs.insert(run.id.clone(), run);
        corrupted_carrier_refuses(&store, source, grant, bound).await;
    }
}

#[tokio::test]
async fn admission_recovery_missing_audits_cannot_hide_preparation_publications() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let (source, grant, _) = reserved_fixture(&store, &clock).await;
    let ids = grant.original_reservation().reconciliation().unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state
            .session_owners
            .erase_historical_audit_for_test(&source.request().session_id, &ids.reserve_id);
        state
            .session_owners
            .erase_historical_audit_for_test(&source.request().session_id, &ids.begin_id);
    }
    assert_eq!(
        store
            .claim_unstarted_admission(source.request().clone())
            .await
            .unwrap_err(),
        AdmissionExecutionError::PreparationBusy
    );
}
