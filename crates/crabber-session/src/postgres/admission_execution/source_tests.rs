//! Genuine source guards remain closed after private carrier corruption.
use crate::admission_execution_contract::inspection::{
    corrupted_carrier_refuses, reserved_fixture,
};
use crate::{AdmissionExecutionError, Store};
#[tokio::test]
async fn admission_recovery_original_reserve_still_blocks_after_latest_origin_erasure() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let (source, grant, _) = reserved_fixture(&store, &clock).await;
    let id = grant
        .original_reservation()
        .reconciliation()
        .unwrap()
        .begin_id;
    let data: String = sqlx::query_scalar(
        "SELECT data_json FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&source.request().session_id.0)
    .bind(&id.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let mut data: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert!(
        data["record"]
            .as_object_mut()
            .unwrap()
            .remove("preparation")
            .is_some()
    );
    sqlx::query(
        "UPDATE session_owner_audits SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&source.request().session_id.0)
    .bind(&id.0)
    .bind(data.to_string())
    .execute(&store.pool)
    .await
    .unwrap();
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
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
        sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
            .bind(&run.id.0)
            .bind(serde_json::to_value(&run).unwrap())
            .execute(&store.pool)
            .await
            .unwrap();
        corrupted_carrier_refuses(&store, source, grant, bound).await;
    }
}
