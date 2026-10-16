//! Coherent current binding is insufficient without the independently original input.
use crate::session_owner::preparation::ExpiredRunOrigin;
use crate::{PostgresStore, Store};
use crabber_core::ManualClock;
use std::sync::Arc;
#[tokio::test]
async fn unkeyed_expired_source_rejects_coherent_binding_and_missing_original_join() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let (source, binding) =
        crate::run_control_contract::expired::unkeyed::source(&store, &clock).await;
    let session = &source.request().session_id;
    let run_id = &source.request().expected_fence.run_id;
    let original = store.get_run(run_id).await.unwrap().unwrap();
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM run_execution_bindings WHERE run_id=$1")
            .bind(&run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    let mut retained: crate::RetainedExecutionBinding = serde_json::from_str(&raw).unwrap();
    retained.capsule.plan_fingerprint =
        crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    retained.capsule.input_fingerprint = retained.capsule.computed_input_fingerprint().unwrap();
    let mut changed = original.clone();
    changed.plan_fingerprint = retained.capsule.plan_fingerprint.as_str().into();
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run_id.0)
        .bind(sqlx::types::Json(serde_json::to_value(&changed).unwrap()))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&run_id.0)
        .bind(serde_json::to_string(&retained).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        store
            .execution_binding(session, run_id)
            .await
            .unwrap()
            .is_some()
    );
    crate::run_control_contract::expired::unkeyed::assert_refuses(&store, &source, &binding).await;
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run_id.0)
        .bind(sqlx::types::Json(serde_json::to_value(&original).unwrap()))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&run_id.0)
        .bind(raw)
        .execute(&store.pool)
        .await
        .unwrap();
    store
        .inspect_expired_run_preparation(source.request().clone())
        .await
        .unwrap();
    let ExpiredRunOrigin::UnkeyedAdmission(join) = &source.target().origin else {
        panic!("explicit original");
    };
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&session.0)
        .bind(&join.0)
        .execute(&store.pool)
        .await
        .unwrap();
    crate::run_control_contract::expired::unkeyed::assert_refuses(&store, &source, &binding).await;
}
