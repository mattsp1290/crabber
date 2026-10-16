//! Coherent current binding is insufficient without the independently original input.
use crate::Store;
use crate::session_owner::preparation::ExpiredRunOrigin;
use crabber_core::ManualClock;
use std::sync::Arc;
#[tokio::test]
async fn unkeyed_expired_source_rejects_coherent_binding_and_missing_original_join() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock.clone());
    let (source, binding) =
        crate::run_control_contract::expired::unkeyed::source(&store, &clock).await;
    let session = &source.request().session_id;
    let run_id = &source.request().expected_fence.run_id;
    let original = store.get_run(run_id).await.unwrap().unwrap();
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM run_execution_bindings WHERE run_id=$1")
            .bind(&run_id.0)
            .fetch_one(&store.writer)
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
        .bind(serde_json::to_string(&changed).unwrap())
        .execute(&store.writer)
        .await
        .unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&run_id.0)
        .bind(serde_json::to_string(&retained).unwrap())
        .execute(&store.writer)
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
        .bind(serde_json::to_string(&original).unwrap())
        .execute(&store.writer)
        .await
        .unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&run_id.0)
        .bind(raw)
        .execute(&store.writer)
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
        .execute(&store.writer)
        .await
        .unwrap();
    crate::run_control_contract::expired::unkeyed::assert_refuses(&store, &source, &binding).await;
}
