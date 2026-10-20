use super::*;
use crate::Store;

#[tokio::test]
async fn postgres_execution_binding_contract() {
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::execution_binding_contract::run_contract(|clock| store.with_clock(clock)).await;
}

#[tokio::test]
async fn postgres_execution_binding_fault_contract() {
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::execution_binding_contract::faults::run(&store, |stage| {
        store
            .binding_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}

#[tokio::test]
async fn binding_read_is_scoped_and_independent_of_worker_token() {
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let session = SessionId::new();
    let original = crate::execution_binding_contract::fixture(
        &session,
        time::OffsetDateTime::now_utc(),
        "metadata-backend",
        'b',
    );
    let KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_bound_keyed_run(original.clone()).await.unwrap()
    else {
        panic!("new admission must start")
    };
    let before = store
        .execution_binding(&session, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE runs SET claim_token=$2,data=jsonb_set(data,'{claim_token}',to_jsonb($2::text)) WHERE id=$1").bind(&receipt.run_id.0).bind("x".repeat(513)).execute(&store.pool).await.unwrap();
    assert_eq!(
        store
            .execution_binding(&session, &receipt.run_id)
            .await
            .unwrap(),
        Some(before)
    );
    sqlx::query("UPDATE run_execution_bindings SET data_json='malformed' WHERE run_id=$1")
        .bind(&receipt.run_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .execution_binding(&SessionId::new(), &receipt.run_id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .execution_binding(&session, &receipt.run_id)
            .await
            .unwrap_err(),
        StoreError::AdmissionConflict
    );
    assert_eq!(
        store
            .lookup_admission(&session, &original.keyed.options.key)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        store.admit_bound_keyed_run(original).await.unwrap_err(),
        StoreError::AdmissionConflict
    );
}
