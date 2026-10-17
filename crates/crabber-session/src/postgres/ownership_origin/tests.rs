use super::*;
#[tokio::test]
async fn prepared_admission_origin_authenticates_independent_sources() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();

    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    super::super::lock_session_scope(&mut tx, &receipt.session_id)
        .await
        .unwrap();

    let CoverageRead::Verified(proof) = coverage(&mut tx, &receipt.session_id).await.unwrap()
    else {
        panic!("exact original admission origin");
    };
    assert_eq!(proof.session_id(), &receipt.session_id);
    assert!(proof.proof_bytes() > 0);
    tx.rollback().await.unwrap();
    // Stored sources authenticate the association without the live grant.
    let before: String =
        sqlx::query_scalar("SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1")
            .bind(&receipt.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE preparation_admission_witnesses SET data_json='{}' WHERE run_id=$1")
            .bind(&receipt.run_id.0)
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let after: String =
        sqlx::query_scalar("SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1")
            .bind(&receipt.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    let witness: crate::session_owner::preparation::admission::Witness =
        serde_json::from_str(&before).unwrap();
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&receipt.session_id.0)
        .bind(&witness.join_id().unwrap().0)
        .execute(&store.pool)
        .await
        .unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    super::super::lock_session_scope(&mut tx, &receipt.session_id)
        .await
        .unwrap();

    assert!(matches!(
        coverage(&mut tx, &receipt.session_id).await,
        Err(crate::DeletionError::UnknownStoreOutcome)
    ));
    tx.rollback().await.unwrap();
}
