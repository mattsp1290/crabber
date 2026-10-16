use super::*;
#[tokio::test]
async fn prepared_admission_origin_authenticates_independent_sources() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;

    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut tx = store.writer.begin().await.unwrap();

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
            .fetch_one(&store.writer)
            .await
            .unwrap();
    let mut tx = store.writer.begin().await.unwrap();
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
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert_eq!(before, after);
    for statement in [
        "INSERT OR REPLACE INTO preparation_admission_witnesses SELECT run_id,session_id,root_join,'{}' FROM preparation_admission_witnesses WHERE run_id=$1",
        "INSERT OR IGNORE INTO preparation_admission_witnesses SELECT run_id,session_id,root_join,'{}' FROM preparation_admission_witnesses WHERE run_id=$1",
    ] {
        assert!(
            sqlx::query(statement)
                .bind(&receipt.run_id.0)
                .execute(&store.writer)
                .await
                .is_err()
        );
    }
    let witness: crate::session_owner::preparation::admission::Witness =
        serde_json::from_str(&before).unwrap();
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&receipt.session_id.0)
        .bind(&witness.join_id().unwrap().0)
        .execute(&store.writer)
        .await
        .unwrap();
    let mut tx = store.writer.begin().await.unwrap();

    assert!(matches!(
        coverage(&mut tx, &receipt.session_id).await,
        Err(crate::DeletionError::UnknownStoreOutcome)
    ));
    tx.rollback().await.unwrap();
}
