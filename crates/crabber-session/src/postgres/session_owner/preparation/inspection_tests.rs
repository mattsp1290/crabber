use super::*;
use crate::session_owner::preparation::inspection::contract;
use crate::{PostgresStore, Store};
#[tokio::test]
async fn original_initial_admission_inspection_is_pure_and_bounded() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    contract::run(&store).await;
    let (original, admitted) = contract::fresh(&store).await;
    let mut proof = Proof::bounded(inspection::MAX_PROOF);
    let expected = store
        .inspect_initial_admission_with_proof(&original, &mut proof)
        .await
        .unwrap();
    let used = proof.used();
    assert_eq!(
        store
            .inspect_initial_admission_with_proof(&original, &mut Proof::bounded(used))
            .await
            .unwrap(),
        expected
    );
    assert!(matches!(
        store
            .inspect_initial_admission_with_proof(&original, &mut Proof::bounded(used - 1))
            .await,
        Err(PreparationCommitError::Owner(Error::Capacity))
    ));
    // Corrupt current body is unrelated; restore before assertions so even a
    // failing inspection cannot poison global unfinished-run test listings.
    let saved: String = sqlx::query_scalar("SELECT CAST(data AS TEXT) FROM runs WHERE id=$1")
        .bind(&admitted.run.id.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data='{}' WHERE id=$1")
        .bind(&admitted.run.id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    let inspected = store.inspect_initial_preparation_admission(&original).await;
    sqlx::query("UPDATE runs SET data=$1::jsonb WHERE id=$2")
        .bind(saved)
        .bind(&admitted.run.id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(inspected.unwrap(), expected);
    // Mutable issuer projection changed coherently: independent protected slot rejects it.
    let (other, actual) = contract::fresh(&store).await;
    let mut tx = store.pool.begin().await.unwrap();
    let raw: String = sqlx::query_scalar("SELECT data_json FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=1").bind(&actual.session.id.0).bind(&actual.run.id.0).fetch_one(&mut *tx).await.unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    value["state"]["token"] = serde_json::json!("c".repeat(64));
    sqlx::query("UPDATE native_run_issuances SET data_json=$1 WHERE session_id=$2 AND run_id=$3 AND sequence=1").bind(value.to_string()).bind(&actual.session.id.0).bind(&actual.run.id.0).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .inspect_initial_preparation_admission(&other)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn initial_inspection_rejects_missing_witness_and_audit() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    for fault in ["witness", "audit"] {
        let (original, admitted) = contract::fresh(&store).await;
        let revision = store.session_revision(&admitted.session.id).await.unwrap();
        let root = original.observed_grant().reconciliation().unwrap().join_id;
        if fault == "witness" {
            sqlx::query("DELETE FROM preparation_admission_witnesses WHERE run_id=$1")
                .bind(&admitted.run.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE session_owner_audits SET data_json='{}' WHERE session_id=$1 AND operation_id=$2")
                .bind(&admitted.session.id.0).bind(&root.0).execute(&store.pool).await.unwrap();
        }
        assert!(
            matches!(
                store.inspect_initial_preparation_admission(&original).await,
                Err(PreparationCommitError::Owner(Error::UnknownStoreOutcome))
            ),
            "{fault}"
        );
        assert_eq!(
            store.session_revision(&admitted.session.id).await.unwrap(),
            revision
        );
    }
    let (original, admitted) = contract::fresh(&store).await;
    // Actual immutable protocol rows reject downgrades/deletion before a
    // reader could accept an unprotected original as a fresh cohort.
    assert!(
        sqlx::query("UPDATE runs SET native_source_protocol=0 WHERE id=$1")
            .bind(&admitted.run.id.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM native_run_sources WHERE run_id=$1")
            .bind(&admitted.run.id.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
    assert!(
        store
            .inspect_initial_preparation_admission(&original)
            .await
            .unwrap()
            .is_some()
    );
}
