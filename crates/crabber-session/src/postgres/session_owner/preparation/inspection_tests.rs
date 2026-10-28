use super::*;
use crate::session_owner::preparation::inspection::contract;
use crate::{PostgresStore, Store};
#[tokio::test]
async fn original_initial_admission_inspection_is_pure_and_bounded() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

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
        Err(PreparationCommitError::Store(crate::StoreError::AdmissionLimit(limit)))
            if limit.limit_bytes() == Some(u64::try_from(used - 1).unwrap())
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
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
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

    }).await.unwrap();
}

#[tokio::test]
async fn initial_inspection_rejects_missing_witness_and_audit() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

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

    }).await.unwrap();
}

#[tokio::test]
async fn admission_limit_preserves_uncommitted_preparation_and_original_replay() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        PostgresStore::migrate(&url).await.unwrap();
        let store = PostgresStore::connect(&url).await.unwrap();
        let original =
            crate::session_owner::preparation_contract::resources::initial_unkeyed_observation(
                &store,
            )
            .await;
        let grant = original.observed_grant();
        let session = grant
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone();
        let reconciliation = store
            .lookup_preparation(&session, &grant.original_reservation().key)
            .await
            .unwrap();
        let revision = store.session_revision(&session).await.unwrap();
        let mut input = Proof::bounded(inspection::MAX_PROOF);
        crate::native_enrollment::caller::initial::preparation_input(&original, &mut input)
            .unwrap();
        for cap in [0, input.used()] {
            let mut proof = Proof::bounded(cap);
            let error = Box::pin(store.complete_preparation_captured_with_proof(
                original.clone(),
                false,
                &mut proof,
            ))
            .await
            .err()
            .unwrap();
            let PreparationCommitError::Store(error) = error else {
                panic!("typed store proof limit");
            };
            let limit = error.admission_limit().unwrap();
            assert_eq!(limit.kind().as_str(), "proof_bytes");
            assert_eq!(limit.limit_bytes(), Some(u64::try_from(cap).unwrap()));
            assert_eq!(store.session_revision(&session).await.unwrap(), revision);
            assert_eq!(
                store
                    .lookup_preparation(&session, &grant.original_reservation().key)
                    .await
                    .unwrap(),
                reconciliation
            );
            assert!(
                store
                    .inspect_initial_preparation_admission(&original)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let admitted = store.complete_preparation(original.clone()).await.unwrap();
        assert!(!admitted.is_replayed());
        assert!(
            store
                .complete_preparation(original)
                .await
                .unwrap()
                .is_replayed()
        );
    })
    .await
    .unwrap();
}
