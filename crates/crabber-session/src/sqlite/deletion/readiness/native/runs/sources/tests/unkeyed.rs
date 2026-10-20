//! An unkeyed initial source uses its genuine full-capsule witness.
use super::*;
#[tokio::test]
async fn unkeyed_history_authenticates_original_capsule_before_policy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unkeyed-history.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(&store).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let (verified, used) = read_original(&mut tx, &admitted, MAX_PROOF_BYTES)
        .await
        .unwrap_or_else(|_| panic!("genuine original unkeyed input"));
    assert!(verified);
    assert!(
        read_original(&mut tx, &admitted, used)
            .await
            .unwrap_or_else(|_| panic!("exact original source budget"))
            .0
    );
    assert!(matches!(
        read_original(&mut tx, &admitted, used - 1).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM run_execution_bindings WHERE run_id=$1")
            .bind(&admitted.run.id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let mut binding: crate::RetainedExecutionBinding = serde_json::from_str(&raw).unwrap();
    let original_plan = binding.capsule.plan_fingerprint.clone();
    binding.capsule.plan_fingerprint = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    binding.capsule.input_fingerprint = binding.capsule.computed_input_fingerprint().unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&admitted.run.id.0)
        .bind(serde_json::to_string(&binding).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=json_set(data,'$.plan_fingerprint',$2) WHERE id=$1")
        .bind(&admitted.run.id.0)
        .bind(binding.capsule.plan_fingerprint.as_str())
        .execute(&mut *tx)
        .await
        .unwrap();
    // Current projections agree; only the independently original capsule rejects the change.
    crate::sqlite::execution_binding::read_with_proof(
        &mut tx,
        &admitted.session.id,
        &admitted.run.id,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        read_original(&mut tx, &admitted, MAX_PROOF_BYTES).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&admitted.run.id.0)
        .bind(raw)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=json_set(data,'$.plan_fingerprint',$2) WHERE id=$1")
        .bind(&admitted.run.id.0)
        .bind(original_plan.as_str())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM run_execution_bindings WHERE run_id=$1")
        .bind(&admitted.run.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        read_original(&mut tx, &admitted, MAX_PROOF_BYTES).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}
async fn read_original(
    tx: &mut Transaction<'_, Sqlite>,
    admitted: &crate::AdmitOutcome,
    budget: usize,
) -> crate::sql_deletion::readiness::Result<(bool, usize)> {
    let mut proof = Proof::bounded(budget);
    let (initial, _, _) =
        super::super::super::journal(tx, &admitted.session.id, &admitted.run.id, &mut proof)
            .await?;
    let verified = super::super::verify(
        tx,
        &admitted.session.id,
        &admitted.run.id,
        initial.as_ref(),
        &mut proof,
    )
    .await?;
    Ok((verified, proof.used()))
}

#[tokio::test]
async fn unkeyed_legacy_absence_never_authenticates_an_owned_initial_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy-unkeyed-history.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let admitted =
        crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let (verified, _) = read_original(&mut tx, &admitted, MAX_PROOF_BYTES)
        .await
        .unwrap_or_else(|_| panic!("genuine legacy unkeyed metadata"));
    assert!(!verified);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn unkeyed_history_receipt_guard_recognizes_a_genuine_keyed_cohort() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actual-keyed-cohort.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    let (initial, _, _) =
        super::super::super::journal(&mut tx, &receipt.session_id, &receipt.run_id, &mut proof)
            .await
            .unwrap_or_else(|_| panic!("genuine keyed issuer"));
    let original = super::super::admission::verify(
        &mut tx,
        &receipt.session_id,
        &receipt.run_id,
        initial.as_ref(),
        &mut proof,
    )
    .await
    .unwrap_or_else(|_| panic!("genuine keyed original"))
    .expect("genuine owned keyed witness");
    assert!(!original.is_unkeyed());
    assert!(
        super::super::unkeyed::has_keyed_receipt(&mut tx, &original, &mut proof)
            .await
            .unwrap_or_else(|_| panic!("keyed receipt lookup"))
    );
    tx.rollback().await.unwrap();
}
