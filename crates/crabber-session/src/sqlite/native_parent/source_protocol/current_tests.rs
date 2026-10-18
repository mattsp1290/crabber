//! The closed Raw gate retains full current integrity under one caller Proof.
use crate::Store;
use crate::native_enrollment::caller::contract::Harness;
use crate::public_session::v2::proof::Proof;
use crabber_core::{Clock, ManualClock};
use std::sync::Arc;

#[tokio::test]
async fn raw_current_gate_uses_one_actual_budget_and_preserves_native_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raw-current-budget.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let admitted = crate::public_state_contract::admit(&store, clock.now()).await;
    let before = store.initial_retained(&admitted.session.id).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut measured = Proof::bounded(32 << 20);
    crate::sqlite::credential_access::check_current(&mut tx, &admitted.run, &mut measured)
        .await
        .unwrap();
    let exact = measured.used();
    let mut proof = Proof::bounded(exact);
    crate::sqlite::credential_access::check_current(&mut tx, &admitted.run, &mut proof)
        .await
        .unwrap();
    assert_eq!(proof.used(), exact);
    let refused = crate::sqlite::credential_access::check_current(
        &mut tx,
        &admitted.run,
        &mut Proof::bounded(exact - 1),
    )
    .await
    .is_err();
    tx.rollback().await.unwrap();
    assert!(refused);
    assert_eq!(store.initial_retained(&admitted.session.id).await, before);
}

#[tokio::test]
async fn raw_current_gate_refuses_foreign_original_missing_middle_and_extra_projection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raw-current-prefix.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let admitted = crate::public_state_contract::admit(&store, clock.now()).await;
    let other = crate::public_state_contract::admit(&store, clock.now()).await;
    clock.set(admitted.run.lease_until);
    store
        .claim_expired_run(&admitted.run.id, "current-replacement")
        .await
        .unwrap();
    let first_replacement = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    clock.set(first_replacement.lease_until);
    store
        .claim_expired_run(&admitted.run.id, "middle-replacement")
        .await
        .unwrap();
    let native = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    let before = store.initial_retained(&admitted.session.id).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    assert!(check(&mut tx, &native).await);
    let foreign: String = sqlx::query_scalar(
        "SELECT data_json FROM native_run_issuances WHERE run_id=$1 AND sequence=1",
    )
    .bind(&other.run.id.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let mut save = tx.begin().await.unwrap();
    sqlx::query("UPDATE native_run_issuances SET data_json=$2 WHERE run_id=$1 AND sequence=1")
        .bind(&native.id.0)
        .bind(foreign)
        .execute(&mut *save)
        .await
        .unwrap();
    let foreign_refused = !check(&mut save, &native).await;
    save.rollback().await.unwrap();
    let mut save = tx.begin().await.unwrap();
    sqlx::query("DELETE FROM native_run_issuances WHERE run_id=$1 AND sequence=2")
        .bind(&native.id.0)
        .execute(&mut *save)
        .await
        .unwrap();
    // The exact genuine latest original still agrees with the Run; only the
    // complete prefix detects the erased middle issuer projection.
    let latest = super::super::original(
        &mut save,
        &native.session_id,
        &native.id,
        3,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    latest.verify(&native).unwrap();
    let middle_refused = !check(&mut save, &native).await;
    save.rollback().await.unwrap();
    let mut save = tx.begin().await.unwrap();
    sqlx::query("INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) SELECT session_id,run_id,sequence+1,data_json FROM native_run_issuances WHERE run_id=$1 AND sequence=3")
        .bind(&native.id.0).execute(&mut *save).await.unwrap();
    let extra_refused = !check(&mut save, &native).await;
    save.rollback().await.unwrap();
    assert!(check(&mut tx, &native).await);
    tx.rollback().await.unwrap();
    assert!(foreign_refused && middle_refused && extra_refused);
    assert_eq!(store.initial_retained(&admitted.session.id).await, before);
}
async fn check(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    native: &crabber_core::Run,
) -> bool {
    crate::sqlite::credential_access::check_current(tx, native, &mut Proof::bounded(32 << 20))
        .await
        .is_ok()
}
