//! Actual protected sources remain distinct from coherent current projections.
use super::*;
use crate::approval::terminal::history_contract::{Fixture, fixture};
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES};
use crate::{DeletionBlocker, DeletionError};
use crabber_core::{EventRecord, ManualClock};
#[tokio::test]
async fn approval_terminal_history_requires_frozen_origin_and_independent_cursor() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = store.with_clock(clock.clone());
        for expire in [false, true] {
            let f = fixture(&store, &clock, expire).await;
            let mut tx = crate::postgres::transaction::begin(&store.pool)
                .await
                .unwrap();
            assert!(matches!(read(&mut tx, &f, MAX_PROOF_BYTES).await, Ok(true)));
            exact(&mut tx, &f).await;
            assert!(matches!(
                read(&mut tx, &f, 1).await,
                Err(Failure::Blocked(DeletionBlocker::ProofLimit))
            ));
            tx.rollback().await.unwrap();
            relink(&store, &f).await;
            coherent_usage(&store, &f).await;
            coherent_turn(&store, &f).await;
            current_error(&store, &f).await;
            missing_origin(&store, &f).await;
            legacy(&store, &f).await;
            oversized(&store, &f).await;
        }
    })
    .await
    .unwrap();
}
async fn read(tx: &mut Transaction<'_, Postgres>, f: &Fixture, budget: usize) -> Result<bool> {
    verify(
        tx,
        &f.current.session_id,
        &f.current.id,
        f.current.status,
        &mut Proof::bounded(budget),
    )
    .await
}
async fn save(tx: &mut Transaction<'_, Postgres>, event: &EventRecord) {
    sqlx::query("UPDATE events SET data=$1 WHERE seq=$2")
        .bind(crate::public_state::encode_event(event).unwrap())
        .bind(i64::try_from(event.cursor.unwrap().0).unwrap())
        .execute(&mut **tx)
        .await
        .unwrap();
}
fn rejected(value: &Result<bool>) {
    assert!(matches!(
        value,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
}
async fn relink(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut audit = f.audit.clone();
    audit.payload["terminal"]["settlement"] = serde_json::to_value(&f.older).unwrap();
    save(&mut tx, &audit).await;
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}
async fn coherent_usage(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=jsonb_set(data,'{usage,output_tokens}',to_jsonb(13::BIGINT)) WHERE id=$1").bind(&f.current.id.0).execute(&mut *tx).await.unwrap();
    let mut original = f.original.clone();
    original.payload["usage"]["output_tokens"] = serde_json::json!(13);
    original.payload["output_tokens"] = serde_json::json!(13);
    let mut audit = f.audit.clone();
    audit.payload["terminal"]["settlement"] = serde_json::to_value(&original).unwrap();
    save(&mut tx, &original).await;
    save(&mut tx, &audit).await;
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}
async fn missing_origin(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&f.current.session_id.0)
        .bind(&f.batch.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}
async fn legacy(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut audit = f.audit.clone();
    audit.payload["version"] = serde_json::json!(1);
    audit.payload.as_object_mut().unwrap().remove("terminal");
    save(&mut tx, &audit).await;
    assert!(matches!(read(&mut tx, f, MAX_PROOF_BYTES).await, Ok(false)));
    tx.rollback().await.unwrap();
}
async fn oversized(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=jsonb_set(data,'{error}',to_jsonb($2::TEXT)) WHERE id=$1")
        .bind(&f.current.id.0)
        .bind("x".repeat(MAX_PROOF_BYTES + 1))
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        read(&mut tx, f, MAX_PROOF_BYTES).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback().await.unwrap();
}

async fn coherent_turn(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut original = f.original.clone();
    original.turn_id = Some(crabber_core::TurnId::new());
    let mut audit = f.audit.clone();
    audit.payload["terminal"]["settlement"] = serde_json::to_value(&original).unwrap();
    save(&mut tx, &original).await;
    save(&mut tx, &audit).await;
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}
async fn current_error(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=jsonb_set(data,'{error}',to_jsonb($2::TEXT)) WHERE id=$1")
        .bind(&f.current.id.0)
        .bind("forged private error")
        .execute(&mut *tx)
        .await
        .unwrap();
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}

async fn exact(tx: &mut Transaction<'_, Postgres>, f: &Fixture) {
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    assert!(matches!(
        verify(
            tx,
            &f.current.session_id,
            &f.current.id,
            f.current.status,
            &mut proof
        )
        .await,
        Ok(true)
    ));
    let bytes = proof.used();
    assert!(matches!(read(tx, f, bytes).await, Ok(true)));
    assert!(matches!(
        read(tx, f, bytes - 1).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
}
