//! Actual protected sources remain distinct from coherent current projections.
use super::*;
use crate::run_control::terminal::history_contract::{Fixture, fixture};
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES};
use crate::{DeletionBlocker, DeletionError};
use crabber_core::{EventRecord, ManualClock};
#[tokio::test]
async fn waiting_terminal_history_requires_frozen_origin_and_independent_cursor() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock.clone());
    for (tool, previous) in [(false, false), (true, false), (false, true)] {
        let f = fixture(&store, &clock, tool, previous).await;
        let mut tx = store.pool.begin().await.unwrap();
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
        if tool {
            oversized_binding(&store, &f).await;
        }
    }
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
async fn coherent_usage(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = store.pool.begin().await.unwrap();
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
    let mut tx = store.pool.begin().await.unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&f.current.session_id.0)
        .bind(&f.origin_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}
async fn legacy(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = store.pool.begin().await.unwrap();
    let mut audit = f.audit.clone();
    audit
        .payload
        .as_object_mut()
        .unwrap()
        .remove("terminal_version");
    audit.payload.as_object_mut().unwrap().remove("terminal");
    save(&mut tx, &audit).await;
    assert!(matches!(read(&mut tx, f, MAX_PROOF_BYTES).await, Ok(false)));
    tx.rollback().await.unwrap();
}
async fn oversized(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = store.pool.begin().await.unwrap();
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
    let mut tx = store.pool.begin().await.unwrap();
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
    let mut tx = store.pool.begin().await.unwrap();
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

async fn oversized_binding(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = store.pool.begin().await.unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
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

async fn relink(store: &crate::PostgresStore, f: &Fixture) {
    let mut tx = store.pool.begin().await.unwrap();
    let mut older = f.original.clone();
    older.cursor = Some(crabber_core::EventCursor(older.cursor.unwrap().0 - 1));
    assert!(older.cursor.unwrap() > f.summary.receipt.event_cursor);
    let mut audit = f.audit.clone();
    audit.payload["terminal"]["settlement"] = serde_json::to_value(&older).unwrap();
    // Corroboration still sees a canonical frame and identical frozen facts;
    // only the independent latest-settlement index rejects this older cursor.
    save(&mut tx, &older).await;
    save(&mut tx, &audit).await;
    rejected(&read(&mut tx, f, MAX_PROOF_BYTES).await);
    tx.rollback().await.unwrap();
}
