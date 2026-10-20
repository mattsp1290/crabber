//! Actual original publications distinguish terminal proof from current labels.
use super::*;
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES};
use crate::{AttemptMutationOutcome, DeletionBlocker, DeletionError, Store};
use crabber_core::{Clock, ManualClock};
#[tokio::test]
async fn terminal_source_rejects_forgery_relink_and_bounded_overflow() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock.clone());
    let (admitted, execution, owner, mut conclusion) =
        crate::attempt_contract::conclusion::started(
            &store,
            &clock,
            crate::attempt_contract::conclusion::terminal(RunStatus::Completed),
        )
        .await;
    // A same-run, identical public settlement is insufficient without the actual
    // conclusion transaction's independent latest publication.
    execution
        .append_event(conclusion.materialize(&admitted.run).unwrap())
        .await
        .unwrap();
    conclusion.finish.expected_revision =
        store.session_revision(&admitted.session.id).await.unwrap();
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .conclude_attempt(conclusion, Some(owner))
        .await
        .unwrap()
    else {
        panic!("actual conclusion")
    };
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    let older = events
        .iter()
        .find(|event| event.kind == crabber_core::EventKind::RunSettled)
        .unwrap();
    let mut audit = events
        .iter()
        .find(|event| event.cursor == Some(receipt.event_cursor))
        .unwrap()
        .clone();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            &admitted.session.id,
            &admitted.run.id,
            RunStatus::Completed,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Ok(true)
    ));
    assert!(matches!(
        verify(
            &mut tx,
            &admitted.session.id,
            &admitted.run.id,
            RunStatus::Completed,
            &mut Proof::bounded(1)
        )
        .await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback().await.unwrap();
    reject_forged_status(&store, &admitted).await;
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    audit.payload["settlement"] = serde_json::to_value(older).unwrap();
    sqlx::query("UPDATE events SET data=$1 WHERE seq=$2")
        .bind(crate::public_state::encode_event(&audit).unwrap())
        .bind(i64::try_from(receipt.event_cursor.0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            &admitted.session.id,
            &admitted.run.id,
            RunStatus::Completed,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
    reject_oversized_error(&store, &admitted).await;
    assert_eq!(clock.now(), time::OffsetDateTime::UNIX_EPOCH);
}

async fn reject_forged_status(store: &crate::PostgresStore, admitted: &crate::AdmitOutcome) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET status='failed',data=jsonb_set(data,'{status}',to_jsonb('failed'::TEXT)) WHERE id=$1").bind(&admitted.run.id.0).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE native_run_issuances SET data_json=jsonb_set(data_json::jsonb,'{state,status}',to_jsonb('failed'::TEXT))::TEXT WHERE run_id=$1 AND sequence=(SELECT max(sequence) FROM native_run_issuances WHERE run_id=$1)").bind(&admitted.run.id.0).execute(&mut *tx).await.unwrap();
    assert!(matches!(
        super::super::verify(
            &mut tx,
            &admitted.session.id,
            &admitted.run.id,
            RunStatus::Failed,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}

async fn reject_oversized_error(store: &crate::PostgresStore, admitted: &crate::AdmitOutcome) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE runs SET data=jsonb_set(data,'{error}',to_jsonb(repeat('x',33554433))) WHERE id=$1",
    )
    .bind(&admitted.run.id.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            &admitted.session.id,
            &admitted.run.id,
            RunStatus::Completed,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback().await.unwrap();
}

mod retirement;
