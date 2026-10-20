//! Coherent mutable pairs cannot replace independent original claim sources.
#[path = "approval_tests/static_sources.rs"]
mod static_sources;
use super::*;
use crate::{
    Store,
    successor_driver_contract::approval::{Fixture, fixture_input, integrity},
};
use crabber_core::{Clock, ManualClock};
pub(in super::super) async fn run(store: &crate::SqliteStore, clock: &ManualClock) {
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let f = fixture_input(store, clock, recovery, unkeyed).await;
        let mut tx = store.begin_write().await.unwrap();
        let mut proof = Proof::bounded(32 << 20);
        Box::pin(source::original(&mut tx, &f.request, &mut proof, None))
            .await
            .unwrap();
        assert!(
            Box::pin(source::original(
                &mut tx,
                &f.request,
                &mut Proof::bounded(proof.used() - 1),
                None
            ))
            .await
            .is_err()
        );
        tx.rollback().await.unwrap();
        Box::pin(static_sources::original_sources(store, &f)).await;
        Box::pin(refreshed_revision(store, clock, &f)).await;
        let f = fixture_input(store, clock, recovery, unkeyed).await;
        Box::pin(rewritten_binding(store, clock, &f)).await;
        Box::pin(original_metadata(store, &f)).await;
        Box::pin(begin_restore(store, clock, &f)).await;
        if recovery {
            Box::pin(parent_swap(store, &f)).await;
        }
    }
}
async fn refreshed_revision(store: &crate::SqliteStore, clock: &ManualClock, f: &Fixture) {
    let mut receipt = f.receipt.clone();
    receipt.revision = integrity::advance(store, f).await;
    let mut tx = store.begin_write().await.unwrap();
    let raw: String = sqlx::query_scalar("SELECT data FROM events WHERE session_id=$1 AND seq=$2")
        .bind(&receipt.session_id.0)
        .bind(i64::try_from(receipt.event_cursor.0).unwrap())
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let mut event: crabber_core::EventRecord = serde_json::from_str(&raw).unwrap();
    integrity::refreshed_event(&mut event, &receipt);
    sqlx::query("UPDATE events SET data=$3 WHERE session_id=$1 AND seq=$2")
        .bind(&receipt.session_id.0)
        .bind(i64::try_from(receipt.event_cursor.0).unwrap())
        .bind(serde_json::to_string(&event).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE operation_receipts SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&receipt.session_id.0)
    .bind(&receipt.operation_id.0)
    .bind(serde_json::to_string(&receipt).unwrap())
    .execute(&mut *tx)
    .await
    .unwrap();
    // Even the original receipt/event decoder accepts this consistent pair.
    assert_eq!(
        crate::sqlite::approval::receipt_with_proof(
            &mut tx,
            &receipt.session_id,
            &receipt.operation_id,
            &mut Proof::bounded(32 << 20)
        )
        .await
        .unwrap()
        .unwrap(),
        receipt
    );
    assert!(
        Box::pin(source::original(
            &mut tx,
            &f.request,
            &mut Proof::bounded(32 << 20),
            None
        ))
        .await
        .is_err()
    );
    assert!(
        Box::pin(fresh::fresh(
            &mut tx,
            &f.request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
async fn rewritten_binding(store: &crate::SqliteStore, clock: &ManualClock, f: &Fixture) {
    let session = f.request.context.session_id();
    let run = &f.request.fence.run_id;
    let mut binding = store
        .execution_binding(session, run)
        .await
        .unwrap()
        .unwrap();
    integrity::rewrite_binding(&mut binding);
    let mut native = store.get_run(run).await.unwrap().unwrap();
    native.config_hash = binding.capsule.config_hash.as_str().into();
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&run.0)
        .bind(crate::execution_binding::encode_bounded(&binding).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run.0)
        .bind(serde_json::to_string(&native).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    crate::sqlite::execution_binding::read_with_proof(
        &mut tx,
        session,
        run,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        Box::pin(source::original(
            &mut tx,
            &f.request,
            &mut Proof::bounded(32 << 20),
            Some(&binding.capsule)
        ))
        .await
        .is_err()
    );
    assert!(
        Box::pin(fresh::fresh(
            &mut tx,
            &f.request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
async fn original_metadata(store: &crate::SqliteStore, f: &Fixture) {
    let SuccessorExecutionDriverOutcome::Applied { receipt, .. } = store
        .register_successor_execution_driver(f.request.clone())
        .await
        .unwrap()
    else {
        panic!()
    };
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("UPDATE runs SET status='completed' WHERE id=$1")
        .bind(&f.request.fence.run_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE continuations SET state='waiting',data_json='{}' WHERE id=$1")
        .bind(&f.batch.continuation.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    // Original batch is authenticated even on metadata reconciliation.
    assert!(
        metadata(&mut tx, &f.request, &mut Proof::bounded(32 << 20))
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("UPDATE runs SET status='completed' WHERE id=$1")
        .bind(&f.request.fence.run_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        metadata(&mut tx, &f.request, &mut Proof::bounded(32 << 20))
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    tx.rollback().await.unwrap();
}

async fn begin_restore(store: &crate::SqliteStore, clock: &ManualClock, f: &Fixture) {
    let begun = integrity::begin(store, f).await;
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("UPDATE execution_attempts SET state='Unstarted' WHERE run_id=$1 AND id=$2")
        .bind(&f.request.fence.run_id.0)
        .bind(&f.batch.next_attempt.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&begun.attempt.session_id.0)
        .bind(&begun.operation_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SAVEPOINT protected_begin")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        sqlx::query("DELETE FROM events WHERE seq=$1")
            .bind(i64::try_from(begun.event_cursor.0).unwrap())
            .execute(&mut *tx)
            .await
            .is_err()
    );
    sqlx::query("ROLLBACK TO SAVEPOINT protected_begin")
        .execute(&mut *tx)
        .await
        .unwrap();
    let run = store
        .get_run(&f.request.fence.run_id)
        .await
        .unwrap()
        .unwrap();
    // This direct eligibility check deliberately has no revision comparison;
    // the independently retained Begin witness must refuse the restored row.
    assert!(
        approval::check(
            &mut tx,
            &run,
            &f.batch.continuation,
            &f.batch.next_attempt,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .await
        .is_err()
    );
    Box::pin(source::original(
        &mut tx,
        &f.request,
        &mut Proof::bounded(32 << 20),
        None,
    ))
    .await
    .unwrap();
    Box::pin(static_sources::malformed_witness(
        &mut tx, &run, f, &begun, clock,
    ))
    .await;
    tx.rollback().await.unwrap();
}
async fn parent_swap(store: &crate::SqliteStore, f: &Fixture) {
    let mut original = store
        .lookup_approval(&f.receipt.session_id, &f.original.id)
        .await
        .unwrap()
        .unwrap();
    let old_id = original.operation_id.clone();
    let mut recovery = f.receipt.clone();
    integrity::replace_parent(&mut original, &mut recovery);
    let mut tx = store.begin_write().await.unwrap();
    for receipt in [&original, &recovery] {
        let raw: String =
            sqlx::query_scalar("SELECT data FROM events WHERE session_id=$1 AND seq=$2")
                .bind(&receipt.session_id.0)
                .bind(i64::try_from(receipt.event_cursor.0).unwrap())
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        let mut event: crabber_core::EventRecord = serde_json::from_str(&raw).unwrap();
        integrity::synchronize(&mut event, receipt);
        sqlx::query("UPDATE events SET data=$3 WHERE session_id=$1 AND seq=$2")
            .bind(&receipt.session_id.0)
            .bind(i64::try_from(receipt.event_cursor.0).unwrap())
            .bind(serde_json::to_string(&event).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
        let selected = if receipt.operation_id == original.operation_id {
            &old_id
        } else {
            &receipt.operation_id
        };
        sqlx::query("UPDATE operation_receipts SET operation_id=$3,data_json=$4 WHERE session_id=$1 AND operation_id=$2")
            .bind(&receipt.session_id.0).bind(&selected.0).bind(&receipt.operation_id.0)
            .bind(serde_json::to_string(receipt).unwrap()).execute(&mut *tx).await.unwrap();
        assert_eq!(
            crate::sqlite::approval::receipt_with_proof(
                &mut tx,
                &receipt.session_id,
                &receipt.operation_id,
                &mut Proof::bounded(32 << 20)
            )
            .await
            .unwrap()
            .unwrap(),
            *receipt
        );
    }
    assert!(
        Box::pin(source::original(
            &mut tx,
            &f.request,
            &mut Proof::bounded(32 << 20),
            None
        ))
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
