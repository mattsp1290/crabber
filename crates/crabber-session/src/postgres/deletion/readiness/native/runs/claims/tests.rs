//! Two genuine source-backed recoveries, bounded reads, and erased-tail limits.
use super::*;
use crate::deletion::history::Fact;
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES};
use crate::{DeletionBlocker, DeletionError};
use std::sync::Arc;
fn proof() -> Proof {
    Proof::bounded(MAX_PROOF_BYTES)
}
#[tokio::test]
async fn ordered_claim_sources_reject_borrowing_but_never_infer_erased_history_complete() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"))
        .with_clock(clock.clone());
    let f =
        crate::session_owner::preparation_contract::claim_history::fixture(&store, &clock).await;
    let session = &f.before_tail.session_id;
    let run = &f.before_tail.id;
    let mut tx = store
        .pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    let mut charged = proof();
    let (_, _, claims) = super::super::journal(&mut tx, session, run, &mut charged)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    assert_eq!(claims.retained_sources, Fact::Authenticated);
    assert_eq!(claims.complete, Fact::Unproven);
    super::super::journal(&mut tx, session, run, &mut Proof::bounded(charged.used()))
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    assert!(matches!(
        super::super::journal(
            &mut tx,
            session,
            run,
            &mut Proof::bounded(charged.used() - 1)
        )
        .await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    let rows = sqlx::query_scalar::<_,String>("SELECT data_json FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 ORDER BY sequence")
        .bind(&session.0).bind(&run.0).fetch_all(&mut *tx).await.unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    let issuers = rows
        .iter()
        .map(|row| serde_json::from_str::<RunIssuance>(row).unwrap())
        .collect::<Vec<_>>();
    let recoveries = issuers
        .iter()
        .filter(|issuer| {
            issuer
                .completion_source
                .as_ref()
                .is_some_and(|s| s.native_operation == CompletionOperation::ToolRecovery)
        })
        .collect::<Vec<_>>();
    assert_eq!(recoveries.len(), 2);
    let first = recoveries[0];
    let tail = recoveries[1];
    let previous = issuers
        .iter()
        .find(|i| i.sequence + 1 == first.sequence)
        .unwrap();
    let mut borrowed = first.clone();
    borrowed.completion_source = tail.completion_source.clone();
    // This is a real locally valid association; only the whole ordered fold
    // rejects using the later original control twice.
    assert_eq!(
        source(&mut tx, Some(previous), &borrowed, &mut proof())
            .await
            .unwrap_or_else(|_| panic!("bounded claim fixture failed")),
        Some(f.tail.event_cursor)
    );
    sqlx::query("UPDATE native_run_issuances SET data_json=$3 WHERE run_id=$1 AND sequence=$2")
        .bind(&run.0)
        .bind(i64::try_from(first.sequence).unwrap())
        .bind(serde_json::to_string(&borrowed).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    assert!(matches!(
        super::super::journal(&mut tx, session, run, &mut proof()).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    missing_and_oversized(&store, &f).await;
    erased_tail(&store, &f, tail.sequence).await;
}
async fn erased_tail(
    store: &crate::PostgresStore,
    f: &crate::session_owner::preparation_contract::claim_history::Fixture,
    sequence: u64,
) {
    let session = &f.before_tail.session_id;
    let run = &f.before_tail.id;
    let mut tx = store
        .pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    sqlx::query(
        "DELETE FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=$3",
    )
    .bind(&session.0)
    .bind(&run.0)
    .bind(i64::try_from(sequence).unwrap())
    .execute(&mut *tx)
    .await
    .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&session.0)
        .bind(&f.tail.operation_id.0)
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    sqlx::query("DELETE FROM events WHERE session_id=$1 AND seq=$2")
        .bind(&session.0)
        .bind(i64::try_from(f.tail.event_cursor.0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    sqlx::query("UPDATE runs SET claim_token=$2,data=$3::jsonb,lease_until=$4 WHERE id=$1")
        .bind(&run.0)
        .bind(&f.before_tail.claim_token)
        .bind(serde_json::to_string(&f.before_tail).unwrap())
        .bind(backend::micros(f.before_tail.lease_until))
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    let (_, latest, claims) = super::super::journal(&mut tx, session, run, &mut proof())
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    assert!(latest == Some(crate::native_parent::RunState::from_run(&f.before_tail).unwrap()));
    assert_eq!(claims.retained_sources, Fact::Authenticated);
    assert_eq!(
        claims.complete,
        Fact::Unproven,
        "present source rows cannot prove erased tail absent"
    );
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
}
async fn missing_and_oversized(
    store: &crate::PostgresStore,
    f: &crate::session_owner::preparation_contract::claim_history::Fixture,
) {
    let mut tx = store
        .pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&f.before_tail.session_id.0)
        .bind(&f.first.operation_id.0)
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    assert!(matches!(
        super::super::journal(
            &mut tx,
            &f.before_tail.session_id,
            &f.before_tail.id,
            &mut proof()
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    let mut tx = store
        .pool
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    sqlx::query(
        "UPDATE operation_receipts SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&f.before_tail.session_id.0)
    .bind(&f.first.operation_id.0)
    .bind("x".repeat(MAX_PROOF_BYTES + 1))
    .execute(&mut *tx)
    .await
    .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
    assert!(matches!(
        super::super::journal(
            &mut tx,
            &f.before_tail.session_id,
            &f.before_tail.id,
            &mut proof()
        )
        .await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded claim fixture failed"));
}
