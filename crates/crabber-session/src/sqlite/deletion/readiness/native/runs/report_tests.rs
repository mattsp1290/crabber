//! Actual bounded journal folds distinguish source authentication from coverage.
use super::*;
use crate::deletion::history::Fact;
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES};
use crate::{DeletionBlocker, DeletionError};
#[tokio::test]
async fn run_history_report_preserves_noninitial_and_missing_proof() {
    let directory =
        tempfile::tempdir().unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let path = directory.path().join("run-report.db");
    crate::SqliteStore::migrate(&path)
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut tx = store
        .writer
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let report = verify(
        &mut tx,
        &receipt.session_id,
        &receipt.run_id,
        RunStatus::Running,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    )
    .await
    .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    assert_eq!(report.initial_source, Fact::Authenticated);
    assert_eq!(report.every_noninitial_claim, Fact::Unproven);
    assert_eq!(report.retained_noninitial_sources, Fact::Authenticated);
    assert_eq!(report.terminal_source, Fact::Unproven);
    assert_eq!(report.main_join, Fact::Unproven);
    assert_eq!(report.resource_policy, Fact::Unproven);
    assert!(!report.complete());
    let (initial, _, _) = journal(
        &mut tx,
        &receipt.session_id,
        &receipt.run_id,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    )
    .await
    .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let mut next = initial.expect("original issuer");
    next.version = 1;
    next.sequence = 2;
    next.ownership_handoff = None;
    next.state.token = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    sqlx::query(
        "INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,2,$3)",
    )
    .bind(&receipt.session_id.0)
    .bind(&receipt.run_id.0)
    .bind(serde_json::to_string(&next).unwrap())
    .execute(&mut *tx)
    .await
    .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let (_, _, claims) = journal(
        &mut tx,
        &receipt.session_id,
        &receipt.run_id,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    )
    .await
    .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    assert_eq!(
        claims.retained_sources,
        Fact::Unproven,
        "valid legacy issuer is not a verified native claim"
    );
    sqlx::query("DELETE FROM native_run_issuances WHERE run_id=$1 AND sequence=1")
        .bind(&receipt.run_id.0)
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    assert!(matches!(
        journal(
            &mut tx,
            &receipt.session_id,
            &receipt.run_id,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    relinked_and_oversized(&store, &receipt, next).await;
}
async fn relinked_and_oversized(
    store: &crate::SqliteStore,
    receipt: &crabber_core::AdmissionReceipt,
    mut next: RunIssuance,
) {
    let mut tx = store
        .writer
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    next.sequence = 1;
    next.state.run_id = RunId::new();
    sqlx::query("UPDATE native_run_issuances SET data_json=$2 WHERE run_id=$1 AND sequence=1")
        .bind(&receipt.run_id.0)
        .bind(serde_json::to_string(&next).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    assert!(matches!(
        journal(
            &mut tx,
            &receipt.session_id,
            &receipt.run_id,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    let mut tx = store
        .writer
        .begin()
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    sqlx::query("UPDATE native_run_issuances SET data_json=$2 WHERE run_id=$1 AND sequence=1")
        .bind(&receipt.run_id.0)
        .bind("x".repeat(MAX_PROOF_BYTES + 1))
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
    assert!(matches!(
        journal(
            &mut tx,
            &receipt.session_id,
            &receipt.run_id,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback()
        .await
        .unwrap_or_else(|_| panic!("bounded fixture operation failed"));
}
