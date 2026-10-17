//! The independent birth retains the complete original creation bodies.
use super::{Error, Proof};
use crate::Store;
#[tokio::test]
async fn scope_birth_refuses_coherently_rewritten_creation_receipt_and_audit() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    for fork in [false, true] {
        let saved = store
            .manage_session(crate::management_contract::create())
            .await
            .unwrap();
        let (session, operation, receipt, audit, table) = if fork {
            let crate::ForkOutcome::Applied(mut receipt) = store
                .fork_session(crate::sql_fork_contract::request(&saved.summary))
                .await
                .unwrap()
            else {
                panic!("actual fresh fork");
            };
            let raw: String = sqlx::query_scalar(
                "SELECT data_json FROM session_fork_audit WHERE session_id=$1 AND operation_id=$2",
            )
            .bind(&receipt.target.session_id.0)
            .bind(&receipt.operation_id.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
            let mut audit: crate::fork::Audit = serde_json::from_str(&raw).unwrap();
            receipt.history_digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
            audit.receipt = receipt.clone();
            audit.validate().unwrap();
            (
                receipt.target.session_id.clone(),
                receipt.operation_id.clone(),
                serde_json::to_string(&receipt).unwrap(),
                serde_json::to_string(&audit).unwrap(),
                "session_fork_audit",
            )
        } else {
            let raw: String = sqlx::query_scalar("SELECT data_json FROM session_management_audit WHERE session_id=$1 AND operation_id=$2")
                .bind(&saved.session_id.0).bind(&saved.operation_id.0).fetch_one(&store.writer).await.unwrap();
            let mut audit: crate::management::Audit = serde_json::from_str(&raw).unwrap();
            let mut receipt = saved;
            audit.created_at += time::Duration::seconds(1);
            receipt.summary.created_at = audit.created_at;
            receipt.summary.updated_at = audit.created_at;
            audit.receipt = receipt.clone();
            crate::management::verify_audit(&audit, &receipt).unwrap();
            (
                receipt.session_id.clone(),
                receipt.operation_id.clone(),
                serde_json::to_string(&receipt).unwrap(),
                serde_json::to_string(&audit).unwrap(),
                "session_management_audit",
            )
        };
        let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
        sqlx::query(
            "UPDATE operation_receipts SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&session.0)
        .bind(&operation.0)
        .bind(receipt)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(&format!(
            "UPDATE {table} SET data_json=$3 WHERE session_id=$1 AND operation_id=$2"
        ))
        .bind(&session.0)
        .bind(&operation.0)
        .bind(audit)
        .execute(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            super::super::verify_if_present(
                &mut tx,
                &session,
                &mut Proof::bounded(32 * 1024 * 1024)
            )
            .await
            .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        tx.rollback().await.unwrap();
    }
}
