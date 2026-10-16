//! Actual owned and legacy inputs distinguish authenticated source from policy.
mod bounds;
use super::*;
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES};
use crate::{DeletionBlocker, DeletionError, Store};
use crabber_core::{AdmissionReceipt, ContentBlock, Message};
#[tokio::test]
async fn run_history_sources_authenticate_owned_journal_and_original_input() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut tx = store.pool.begin().await.unwrap();
    assert!(matches!(
        read(&mut tx, &receipt, MAX_PROOF_BYTES).await,
        Ok(true)
    ));
    assert!(matches!(
        read(&mut tx, &receipt, 1).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    let witness: crate::session_owner::preparation::admission::Witness = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1",
        )
        .bind(&receipt.run_id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap(),
    )
    .unwrap();
    tx.rollback().await.unwrap();
    // Each original root/child audit is required, including historical Begin.
    for operation in witness.audit_ids().unwrap() {
        let mut tx = store.pool.begin().await.unwrap();
        sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
            .bind(&receipt.session_id.0)
            .bind(&operation.0)
            .execute(&mut *tx)
            .await
            .unwrap();
        assert!(matches!(
            read(&mut tx, &receipt, MAX_PROOF_BYTES).await,
            Err(Failure::Error(DeletionError::UnknownStoreOutcome))
        ));
        tx.rollback().await.unwrap();
    }
    // The actual Begin transaction remains independently corroborated.
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    store
        .execution(crabber_core::RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token,
        })
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    assert!(matches!(
        read(&mut tx, &receipt, MAX_PROOF_BYTES).await,
        Ok(true)
    ));
    tx.rollback().await.unwrap();
    original_input(&store, &receipt).await;
    bounds::oversized(&store, &receipt).await;
    bounds::no_effect_still_requires_resource_policy(&store).await;
    original_issuer(&store, &receipt).await;
    unowned(&store).await;
}
async fn read(
    tx: &mut Transaction<'_, Postgres>,
    receipt: &AdmissionReceipt,
    budget: usize,
) -> crate::sql_deletion::readiness::Result<bool> {
    let mut proof = Proof::bounded(budget);
    let (initial, _, _) =
        super::super::journal(tx, &receipt.session_id, &receipt.run_id, &mut proof).await?;
    verify(
        tx,
        &receipt.session_id,
        &receipt.run_id,
        initial.as_ref(),
        &mut proof,
    )
    .await
}
async fn original_input(store: &crate::PostgresStore, receipt: &AdmissionReceipt) {
    let mut tx = store.pool.begin().await.unwrap();
    let raw: String = sqlx::query_scalar("SELECT snapshot_record FROM messages WHERE id=$1")
        .bind(&receipt.user_message_id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let mut message: Message = serde_json::from_str(&raw).unwrap();
    message.parts[0].content = ContentBlock::Text {
        text: "coherently changed original input".into(),
    };
    let accounting = crate::sql_snapshot::message_record(&message).unwrap();
    sqlx::query("UPDATE messages SET data=$2,snapshot_record=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1")
        .bind(&message.id.0).bind(crate::postgres::json(&message).unwrap()).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    assert!(matches!(
        read(&mut tx, receipt, MAX_PROOF_BYTES).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}
async fn original_issuer(store: &crate::PostgresStore, receipt: &AdmissionReceipt) {
    let mut tx = store.pool.begin().await.unwrap();
    let raw: String = sqlx::query_scalar(
        "SELECT data_json FROM native_run_issuances WHERE run_id=$1 AND sequence=1",
    )
    .bind(&receipt.run_id.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let mut initial: crate::native_parent::RunIssuance = serde_json::from_str(&raw).unwrap();
    initial.state.token = crate::native_parent::RunState::new(
        &receipt.session_id,
        &receipt.run_id,
        crabber_core::RunStatus::Running,
        "changed-original-fence",
    )
    .unwrap()
    .token;
    sqlx::query("UPDATE native_run_issuances SET data_json=$2 WHERE run_id=$1 AND sequence=1")
        .bind(&receipt.run_id.0)
        .bind(serde_json::to_string(&initial).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        read(&mut tx, receipt, MAX_PROOF_BYTES).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}
async fn unowned(store: &crate::PostgresStore) {
    let keyed = crate::admission_execution_contract::keyed(
        &SessionId::new(),
        time::OffsetDateTime::now_utc(),
    );
    let crate::KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_keyed_run(keyed).await.unwrap()
    else {
        panic!("actual keyed admission")
    };
    let mut tx = store.pool.begin().await.unwrap();
    assert!(matches!(
        read(&mut tx, &receipt, MAX_PROOF_BYTES).await,
        Ok(false)
    ));
    tx.rollback().await.unwrap();
}

mod unkeyed;
