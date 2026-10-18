//! Actual oversized raw carrier and valid source never supply resource policy.
use super::*;
pub(super) async fn oversized(store: &crate::PostgresStore, receipt: &AdmissionReceipt) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut wrong = receipt.clone();
    wrong.session_id = SessionId::new();
    assert!(matches!(
        read(&mut tx, &wrong, MAX_PROOF_BYTES).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    let raw: String = sqlx::query_scalar("SELECT snapshot_record FROM messages WHERE id=$1")
        .bind(&receipt.user_message_id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let mut message: Message = serde_json::from_str(&raw).unwrap();
    message.parts[0].content = ContentBlock::Text {
        text: "x".repeat(MAX_PROOF_BYTES + 1),
    };
    let accounting = crate::sql_snapshot::message_record(&message).unwrap();
    assert!(sqlx::query("UPDATE messages SET data=$2,snapshot_record=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1").bind(&message.id.0).bind(sqlx::types::Json(&message)).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.is_ok(), "oversized canonical fixture update");
    assert!(matches!(
        read(&mut tx, receipt, MAX_PROOF_BYTES).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback().await.unwrap();
}
pub(super) async fn no_effect_still_requires_resource_policy(store: &crate::PostgresStore) {
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(store, false).await;
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    store
        .abandon_run(crabber_core::AbandonRequest {
            expected: crabber_core::RunFence {
                run_id: run.id,
                claim_token: run.claim_token,
            },
            expected_owner: run.owner,
            authority: crabber_core::AbandonAuthority::HostStoppedOwner,
        })
        .await
        .unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        super::super::super::verify(
            &mut tx,
            &receipt.session_id,
            &receipt.run_id,
            crabber_core::RunStatus::Interrupted,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Ok(crate::deletion::history::RunHistoryReport {
            initial_source: crate::deletion::history::Fact::Authenticated,
            initial_consumption:
                crate::native_enrollment::initial_consumer::InitialConsumption::Unproven,
            terminal_source: crate::deletion::history::Fact::Authenticated,
            main_join: crate::deletion::history::Fact::Unproven,
            resource_policy: crate::deletion::history::Fact::Unproven,
            ..
        })
    ));
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        crate::postgres::ownership_origin::coverage(&mut tx, &receipt.session_id)
            .await
            .unwrap()
    else {
        panic!("actual owned origin")
    };
    assert!(matches!(
        super::super::super::super::super::readiness(&mut tx, &receipt.session_id, &coverage)
            .await
            .unwrap(),
        crate::sql_deletion::readiness::Readiness::Blocked(DeletionBlocker::OwnershipUnknown)
    ));
    tx.rollback().await.unwrap();
}
