use super::*;
use crate::session_owner::preparation::{PendingApprovalSource, PendingToolSource};
use crate::transaction::Transaction;
use crabber_core::Clock;
use sqlx::Sqlite;
use std::sync::Arc;
#[tokio::test]
async fn pending_tool_preflight_rejects_coherent_tool_and_approval_claim_rewrites() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coherent-claim.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for frontend in [false, true] {
        Box::pin(tool_case(&store, &clock, frontend)).await;
    }
    Box::pin(approval_case(&store, &clock)).await;
}
async fn tool_case(store: &crate::SqliteStore, clock: &crabber_core::ManualClock, frontend: bool) {
    let mut request = Box::pin(
        crate::successor_driver_contract::tools::pending::unprepared(
            store,
            clock,
            u8::from(frontend),
            true,
            false,
        ),
    )
    .await;
    let c = match &mut request.source {
        PendingToolSource::Recovery(r) => &mut r.original,
        PendingToolSource::FrontendRecovery(r) => &mut r.original.claim,
        _ => unreachable!(),
    };
    c.expected_revision = c.expected_revision.next().unwrap();
    let c = c.clone();
    let digest = match &request.source {
        PendingToolSource::Recovery(r) => r.original.fingerprint().unwrap(),
        PendingToolSource::FrontendRecovery(r) => r.original.fingerprint().unwrap(),
        _ => unreachable!(),
    };
    let mut tx = store.begin_write().await.unwrap();
    let mut record = crate::sqlite::tool_continuation::read::origin_with_proof(
        &mut tx,
        &c.session_id,
        &c.run_id,
        Some(&c.continuation.id),
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    record.claim = Some(c.clone());
    if let PendingToolSource::FrontendRecovery(r) = &request.source {
        record.frontend_claim = Some(Box::new(r.original.clone()));
    }
    crate::sqlite::tool_continuation::write::save(&mut tx, &record, false)
        .await
        .unwrap();
    let (mut receipt, mut event) = crate::sqlite::tool_continuation::read::receipt_with_proof(
        &mut tx,
        &c.session_id,
        &c.id,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    receipt.semantic_digest = digest.clone();
    event.payload["semantic_digest"] = serde_json::to_value(&digest).unwrap();
    rewrite(
        &mut tx,
        &receipt.session_id,
        &receipt.operation_id,
        &digest,
        &serde_json::to_string(&receipt).unwrap(),
        &event,
    )
    .await;
    crate::sqlite::tool_continuation::current::verify_with_proof(
        &mut tx,
        &record,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    assert!(
        Box::pin(inspect(
            &mut tx,
            &request,
            choice::metadata(&request).unwrap(),
            clock.now(),
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
async fn approval_case(store: &crate::SqliteStore, clock: &crabber_core::ManualClock) {
    let mut approval = Box::pin(
        crate::successor_driver_contract::approval::pending::unprepared(store, clock, true, false),
    )
    .await;
    let PendingApprovalSource::Recovery(r) = &mut approval.source else {
        unreachable!()
    };
    r.original.expected_revision = r.original.expected_revision.next().unwrap();
    let c = r.original.clone();
    let digest = c.fingerprint().unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let mut receipt = crate::sqlite::approval::receipt_with_proof(
        &mut tx,
        &c.session_id,
        &c.id,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    let mut event = crate::sqlite::read_proof::event(
        &mut tx,
        &c.session_id,
        receipt.event_cursor,
        false,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    receipt.semantic_digest = digest.clone();
    event.payload["semantic_digest"] = serde_json::to_value(&digest).unwrap();
    rewrite(
        &mut tx,
        &receipt.session_id,
        &receipt.operation_id,
        &digest,
        &serde_json::to_string(&receipt).unwrap(),
        &event,
    )
    .await;
    assert!(
        Box::pin(super::super::pending_approval::inspect(
            &mut tx,
            &approval,
            crate::session_owner::preparation::pending_approval::metadata(&approval).unwrap(),
            clock.now(),
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
async fn rewrite(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
    digest: &crabber_core::InputFingerprint,
    receipt: &str,
    event: &crabber_core::EventRecord,
) {
    sqlx::query("UPDATE operation_receipts SET data_json=$3,semantic_digest=$4 WHERE session_id=$1 AND operation_id=$2").bind(&session.0).bind(&id.0).bind(receipt).bind(digest.as_str()).execute(&mut **tx).await.unwrap();
    sqlx::query("UPDATE events SET data=$3 WHERE session_id=$1 AND seq=$2")
        .bind(&session.0)
        .bind(i64::try_from(event.cursor.unwrap().0).unwrap())
        .bind(serde_json::to_string(event).unwrap())
        .execute(&mut **tx)
        .await
        .unwrap();
}
