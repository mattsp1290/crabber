//! Coherent mutable claim receipts must still disagree with their protected root.
use super::*;
use crate::session_owner::preparation::{PendingApprovalSource, PendingToolSource};
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn tool_preflight_coherent_claim_rewrite_is_refused() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let mut request = Box::pin(
        crate::successor_driver_contract::tools::pending::unprepared(
            &store, &clock, 0, true, false,
        ),
    )
    .await;
    let PendingToolSource::Recovery(recovery) = &mut request.source else {
        unreachable!()
    };
    recovery.original.expected_revision = recovery.original.expected_revision.next().unwrap();
    let digest = recovery.original.fingerprint().unwrap();
    let mut state = store.state.lock().unwrap();
    state
        .tool_continuations
        .get_mut(&recovery.original.continuation.id)
        .unwrap()
        .claim = Some(recovery.original.clone());
    let Some(crate::memory::OperationReceipt::ToolContinuation(receipt)) =
        state.operation_receipts.get_mut(&(
            recovery.original.session_id.clone(),
            recovery.original.id.clone(),
        ))
    else {
        unreachable!()
    };
    receipt.semantic_digest = digest.clone();
    let cursor = receipt.event_cursor;
    rewrite(&mut state, cursor, &digest);
    crate::memory::tool_continuation::checked_record_with_proof(
        &state,
        &recovery.original.continuation.id,
        &mut Proof::bounded(32 << 20),
    )
    .unwrap();
    assert!(
        inspect(
            &state,
            &request,
            choice::metadata(&request).unwrap(),
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .is_err(),
        "mutable current claim passed without its protected original root"
    );
}
#[tokio::test]
async fn approval_preflight_coherent_claim_rewrite_is_refused() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let mut request = Box::pin(
        crate::successor_driver_contract::approval::pending::unprepared(
            &store, &clock, true, false,
        ),
    )
    .await;
    let PendingApprovalSource::Recovery(recovery) = &mut request.source else {
        unreachable!()
    };
    recovery.original.expected_revision = recovery.original.expected_revision.next().unwrap();
    let digest = recovery.original.fingerprint().unwrap();
    {
        let mut state = store.state.lock().unwrap();
        let Some(crate::memory::OperationReceipt::Approval(receipt)) =
            state.operation_receipts.get_mut(&(
                recovery.original.session_id.clone(),
                recovery.original.id.clone(),
            ))
        else {
            unreachable!()
        };
        receipt.semantic_digest = digest.clone();
        let cursor = receipt.event_cursor;
        rewrite(&mut state, cursor, &digest);
    }
    assert!(
        store.inspect_pending_approval(&request).is_err(),
        "Approval mutable current claim passed without its protected original root"
    );
}
fn rewrite(
    state: &mut State,
    cursor: crabber_core::EventCursor,
    digest: &crabber_core::InputFingerprint,
) {
    let events: Vec<_> = state.events.iter().cloned().collect();
    state.events = crate::memory::events::EventLog::default();
    for mut event in events {
        if event.cursor == Some(cursor) {
            event.payload["semantic_digest"] = serde_json::to_value(digest).unwrap();
        }
        state.events.append(event).unwrap();
    }
}
