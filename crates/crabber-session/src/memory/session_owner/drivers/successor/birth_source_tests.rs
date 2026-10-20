//! Coherent mutable carrier rewrites cannot replace protected original births.
use super::*;
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn approval_birth_refuses_coherent_frozen_prompt_rewrite() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = crate::successor_driver_contract::approval::fixture(&store, &clock, false).await;
    let mut state = store.state.lock().unwrap();
    let digest = {
        let record = state.approvals.get_mut(&f.batch.continuation.id).unwrap();
        let crate::CallApproval::Ask { description, .. } = &mut record.batch.calls[0].decision
        else {
            panic!("frozen original prompt");
        };
        *description = crabber_core::InteractionDescription::new(
            description.name(),
            description.kind(),
            "DIFFERENT ORIGINAL HUMAN PROMPT",
            description.response_schema().clone(),
            description.authority().clone(),
            description.expires_after_ms(),
        )
        .unwrap();
        record
            .batch
            .fingerprint(&record.session_id, &record.run_id)
            .unwrap()
    };
    let Some(crate::memory::OperationReceipt::Approval(receipt)) = state
        .operation_receipts
        .get_mut(&(f.receipt.session_id.clone(), f.batch.id.clone()))
    else {
        panic!("actual suspension");
    };
    receipt.semantic_digest = digest;
    let receipt = receipt.clone();
    event(&mut state, &receipt);
    assert!(
        source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None).is_err(),
        "original metadata cannot borrow rewritten original batch"
    );
    assert!(
        fresh(
            &state,
            &f.request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .is_err(),
        "coherent original Approval prompt rewrite must not authorize a fresh source grant"
    );
}
#[tokio::test]
async fn approval_birth_refuses_coherent_grant_to_deny_rewrite() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = crate::successor_driver_contract::approval::fixture(&store, &clock, false).await;
    let mut state = store.state.lock().unwrap();
    let answer = {
        let record = state.approvals.get_mut(&f.batch.continuation.id).unwrap();
        let question = &mut record.questions[0];
        question.view.state = crabber_core::InteractionState::Denied;
        let answer = question.answer.as_mut().unwrap();
        answer.reply = crate::ApprovalReply::Deny;
        answer.clone()
    };
    let Some(crate::memory::OperationReceipt::Approval(receipt)) = state
        .operation_receipts
        .get_mut(&(answer.session_id.clone(), answer.id.clone()))
    else {
        panic!("actual answer");
    };
    receipt.semantic_digest = answer.fingerprint().unwrap();
    let crate::ApprovalOutcome::Answered { state: outcome, .. } = &mut receipt.outcome else {
        panic!("actual answer");
    };
    *outcome = crabber_core::InteractionState::Denied;
    let receipt = receipt.clone();
    event(&mut state, &receipt);
    source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None).unwrap();
    assert!(
        fresh(
            &state,
            &f.request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .is_err(),
        "coherent original Approval answer rewrite must not authorize a fresh source grant"
    );
}
fn event(state: &mut State, receipt: &crate::ApprovalReceipt) {
    let events: Vec<_> = state.events.iter().cloned().collect();
    state.events = crate::memory::events::EventLog::default();
    for mut event in events {
        if event.cursor == Some(receipt.event_cursor) {
            event.payload["semantic_digest"] =
                serde_json::to_value(&receipt.semantic_digest).unwrap();
            event.payload["outcome"] = serde_json::to_value(&receipt.outcome).unwrap();
            crate::approval::verify_receipt(receipt, &event).unwrap();
        }
        state.events.append(event).unwrap();
    }
}

#[tokio::test]
async fn legacy_birth_metadata_replays_but_cannot_register_or_activate() {
    use crate::Store;
    use crate::session_owner::{
        SessionOwnerError,
        driver::SuccessorExecutionDriverOutcome,
        owned_scope::{OwnedScopeParentAuthority, OwnedTaskRegistration},
    };
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = crate::successor_driver_contract::approval::fixture(&store, &clock, false).await;
    let SuccessorExecutionDriverOutcome::Applied { grant, .. } = store
        .register_successor_execution_driver(f.request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh registration")
    };
    // Model importing the original legacy v1 record+receipt format: no birth marker existed.
    store.state.lock().unwrap().continuation_inputs = crate::continuation_input::Ledger::default();
    store
        .approval_snapshot(&f.receipt.session_id, &f.receipt.run_id)
        .await
        .unwrap();
    assert!(matches!(
        store
            .register_successor_execution_driver(f.request.clone())
            .await
            .unwrap(),
        SuccessorExecutionDriverOutcome::Replayed(_)
    ));
    assert!(matches!(
        fresh(
            &store.state.lock().unwrap(),
            &f.request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        ),
        Err(SessionOwnerError::Unsupported)
    ));
    let task = OwnedTaskRegistration::register(grant.scope_source().unwrap(), || async {}).unwrap();
    assert!(matches!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant))
            )
            .await,
        Err(SessionOwnerError::Unsupported)
    ));
}
