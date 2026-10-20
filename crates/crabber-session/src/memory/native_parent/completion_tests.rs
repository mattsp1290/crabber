//! Actual six continuation writers, followed by borrowed historical corruption.
use crate::memory::{MemoryStore, OperationReceipt};
use crate::native_parent::{CompletionOperation, RunIssuance, completion_history as history};
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use crabber_core::{InputFingerprint, ManualClock};
use std::sync::Arc;

#[tokio::test]
async fn original_continuation_sources_authenticate_six_native_claims() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    crate::session_owner::preparation_contract::continuations::approval(&store, &clock).await;
    crate::session_owner::preparation_contract::continuations::tools(&store, &clock).await;
    crate::session_owner::preparation_contract::frontend::run(&store, &clock).await;
    let state = store.state.lock().unwrap();
    let mut seen = Vec::new();
    for current in state
        .native_run_issuances
        .values()
        .filter(|issuer| issuer.version == 3)
    {
        let source = current.completion_source.as_ref().unwrap();
        seen.push(source.native_operation);
        let previous = state
            .native_run_issuances
            .get(&(
                current.state.session_id.clone(),
                current.state.run_id.clone(),
                current.sequence - 1,
            ))
            .unwrap();
        let join = state
            .session_owners
            .source_audit(&current.state.session_id, &source.root_join_id)
            .unwrap();
        let reserve = state
            .session_owners
            .source_audit(&current.state.session_id, &join.record.origin)
            .unwrap();
        let origin = reserve.record.preparation.as_ref().unwrap();
        let ids = crate::session_owner::preparation::identity::ids(
            &origin.key,
            &current.state.session_id,
        )
        .unwrap();
        let begin = state
            .session_owners
            .source_audit(&current.state.session_id, &ids.begin)
            .unwrap();
        let (PreparationKey::Approval(id) | PreparationKey::ToolContinuation(id)) = &origin.key
        else {
            panic!("original continuation key")
        };
        let control = state
            .operation_receipts
            .get(&(current.state.session_id.clone(), id.clone()))
            .unwrap();
        let cursor = match &control {
            OperationReceipt::Approval(receipt) => receipt.event_cursor,
            OperationReceipt::ToolContinuation(receipt) => receipt.event_cursor,
            _ => panic!("original native control"),
        };
        let event = state
            .events
            .iter()
            .find(|event| event.cursor == Some(cursor))
            .unwrap();
        let control = match &control {
            OperationReceipt::Approval(receipt) => history::Control::Approval(receipt, event),
            OperationReceipt::ToolContinuation(receipt) => history::Control::Tool(receipt, event),
            _ => unreachable!(),
        };
        assert!(
            history::verify_control(previous, current, reserve, begin, join, &control).unwrap()
        );
        corruptions(previous, current, reserve, begin, join, &control);
    }
    for operation in [
        CompletionOperation::ApprovalClaim,
        CompletionOperation::ApprovalRecovery,
        CompletionOperation::ToolClaim,
        CompletionOperation::ToolRecovery,
        CompletionOperation::FrontendClaim,
        CompletionOperation::FrontendRecovery,
    ] {
        assert!(seen.contains(&operation));
    }
    assert_eq!(seen.len(), 6);
}
fn corruptions(
    previous: &RunIssuance,
    current: &RunIssuance,
    reserve: &Audit,
    begin: &Audit,
    join: &Audit,
    control: &history::Control<'_>,
) {
    let valid = |issuer: &RunIssuance, origin: &Audit, started: &Audit, joined: &Audit| {
        history::verify_control(previous, issuer, origin, started, joined, control)
    };
    let mut changed = current.clone();
    changed
        .completion_source
        .as_mut()
        .unwrap()
        .native_semantic_digest = InputFingerprint::new("c".repeat(64)).unwrap();
    assert!(valid(&changed, reserve, begin, join).is_err());
    let mut changed = current.clone();
    changed.completion_source.as_mut().unwrap().root_join_id = "foreign-join".into();
    assert!(valid(&changed, reserve, begin, join).is_err());
    let mut predecessor = previous.clone();
    predecessor.state.status = if predecessor.state.status == crabber_core::RunStatus::Paused {
        crabber_core::RunStatus::Running
    } else {
        crabber_core::RunStatus::Paused
    };
    if previous.version == 1 {
        predecessor.validate().unwrap();
    }
    assert!(history::verify_control(&predecessor, current, reserve, begin, join, control).is_err());
    let mut changed = current.clone();
    changed.state.token.clone_from(&previous.state.token);
    assert!(valid(&changed, reserve, begin, join).is_err());
    let mut changed = current.clone();
    changed.sequence += 1;
    assert!(valid(&changed, reserve, begin, join).is_err());
    let mut changed = begin.clone();
    changed.record.preparation = None;
    assert!(valid(current, reserve, &changed, join).is_err());
    let mut changed = join.clone();
    changed.receipt.semantic_digest = InputFingerprint::new("d".repeat(64)).unwrap();
    assert!(valid(current, reserve, begin, &changed).is_err());
    let (mut event, receipt) = match control {
        history::Control::Approval(receipt, event) => ((**event).clone(), Some(*receipt)),
        history::Control::Tool(_, event) => ((**event).clone(), None),
    };
    event.payload["oversized"] = serde_json::Value::String("x".repeat(65536));
    let oversized = match (receipt, control) {
        (Some(receipt), _) => history::Control::Approval(receipt, &event),
        (_, history::Control::Tool(receipt, _)) => history::Control::Tool(receipt, &event),
        _ => unreachable!(),
    };
    assert!(history::verify_control(previous, current, reserve, begin, join, &oversized).is_err());
    let mut legacy = current.clone();
    legacy.version = 1;
    legacy.completion_source = None;
    assert!(!valid(&legacy, reserve, begin, join).unwrap());
}
