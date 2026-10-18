//! Actual public readers corroborate retained input births before accepting projections.
use super::*;
use crate::{
    MemoryStore, Store,
    continuation_input::{
        Domain,
        tests::{fixture, rewrite},
    },
};
#[tokio::test]
async fn retained_answer_source_rejects_erased_answer_membership() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    for domain in [Domain::Approval, Domain::Tool] {
        let case = fixture(&store, domain, true).await;
        {
            let mut state = store.state.lock().unwrap();
            state
                .continuation_inputs
                .erase_answer_indexes_for_test(&case.scope);
            // The accepted input projection is absent, but its actual birth remains.
            assert!(
                answers(
                    &state,
                    &case.scope,
                    &[],
                    &mut crate::public_session::v2::proof::Proof::bounded(32 << 20)
                )
                .is_err()
            );
            assert!(
                consistent(
                    &state,
                    domain,
                    &case.scope.session,
                    None,
                    &mut crate::public_session::v2::proof::Proof::bounded(32 << 20)
                )
                .is_err()
            );
            erase_answer_projection(&mut state, &case);
        }
        match domain {
            Domain::Approval => assert!(
                store
                    .approval_snapshot(&case.scope.session, &case.scope.run)
                    .await
                    .is_err()
            ),
            Domain::Tool => assert!(
                store
                    .tool_continuation_snapshot(&case.scope.session, &case.scope.run)
                    .await
                    .is_err()
            ),
        }
    }
}
fn erase_answer_projection(state: &mut State, case: &crate::continuation_input::tests::Case) {
    let cursor = match state
        .operation_receipts
        .remove(&(case.scope.session.clone(), case.operation.clone()))
        .unwrap()
    {
        super::super::OperationReceipt::Approval(r) => r.event_cursor,
        super::super::OperationReceipt::ToolContinuation(r) => r.event_cursor,
        _ => panic!("actual answer receipt"),
    };
    state.events.remove_for_test(cursor).unwrap();
    match case.scope.domain {
        Domain::Approval => {
            let q = &mut state
                .approvals
                .get_mut(&case.scope.continuation.id)
                .unwrap()
                .questions[0];
            q.answer = None;
            q.view.state = crabber_core::InteractionState::Pending;
        }
        Domain::Tool => {
            let q = &mut state
                .tool_continuations
                .get_mut(&case.scope.continuation.id)
                .unwrap()
                .questions[0];
            q.answer = None;
            q.view.state = crabber_core::InteractionState::Pending;
        }
    }
}
#[tokio::test]
async fn coherent_birth_carrier_rewrites_are_refused() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    for domain in [Domain::Approval, Domain::Tool] {
        for answer in [false, true] {
            let case = fixture(&store, domain, answer).await;
            rewrite_memory(&store, &case);
            match domain {
                Domain::Approval => assert!(
                    store
                        .approval_snapshot(&case.scope.session, &case.scope.run)
                        .await
                        .is_err()
                ),
                Domain::Tool => assert!(
                    store
                        .tool_continuation_snapshot(&case.scope.session, &case.scope.run)
                        .await
                        .is_err()
                ),
            }
            if !answer {
                match domain {
                    Domain::Approval => assert!(
                        store
                            .original_approval_plan(
                                &case.scope.session,
                                &case.scope.run,
                                &case.scope.continuation
                            )
                            .await
                            .is_err()
                    ),
                    Domain::Tool => assert!(
                        store
                            .original_tool_continuation_plan(
                                &case.scope.session,
                                &case.scope.run,
                                &case.scope.continuation
                            )
                            .await
                            .is_err()
                    ),
                }
            }
        }
    }
}
#[tokio::test]
async fn retained_cohort_rejects_erased_projection_and_barrier() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    for domain in [Domain::Approval, Domain::Tool] {
        let case = fixture(&store, domain, false).await;
        let mut state = store.state.lock().unwrap();
        match domain {
            Domain::Approval => {
                state.approvals.remove(&case.scope.continuation.id);
                state.approval_barriers.remove(&case.scope.session);
            }
            Domain::Tool => {
                state.tool_continuations.remove(&case.scope.continuation.id);
                state.tool_barriers.remove(&case.scope.session);
            }
        }
        assert!(
            consistent(
                &state,
                domain,
                &case.scope.session,
                None,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20)
            )
            .is_err()
        );
        assert!(
            consistent(
                &state,
                domain,
                &case.scope.session,
                None,
                &mut crate::public_session::v2::proof::Proof::bounded(0)
            )
            .is_err()
        );
    }
}

fn rewrite_memory(store: &MemoryStore, case: &crate::continuation_input::tests::Case) {
    let domain = case.scope.domain;
    let mut state = store.state.lock().unwrap();
    let raw = match domain {
        Domain::Approval => {
            serde_json::to_value(state.approvals.get(&case.scope.continuation.id).unwrap()).unwrap()
        }
        Domain::Tool => serde_json::to_value(
            state
                .tool_continuations
                .get(&case.scope.continuation.id)
                .unwrap(),
        )
        .unwrap(),
    };
    let receipt = state
        .operation_receipts
        .get(&(case.scope.session.clone(), case.operation.clone()))
        .unwrap();
    let receipt = match receipt {
        crate::memory::OperationReceipt::Approval(r) => serde_json::to_value(r).unwrap(),
        crate::memory::OperationReceipt::ToolContinuation(r) => serde_json::to_value(r).unwrap(),
        _ => panic!("original continuation receipt"),
    };
    let (record, receipt) = rewrite(case, raw, receipt);
    match domain {
        Domain::Approval => {
            state.approvals.insert(
                case.scope.continuation.id.clone(),
                serde_json::from_value(record).unwrap(),
            );
        }
        Domain::Tool => {
            state.tool_continuations.insert(
                case.scope.continuation.id.clone(),
                serde_json::from_value(record).unwrap(),
            );
        }
    }
    let events: Vec<_> = state.events.iter().cloned().collect();
    let cursor: crabber_core::EventCursor =
        serde_json::from_value(receipt["event_cursor"].clone()).unwrap();
    let mut rewritten = 0;
    state.events = crate::memory::events::EventLog::default();
    for mut event in events {
        // Waiting presentation deliberately shares this operation correlation.
        // Rewrite the receipt's actual native audit, just as the SQL fixture
        // selects its exact receipt cursor, rather than rewriting presentation.
        if event.cursor == Some(cursor) {
            assert_eq!(
                event.correlation.as_deref(),
                Some(case.operation.0.as_str())
            );
            crate::continuation_input::tests::event(case, &receipt, &mut event);
            rewritten += 1;
        }
        state.events.append(event).unwrap();
    }
    assert_eq!(rewritten, 1);
    let saved = match domain {
        Domain::Approval => {
            crate::memory::OperationReceipt::Approval(serde_json::from_value(receipt).unwrap())
        }
        Domain::Tool => crate::memory::OperationReceipt::ToolContinuation(
            serde_json::from_value(receipt).unwrap(),
        ),
    };
    state
        .operation_receipts
        .insert((case.scope.session.clone(), case.operation.clone()), saved);
}

#[tokio::test]
async fn original_source_prevents_legacy_after_all_projection_indexes_are_erased() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    for domain in [Domain::Approval, Domain::Tool] {
        let case = fixture(&store, domain, false).await;
        {
            let mut state = store.state.lock().unwrap();
            state
                .continuation_inputs
                .erase_indexes_for_test(&case.scope);
            assert!(
                state
                    .continuation_inputs
                    .protocol(
                        &case.scope,
                        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20)
                    )
                    .is_err()
            );
            assert!(
                consistent(
                    &state,
                    domain,
                    &case.scope.session,
                    None,
                    &mut crate::public_session::v2::proof::Proof::bounded(32 << 20)
                )
                .is_err()
            );
        }
        match domain {
            Domain::Approval => assert!(
                store
                    .original_approval_plan(
                        &case.scope.session,
                        &case.scope.run,
                        &case.scope.continuation
                    )
                    .await
                    .is_err()
            ),
            Domain::Tool => assert!(
                store
                    .original_tool_continuation_plan(
                        &case.scope.session,
                        &case.scope.run,
                        &case.scope.continuation
                    )
                    .await
                    .is_err()
            ),
        }
    }
}
