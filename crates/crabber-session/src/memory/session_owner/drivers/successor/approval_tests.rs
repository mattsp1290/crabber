//! Actual original sources remain strict under decoder-valid current rewrites.
use super::*;
use crate::Store;
use crate::successor_driver_contract::approval::{fixture_input, integrity};
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn approval_successor_source_integrity() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let f = fixture_input(&store, &clock, recovery, unkeyed).await;
        let revision = integrity::advance(&store, &f).await;
        {
            let mut state = store.state.lock().unwrap();
            let saved = state.clone();
            let mut receipt = f.receipt.clone();
            receipt.revision = revision;
            state.operation_receipts.insert(
                (receipt.session_id.clone(), receipt.operation_id.clone()),
                crate::memory::OperationReceipt::Approval(receipt.clone()),
            );
            let events: Vec<_> = state.events.iter().cloned().collect();
            state.events = crate::memory::events::EventLog::default();
            for mut event in events {
                if event.cursor == Some(receipt.event_cursor) {
                    integrity::refreshed_event(&mut event, &receipt);
                }
                state.events.append(event).unwrap();
            }
            assert!(
                source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None).is_err()
            );
            assert!(
                fresh(
                    &state,
                    &f.request,
                    None,
                    clock.now(),
                    &mut Proof::bounded(32 << 20)
                )
                .is_err()
            );
            *state = saved;
        }

        let f = fixture_input(&store, &clock, recovery, unkeyed).await;
        {
            let mut state = store.state.lock().unwrap();
            let saved = state.clone();
            let run = &f.request.fence.run_id;
            let mut binding =
                crate::RetainedExecutionBinding::decode(&state.execution_bindings[run]).unwrap();
            integrity::rewrite_binding(&mut binding);
            state.runs.get_mut(run).unwrap().config_hash =
                binding.capsule.config_hash.as_str().into();
            state
                .execution_bindings
                .insert(run.clone(), serde_json::to_string(&binding).unwrap());
            // The legacy mutable-pair reader accepts the internally consistent rewrite.
            crate::memory::execution_binding::read(&state, f.request.context.session_id(), run)
                .unwrap()
                .unwrap();
            assert!(
                source::original(
                    &state,
                    &f.request,
                    &mut Proof::bounded(32 << 20),
                    Some(&binding.capsule)
                )
                .is_err()
            );
            assert!(
                fresh(
                    &state,
                    &f.request,
                    None,
                    clock.now(),
                    &mut Proof::bounded(32 << 20)
                )
                .is_err()
            );
            *state = saved;
        }
        Box::pin(original_metadata(&store, &f)).await;
    }
}

#[tokio::test]
async fn approval_successor_begin_restore_and_parent_swap() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    for recovery in [false, true] {
        let f = fixture_input(&store, &clock, recovery, false).await;
        let current = store.state.lock().unwrap().attempts[&f.batch.next_attempt.id].clone();
        let begun = integrity::begin(&store, &f).await;
        let mut state = store.state.lock().unwrap();
        state
            .attempts
            .insert(f.batch.next_attempt.id.clone(), current);
        state
            .operation_receipts
            .remove(&(begun.attempt.session_id.clone(), begun.operation_id));
        state.events.remove_for_test(begun.event_cursor).unwrap();
        let run = &state.runs[&f.request.fence.run_id];
        source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None).unwrap();
        assert!(
            approval::fresh(
                &state,
                run,
                &f.batch.continuation,
                &f.batch.next_attempt,
                clock.now(),
                &mut Proof::bounded(32 << 20)
            )
            .is_err()
        );
        state
            .native_parent_witnesses
            .get_mut(&begun.event_cursor)
            .unwrap()
            .source
            .attempt
            .id = crabber_core::AttemptId(String::new());
        assert!(
            approval::fresh(
                &state,
                &state.runs[&f.request.fence.run_id],
                &f.batch.continuation,
                &f.batch.next_attempt,
                clock.now(),
                &mut Proof::bounded(32 << 20)
            )
            .is_err()
        );
    }
    for unkeyed in [false, true] {
        let store = MemoryStore::with_clock(clock.clone());
        let f = fixture_input(&store, &clock, true, unkeyed).await;
        let mut original = store
            .lookup_approval(&f.receipt.session_id, &f.original.id)
            .await
            .unwrap()
            .unwrap();
        let old_id = original.operation_id.clone();
        let mut recovery = f.receipt.clone();
        integrity::replace_parent(&mut original, &mut recovery);
        let mut state = store.state.lock().unwrap();
        state
            .operation_receipts
            .remove(&(original.session_id.clone(), old_id));
        for r in [&original, &recovery] {
            state.operation_receipts.insert(
                (r.session_id.clone(), r.operation_id.clone()),
                crate::memory::OperationReceipt::Approval(r.clone()),
            );
        }
        let events: Vec<_> = state.events.iter().cloned().collect();
        state.events = crate::memory::events::EventLog::default();
        for mut event in events {
            if event.cursor == Some(original.event_cursor) {
                integrity::synchronize(&mut event, &original);
            } else if event.cursor == Some(recovery.event_cursor) {
                integrity::synchronize(&mut event, &recovery);
            }
            state.events.append(event).unwrap();
        }
        assert!(source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None).is_err());
    }
}

async fn original_metadata(
    store: &MemoryStore,
    f: &crate::successor_driver_contract::approval::Fixture,
) {
    let run = &f.request.fence.run_id;
    let crate::session_owner::driver::SuccessorExecutionDriverOutcome::Applied { receipt, .. } =
        store
            .register_successor_execution_driver(f.request.clone())
            .await
            .unwrap()
    else {
        panic!()
    };
    let saved = {
        let mut state = store.state.lock().unwrap();
        let saved = state.clone();
        state.runs.get_mut(run).unwrap().status = crabber_core::RunStatus::Completed;
        state
            .approvals
            .get_mut(&f.batch.continuation.id)
            .unwrap()
            .state = crate::ApprovalContinuationState::Waiting;
        saved
    };
    assert!(
        matches!(store.register_successor_execution_driver(f.request.clone()).await.unwrap(),
            crate::session_owner::driver::SuccessorExecutionDriverOutcome::Replayed(actual) if actual == receipt)
    );
    *store.state.lock().unwrap() = saved;
}

#[tokio::test]
async fn approval_successor_static_suspension_sources() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let f = fixture_input(&store, &clock, recovery, unkeyed).await;
        let mut state = store.state.lock().unwrap();
        let saved = state.clone();
        let id = crate::approval::derived_id("suspend-attempt", &f.batch.id);
        let key = (f.receipt.session_id.clone(), id);
        let crate::memory::OperationReceipt::Attempt(native) = &state.operation_receipts[&key]
        else {
            panic!()
        };
        let frame = state
            .events
            .iter()
            .rev()
            .find(|event| {
                event.session_id == f.receipt.session_id
                    && event.cursor.is_some_and(|c| c < native.event_cursor)
            })
            .unwrap()
            .cursor
            .unwrap();
        let begin = state
            .native_parent_witnesses
            .values()
            .find(|w| {
                w.source.run_id == f.receipt.run_id
                    && w.source.attempt == f.batch.attempt
                    && w.source.kind == crate::native_parent::Kind::Attempt
            })
            .unwrap();
        let replacement = state.operation_receipts[&(
            f.receipt.session_id.clone(),
            begin.source.operation_id.clone(),
        )]
            .clone();
        for change in 0..4 {
            match change {
                0 => {
                    state.operation_receipts.remove(&key);
                }
                1 => {
                    state
                        .operation_receipts
                        .insert(key.clone(), replacement.clone());
                }
                2 => {
                    let events: Vec<_> = state.events.iter().cloned().collect();
                    state.events = crate::memory::events::EventLog::default();
                    for mut event in events {
                        if event.cursor == Some(frame) {
                            event.payload["usage"]["input_tokens"] = serde_json::json!(999);
                        }
                        state.events.append(event).unwrap();
                    }
                }
                _ => {
                    state.events.remove_for_test(frame).unwrap();
                }
            }
            assert!(
                source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None).is_err()
            );
            *state = saved.clone();
        }
    }
}

#[tokio::test]
async fn approval_missing_protected_input_is_corruption() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    for recovery in [false, true] {
        let f = fixture_input(&store, &clock, recovery, false).await;
        let mut state = store.state.lock().unwrap();
        state
            .preparation_admissions
            .remove(&f.request.fence.run_id)
            .unwrap();
        assert!(matches!(
            source::original(&state, &f.request, &mut Proof::bounded(32 << 20), None),
            Err(Error::UnknownStoreOutcome)
        ));
    }
}
