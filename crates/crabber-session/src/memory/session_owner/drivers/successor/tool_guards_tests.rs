//! Decoder-valid current mutations cannot replace the protected original phase set.
use super::*;
use crate::successor_driver_contract::tools::fixture;
use crabber_core::Clock;
use std::sync::Arc;

#[tokio::test]
async fn tool_fresh_guards_reject_current_carrier_mutations_and_budget_overflow() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    for (frontend, mixed) in [(false, false), (true, false), (true, true)] {
        for recovery in [false, true] {
            let f = if mixed {
                Box::pin(crate::successor_driver_contract::tools::fixture_mixed(
                    &store, &clock, recovery, false,
                ))
                .await
            } else {
                Box::pin(fixture(&store, &clock, frontend, recovery, false)).await
            };
            let request = super::tool_source_tests::request(&store, &f);
            let original = { store.state.lock().unwrap().clone() };
            let mut proof = Proof::bounded(32 << 20);
            fresh(&original, &request, None, clock.now(), &mut proof).unwrap();
            fresh(
                &original,
                &request,
                None,
                clock.now(),
                &mut Proof::bounded(proof.used()),
            )
            .unwrap();
            assert!(
                fresh(
                    &original,
                    &request,
                    None,
                    clock.now(),
                    &mut Proof::bounded(proof.used() - 1)
                )
                .is_err()
            );
            for mutation in 0..10 {
                let mut changed = original.clone();
                mutate(&mut changed, &f, mutation);
                assert!(
                    fresh(
                        &changed,
                        &request,
                        None,
                        clock.now(),
                        &mut Proof::bounded(32 << 20)
                    )
                    .is_err(),
                    "frontend={frontend} recovery={recovery} mutation={mutation}"
                );
            }
            if mixed {
                Box::pin(crate::successor_driver_contract::tools::fresh::exercise(
                    &store, request,
                ))
                .await;
            }
        }
    }
}
fn mutate(state: &mut State, f: &crate::successor_driver_contract::tools::Fixture, mutation: u8) {
    let (target, _) =
        crate::session_owner::driver::successor::tool_fresh::expected_phases(&f.batch)
            .next()
            .unwrap();
    match mutation {
        0 => {
            state.phases.remove(&target.id).unwrap();
        }
        1 => state.phases.get_mut(&target.id).unwrap().snapshot.state = crate::PhaseState::Started,
        2 => {
            state
                .phases
                .get_mut(&target.id)
                .unwrap()
                .snapshot
                .quiescence = crate::PhaseQuiescence::Complete;
        }
        3 => {
            state
                .phases
                .get_mut(&target.id)
                .unwrap()
                .snapshot
                .call_binding = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        }
        4 => state.phases.get_mut(&target.id).unwrap().token.clear(),
        5 => {
            state.calls.get_mut(&target.call_id).unwrap().status =
                crabber_core::ToolCallStatus::Running;
        }
        6 => {
            state
                .attempts
                .get_mut(&f.batch.next_attempt.id)
                .unwrap()
                .snapshot
                .state = crate::AttemptState::Started;
        }
        7 => {
            state.tool_barriers.remove(&f.receipt.session_id).unwrap();
        }
        8 => {
            state
                .tool_continuations
                .get_mut(&f.batch.continuation.id)
                .unwrap()
                .state = crate::ToolContinuationState::Waiting;
        }
        9 => {
            state
                .attempts
                .get_mut(&f.batch.attempt.id)
                .unwrap()
                .snapshot
                .cleanup = crate::AttemptCleanup::Pending;
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn tool_fresh_phase_set_rejects_same_count_ordinary_call_substitution() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = Box::pin(fixture(&store, &clock, true, false, false)).await;
    let request = super::tool_source_tests::request(&store, &f);
    let mut state = store.state.lock().unwrap();
    let expected = &f.batch.frontend[0].target;
    let ordinary = f
        .batch
        .calls
        .iter()
        .find(|v| v.target.call_id != expected.call_id)
        .unwrap();
    let mut replacement = state.phases.remove(&expected.id).unwrap();
    replacement.snapshot.target = ordinary.target.clone();
    replacement.snapshot.call_binding = ordinary.call_binding.clone();
    replacement.validate().unwrap();
    state.phases.insert(ordinary.target.id.clone(), replacement);
    assert_eq!(
        state
            .phases
            .values()
            .filter(|v| v.snapshot.target.attempt == f.batch.next_attempt)
            .count(),
        f.batch.frontend.len()
    );
    assert!(
        fresh(
            &state,
            &request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .is_err()
    );
}

#[tokio::test]
async fn tool_fresh_binds_latest_projection_to_actual_selected_receipt() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = Box::pin(fixture(&store, &clock, false, false, false)).await;
    let request = super::tool_source_tests::request(&store, &f);
    let mut state = store.state.lock().unwrap();
    let original = state
        .tool_continuations
        .get(&f.batch.continuation.id)
        .unwrap()
        .claim
        .clone()
        .unwrap();
    let recovery = crate::ToolContinuationRecovery {
        id: crabber_core::ControlId::new(),
        expected_revision: f.receipt.revision,
        original,
        attempt: f.batch.next_attempt.clone(),
        owner: "forged-later".into(),
        lease_ms: 30000,
    };
    let receipt = crate::ToolContinuationReceipt {
        session_id: f.receipt.session_id.clone(),
        run_id: f.receipt.run_id.clone(),
        operation_id: recovery.id.clone(),
        semantic_digest: recovery.fingerprint().unwrap(),
        revision: f.receipt.revision,
        event_cursor: state.events.next_cursor().unwrap(),
        outcome: crate::ToolContinuationOutcome::ReclaimedUnstarted {
            continuation: f.batch.continuation.clone(),
            original_claim_id: recovery.original.id.clone(),
            attempt: state
                .attempts
                .get(&f.batch.next_attempt.id)
                .unwrap()
                .snapshot
                .clone(),
        },
    };
    let mut event = crate::tool_continuation::audit(&receipt, clock.now());
    event.cursor = Some(receipt.event_cursor);
    state.events.append(event).unwrap();
    state.operation_receipts.insert(
        (f.receipt.session_id.clone(), recovery.id.clone()),
        crate::memory::OperationReceipt::ToolContinuation(receipt),
    );
    state
        .tool_continuations
        .get_mut(&f.batch.continuation.id)
        .unwrap()
        .recovery = Some(recovery);
    // Existing current source validation accepts this coherent mutable rewrite.
    crate::memory::tool_continuation::checked_record_with_proof(
        &state,
        &f.batch.continuation.id,
        &mut Proof::bounded(32 << 20),
    )
    .unwrap();
    assert!(
        fresh(
            &state,
            &request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .is_err()
    );
}

#[tokio::test]
async fn tool_fresh_mixed_ordinary_later_phase_is_not_covered_by_phase_count() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = Box::pin(crate::successor_driver_contract::tools::fixture_mixed(
        &store, &clock, false, false,
    ))
    .await;
    let request = super::tool_source_tests::request(&store, &f);
    let mut state = store.state.lock().unwrap();
    let ordinary = f
        .batch
        .calls
        .iter()
        .find(|v| {
            !crate::session_owner::driver::successor::tool_fresh::expected_phases(&f.batch)
                .any(|(target, _)| *target == v.target)
        })
        .unwrap();
    let mut extra = state
        .phases
        .get(&f.batch.frontend[0].target.id)
        .unwrap()
        .clone();
    extra.snapshot.target = crate::PhaseTarget {
        id: crabber_core::PhaseId::new(),
        generation: ordinary.target.generation.next().unwrap(),
        call_id: ordinary.target.call_id.clone(),
        attempt: f.batch.attempt.clone(),
    };
    extra.snapshot.call_binding = ordinary.call_binding.clone();
    extra.validate().unwrap();
    state.phases.insert(extra.snapshot.target.id.clone(), extra);
    crate::memory::tool_continuation::checked_record_with_proof(
        &state,
        &f.batch.continuation.id,
        &mut Proof::bounded(32 << 20),
    )
    .unwrap();
    assert!(
        fresh(
            &state,
            &request,
            None,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        )
        .is_err()
    );
}

#[tokio::test]
async fn tool_fresh_extra_unfinished_call_cannot_hide_outside_frozen_set() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = Box::pin(crate::successor_driver_contract::tools::fixture_mixed(
        &store, &clock, false, false,
    ))
    .await;
    let request = super::tool_source_tests::request(&store, &f);
    let mut state = store.state.lock().unwrap();
    for status in [
        crabber_core::ToolCallStatus::Waiting,
        crabber_core::ToolCallStatus::Pending,
        crabber_core::ToolCallStatus::Running,
    ] {
        let extra = crabber_core::ToolCallRecord {
            id: crabber_core::ToolCallId::new(),
            run_id: f.receipt.run_id.clone(),
            name: "extra".into(),
            arguments: serde_json::json!({}),
            status,
            retry_safe: false,
            result: None,
        };
        state.calls.insert(extra.id.clone(), extra);
        assert!(
            fresh(
                &state,
                &request,
                None,
                clock.now(),
                &mut Proof::bounded(32 << 20)
            )
            .is_err()
        );
    }
}
