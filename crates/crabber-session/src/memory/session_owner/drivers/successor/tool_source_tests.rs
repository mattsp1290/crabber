//! Original selected metadata remains independent of later mutable heads.
use super::*;
use crate::successor_driver_contract::tools::{Fixture, fixture};
use crabber_core::Clock;
use std::sync::Arc;

pub(super) fn request(store: &MemoryStore, f: &Fixture) -> RegisterSuccessorExecutionDriver {
    let join = f.joined.observed_grant().reconciliation().unwrap().join_id;
    let state = store.state.lock().unwrap();
    let selected: Vec<_> = state
        .native_run_issuances
        .values()
        .filter(|v| {
            v.state.session_id == *f.context.session_id()
                && v.state.run_id == f.native.fence.run_id
                && v.completion_source
                    .as_ref()
                    .is_some_and(|source| source.root_join_id == join)
        })
        .collect();
    assert_eq!(selected.len(), 1, "actual completion publisher");
    RegisterSuccessorExecutionDriver {
        context: f.context.clone(),
        fence: f.native.fence.clone(),
        claim: crate::session_owner::driver::NativeClaimLocator {
            issuance: selected[0].sequence,
            root_join_id: join,
        },
    }
}

#[tokio::test]
async fn tool_original_sources_authenticate_all_four_compound_origins() {
    for frontend in [false, true] {
        for recovery in [false, true] {
            for unkeyed in [false, true] {
                let clock = Arc::new(crabber_core::ManualClock::new(
                    time::OffsetDateTime::UNIX_EPOCH,
                ));
                let store = MemoryStore::with_clock(clock.clone());
                let f = Box::pin(fixture(&store, &clock, frontend, recovery, unkeyed)).await;
                let request = request(&store, &f);
                Box::pin(crate::successor_driver_contract::tools::inspect_captured(
                    &store, &f, &request, frontend, recovery,
                ))
                .await;
                {
                    let mut state = store.state.lock().unwrap();
                    let mut proof = Proof::bounded(32 << 20);
                    let selected = source::original(&state, &request, &mut proof, None).unwrap();
                    assert_eq!(
                        selected.tool,
                        Some((f.batch.continuation.clone(), f.batch.next_attempt.clone()))
                    );
                    assert_eq!(selected.revision, f.receipt.revision);
                    let used = proof.used();
                    source::original(&state, &request, &mut Proof::bounded(used), None).unwrap();
                    assert!(
                        source::original(&state, &request, &mut Proof::bounded(used - 1), None)
                            .is_err()
                    );
                    fresh(
                        &state,
                        &request,
                        None,
                        clock.now(),
                        &mut Proof::bounded(32 << 20),
                    )
                    .unwrap();
                    let original = state
                        .tool_continuations
                        .get(&f.batch.continuation.id)
                        .unwrap()
                        .clone();
                    if frontend {
                        crate::successor_driver_contract::tools::replace_frontend_result(
                            state
                                .tool_continuations
                                .get_mut(&f.batch.continuation.id)
                                .unwrap(),
                        );
                        assert!(
                            source::original(&state, &request, &mut Proof::bounded(32 << 20), None)
                                .is_err(),
                            "full original external content is authenticated"
                        );
                    }
                    state
                        .tool_continuations
                        .insert(f.batch.continuation.id.clone(), original);
                }
                Box::pin(crate::successor_driver_contract::tools::fresh::exercise(
                    &store, request,
                ))
                .await;
            }
        }
    }
}

#[tokio::test]
async fn tool_original_sources_require_protected_suspension_birth() {
    for frontend in [false, true] {
        let clock = Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = MemoryStore::with_clock(clock.clone());
        let f = Box::pin(fixture(&store, &clock, frontend, false, false)).await;
        let request = request(&store, &f);
        let mut state = store.state.lock().unwrap();
        source::original(&state, &request, &mut Proof::bounded(32 << 20), None).unwrap();
        // An honest predecessor has no protected20 cohort. Matching mutable
        // batch/receipts never upgrade it into an original input proof.
        state.continuation_inputs = crate::continuation_input::Ledger::default();
        assert!(matches!(
            source::original(&state, &request, &mut Proof::bounded(32 << 20), None),
            Err(Error::Unsupported)
        ));
    }
}

#[tokio::test]
async fn tool_original_source_refuses_coherent_target_rewrite() {
    for frontend in [false, true] {
        let clock = Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = MemoryStore::with_clock(clock.clone());
        let f = Box::pin(fixture(&store, &clock, frontend, false, false)).await;
        let request = request(&store, &f);
        let mut state = store.state.lock().unwrap();
        source::original(&state, &request, &mut Proof::bounded(32 << 20), None).unwrap();
        coherent_target_rewrite(&mut state, &f);
        assert!(source::original(&state, &request, &mut Proof::bounded(32 << 20), None).is_err());
    }
}

fn coherent_target_rewrite(state: &mut State, f: &Fixture) {
    let record = state
        .tool_continuations
        .get_mut(&f.batch.continuation.id)
        .unwrap();
    let replacement = crate::AttemptTarget {
        id: crabber_core::AttemptId::new(),
        generation: record.batch.next_attempt.generation,
    };
    record.batch.next_attempt = replacement.clone();
    for phase in &mut record.batch.phases {
        phase.next.attempt = replacement.clone();
    }
    for phase in &mut record.batch.frontend {
        phase.target.attempt = replacement.clone();
    }
    for call in &mut record.batch.calls {
        call.target.attempt = replacement.clone();
    }
    let suspension_digest = record
        .batch
        .fingerprint(&record.session_id, &record.run_id)
        .unwrap();
    let mut changed = Vec::new();
    for id in [&f.batch.id, &f.receipt.operation_id] {
        let Some(crate::memory::OperationReceipt::ToolContinuation(receipt)) = state
            .operation_receipts
            .get_mut(&(f.receipt.session_id.clone(), id.clone()))
        else {
            panic!("real native receipt");
        };
        if *id == f.batch.id {
            receipt.semantic_digest = suspension_digest.clone();
        } else {
            match &mut receipt.outcome {
                crate::ToolContinuationOutcome::Claimed { attempt, .. }
                | crate::ToolContinuationOutcome::FrontendClaimed { attempt, .. } => {
                    attempt.target = replacement.clone();
                }
                _ => unreachable!(),
            }
        }
        changed.push(receipt.clone());
    }
    let events: Vec<_> = state.events.iter().cloned().collect();
    state.events = crate::memory::events::EventLog::default();
    for mut event in events {
        if let Some(receipt) = changed
            .iter()
            .find(|r| event.cursor == Some(r.event_cursor))
        {
            let handoff = event.payload.get("handoff").cloned();
            event = crate::tool_continuation::audit(receipt, event.created_at);
            if let Some(handoff) = handoff {
                event.payload["handoff"] = handoff;
            }
            crate::tool_continuation::verify_receipt(receipt, &event).unwrap();
        }
        state.events.append(event).unwrap();
    }
}
