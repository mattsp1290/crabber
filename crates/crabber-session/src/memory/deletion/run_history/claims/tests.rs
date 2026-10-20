//! Genuine repeated recovery exposes the local-association/history distinction.
use super::*;
use crate::deletion::{coverage::MAX_PROOF_BYTES, history::Fact};
use std::sync::Arc;
fn proof() -> Proof {
    Proof::bounded(MAX_PROOF_BYTES)
}
#[tokio::test]
async fn ordered_claim_sources_reject_borrowing_but_never_infer_erased_history_complete() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let f =
        crate::session_owner::preparation_contract::claim_history::fixture(&store, &clock).await;
    let mut state = store.state.lock().unwrap();
    let session = &f.before_tail.session_id;
    let run = &f.before_tail.id;
    let selected = || {
        state
            .native_run_issuances
            .iter()
            .filter(|((s, r, _), _)| s == session && r == run)
            .filter(|(_, issuer)| {
                issuer
                    .completion_source
                    .as_ref()
                    .is_some_and(|s| s.native_operation == CompletionOperation::ToolRecovery)
            })
            .map(|(key, issuer)| (key.clone(), issuer.clone()))
            .collect::<Vec<_>>()
    };
    let recoveries = selected();
    assert_eq!(recoveries.len(), 2);
    let (first_key, first) = &recoveries[0];
    let (tail_key, tail) = &recoveries[1];
    let before = state
        .native_run_issuances
        .get(&(session.clone(), run.clone(), first.sequence - 1))
        .unwrap()
        .clone();
    let (_, claims) = read(&state, session, run, &mut proof()).unwrap();
    assert_eq!(claims.retained_sources, Fact::Authenticated);
    assert_eq!(claims.complete, Fact::Unproven);
    let mut borrowed = first.clone();
    borrowed.completion_source = tail.completion_source.clone();
    // The qualified local verifier really accepts this other genuine association.
    assert_eq!(
        source(&state, Some(&before), &borrowed, &mut proof()).unwrap(),
        Some(f.tail.event_cursor)
    );
    state
        .native_run_issuances
        .insert(first_key.clone(), borrowed);
    assert!(matches!(
        read(&state, session, run, &mut proof()),
        Err(Failure::Error(_))
    ));
    state
        .native_run_issuances
        .insert(first_key.clone(), first.clone());
    // Restore the actual earlier Run and erase the entire final claim source.
    // The EventLog highwater remains monotonic, but is not a per-Run roster.
    let next = state.events.next_cursor().unwrap();
    state.native_run_issuances.remove(tail_key);
    state
        .operation_receipts
        .remove(&(session.clone(), f.tail.operation_id.clone()))
        .unwrap();
    state.events.remove_for_test(f.tail.event_cursor).unwrap();
    state.runs.insert(run.clone(), f.before_tail.clone());
    assert_eq!(state.events.next_cursor().unwrap(), next);
    let (_, claims) = read(&state, session, run, &mut proof()).unwrap();
    assert_eq!(claims.retained_sources, Fact::Authenticated);
    assert_eq!(claims.complete, Fact::Unproven);
    assert!(
        state
            .native_run_issuances
            .values()
            .filter(|issuer| &issuer.state.session_id == session && &issuer.state.run_id == run)
            .max_by_key(|issuer| issuer.sequence)
            .unwrap()
            .state
            == crate::native_parent::RunState::from_run(&state.runs[run]).unwrap()
    );
    assert!(
        state
            .operation_receipts
            .contains_key(&(session.clone(), f.first.operation_id))
    );
}
