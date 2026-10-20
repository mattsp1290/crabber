//! A real late purge limit must discard the entire existing transaction candidate.
use super::*;
use crate::Store;
use crate::memory::deletion::readiness::fixed_initial_readiness::{completed, ready};

fn apply_saved(
    store: &MemoryStore,
    saved: &crate::deletion::Tombstone,
    proof: &mut Proof,
) -> Result<crate::DeletionReceipt, Abort> {
    store.transact(|state| {
        if ready(state, &saved.receipt.session_id, proof)?.is_some() {
            return Err(Abort::Domain(Error::UnknownStoreOutcome));
        }
        finish_delete(state, saved.clone(), proof)
    })
}
#[tokio::test]
async fn late_native_purge_limit_rolls_back_pending_owners_and_tombstone() {
    let (host, session) = completed(false, 1).await;
    let store = host.store();
    let genuine = store.state.lock().unwrap().clone();
    let request = DeleteSession {
        id: crabber_core::ControlId::new(),
        session_id: session.clone(),
        expected_revision: store.session_revision(&session).await.unwrap(),
    };
    assert!(matches!(
        store.delete_session(request).await.unwrap(),
        DeletionOutcome::Applied(_)
    ));
    // Retain the actual validated tombstone from that successful original
    // deletion. Reusing its exact time avoids a test budget that varies with
    // SystemClock timestamp digit widths; no clock/eligibility is injected.
    let saved = store
        .state
        .lock()
        .unwrap()
        .tombstones
        .get(&session)
        .unwrap()
        .clone();
    *store.state.lock().unwrap() = genuine.clone();
    let mut baseline = Proof::bounded(MAX_PROOF_BYTES);
    assert!(apply_saved(store, &saved, &mut baseline).is_ok());
    let n = baseline.used();
    *store.state.lock().unwrap() = genuine.clone();
    let mut partial = genuine.clone();
    let mut short = Proof::bounded(n - 1);
    assert_eq!(ready(&partial, &session, &mut short).unwrap(), None);
    assert!(matches!(
        finish_delete(&mut partial, saved.clone(), &mut short),
        Err(Abort::ProofLimit)
    ));
    assert!(
        !partial.pending_initial.contains_scope(&session),
        "the actual Pending purge occurred before the late failure"
    );
    assert!(
        partial.session_owners.contains_scope(&session),
        "native owner preflight refused before erasure"
    );
    assert!(partial.tombstones.contains_key(&session));
    let native_before = genuine.session_owners.raw_initial_snapshot(&session);
    let mut short = Proof::bounded(n - 1);
    assert!(matches!(
        apply_saved(store, &saved, &mut short),
        Err(Abort::ProofLimit)
    ));
    let state = store.state.lock().unwrap();
    assert!(!state.tombstones.contains_key(&session));
    assert!(state.pending_initial.contains_scope(&session));
    assert!(state.initial_consumer.contains_scope(&session));
    assert_eq!(
        state.session_owners.raw_initial_snapshot(&session),
        native_before
    );
    assert_eq!(state.runs.len(), genuine.runs.len());
    assert_eq!(
        ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
        None
    );
}

#[tokio::test]
async fn actual_guarded_entry_maps_finish_limit_outside_transaction() {
    let (host, session) = completed(true, 1).await;
    let store = host.store();
    let mut readiness = Proof::bounded(MAX_PROOF_BYTES);
    {
        let state = store.state.lock().unwrap();
        assert_eq!(ready(&state, &session, &mut readiness).unwrap(), None);
    }
    let native_before = store
        .state
        .lock()
        .unwrap()
        .session_owners
        .raw_initial_snapshot(&session);
    let revision = store.session_revision(&session).await.unwrap();
    let request = DeleteSession {
        id: crabber_core::ControlId::new(),
        session_id: session.clone(),
        expected_revision: revision,
    };
    let outcome = store
        .guarded_delete_with_proof(request, &mut Proof::bounded(readiness.used()))
        .unwrap();
    assert_eq!(outcome, DeletionOutcome::Blocked(Blocker::ProofLimit));
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    let state = store.state.lock().unwrap();
    assert!(!state.tombstones.contains_key(&session));
    assert!(state.pending_initial.contains_scope(&session));
    assert_eq!(
        state.session_owners.raw_initial_snapshot(&session),
        native_before
    );
    assert_eq!(
        ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
        None
    );
}
