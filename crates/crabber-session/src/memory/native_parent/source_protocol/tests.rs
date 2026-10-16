//! The retained head survives complete projection and control-source erasure.
use super::*;
use crate::Store;
use crate::deletion::coverage::MAX_PROOF_BYTES;
use crate::public_session::v2::proof::{Failure, Proof};
use std::sync::Arc;

fn proof() -> Proof {
    Proof::bounded(MAX_PROOF_BYTES)
}

#[tokio::test]
async fn protected_source_head_detects_erased_tail_and_restored_current_run() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let f =
        crate::session_owner::preparation_contract::claim_history::fixture(&store, &clock).await;
    let mut state = store.state.lock().unwrap();
    let session = &f.before_tail.session_id;
    let run = &f.before_tail.id;
    let mut exact = proof();
    assert!(history(&state, session, run, &mut exact).unwrap());
    assert!(
        current(&state, session, run, &mut proof())
            .unwrap()
            .is_some()
    );
    let cost = exact.used();
    assert!(history(&state, session, run, &mut Proof::bounded(cost)).unwrap());
    assert!(matches!(
        history(&state, session, run, &mut Proof::bounded(cost - 1)),
        Err(Failure::Limited(_))
    ));
    let scope = (session.clone(), run.clone());
    let head = state.native_run_source_heads[&scope].clone();
    let key = (session.clone(), run.clone(), head.sequence);
    let issuer = state.native_run_issuances.remove(&key).unwrap();
    state
        .operation_receipts
        .remove(&(session.clone(), f.tail.operation_id))
        .unwrap();
    state.events.remove_for_test(f.tail.event_cursor).unwrap();
    state.runs.insert(run.clone(), f.before_tail.clone());
    // Original metadata is still exact even though later projection evidence
    // is damaged; no fresh-dispatch or full-history claim follows from it.
    assert!(
        original(&state, session, run, 1, &mut proof())
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        history(&state, session, run, &mut proof()),
        Err(Failure::Error(_))
    ));
    // Even deleting the matching source slot cannot hide the independent head.
    let source = state.native_run_sources.remove(&key).unwrap();
    assert!(matches!(
        history(&state, session, run, &mut proof()),
        Err(Failure::Error(_))
    ));
    state.native_run_sources.insert(key.clone(), source);
    state.native_run_issuances.insert(key, issuer);
    assert!(history(&state, session, run, &mut proof()).unwrap());
    state
        .native_run_source_heads
        .get_mut(&scope)
        .unwrap()
        .version = 999;
    assert!(matches!(
        current(&state, session, run, &mut proof()),
        Err(Failure::Error(_))
    ));
    assert!(
        original(&state, session, run, 1, &mut proof())
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        history(&state, session, run, &mut proof()),
        Err(Failure::Error(_))
    ));
    state.native_run_source_heads.insert(scope.clone(), head);
    routes(&state, session, &mut proof()).unwrap();
    state.runs.remove(run);
    state
        .native_run_issuances
        .retain(|(scope, id, _), _| scope != session || id != run);
    assert!(matches!(
        routes(&state, session, &mut proof()),
        Err(Failure::Error(_))
    ));
}

#[tokio::test]
async fn protected_source_digest_rejects_projection_rewrite_without_freezing_projection() {
    let store = crate::MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    let key = (receipt.session_id.clone(), receipt.run_id.clone(), 1);
    assert!(history(&state, &receipt.session_id, &receipt.run_id, &mut proof()).unwrap());
    let mut metadata = proof();
    assert!(
        original(
            &state,
            &receipt.session_id,
            &receipt.run_id,
            1,
            &mut metadata
        )
        .unwrap()
        .is_some()
    );
    let bytes = metadata.used();
    assert!(
        original(
            &state,
            &receipt.session_id,
            &receipt.run_id,
            1,
            &mut Proof::bounded(bytes)
        )
        .unwrap()
        .is_some()
    );
    assert!(matches!(
        original(
            &state,
            &receipt.session_id,
            &receipt.run_id,
            1,
            &mut Proof::bounded(bytes - 1)
        ),
        Err(Failure::Limited(_))
    ));
    let saved = state.native_run_issuances[&key].clone();
    state
        .native_run_issuances
        .get_mut(&key)
        .unwrap()
        .state
        .token = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
    assert!(matches!(
        history(&state, &receipt.session_id, &receipt.run_id, &mut proof()),
        Err(Failure::Error(_))
    ));
    assert!(matches!(
        original(
            &state,
            &receipt.session_id,
            &receipt.run_id,
            1,
            &mut proof()
        ),
        Err(Failure::Error(_))
    ));
    state.native_run_issuances.insert(key, saved);
    state
        .native_run_protocols
        .remove(&(receipt.session_id.clone(), receipt.run_id.clone()));
    assert!(matches!(
        history(&state, &receipt.session_id, &receipt.run_id, &mut proof()),
        Err(Failure::Error(_))
    ));
}

#[tokio::test]
async fn later_native_write_preserves_legacy_without_promoting_source_membership() {
    let store = crate::MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    {
        // Retained pre-protocol state has ordinary issuances but no new cohort.
        let mut state = store.state.lock().unwrap();
        state
            .native_run_protocols
            .remove(&(receipt.session_id.clone(), receipt.run_id.clone()));
        state
            .native_run_source_heads
            .remove(&(receipt.session_id.clone(), receipt.run_id.clone()));
        state
            .native_run_sources
            .retain(|(scope, run, _), _| scope != &receipt.session_id || run != &receipt.run_id);
    }
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    store
        .abandon_run(crabber_core::AbandonRequest {
            expected: crabber_core::RunFence {
                run_id: run.id,
                claim_token: run.claim_token,
            },
            expected_owner: run.owner,
            authority: crabber_core::AbandonAuthority::HostStoppedOwner,
        })
        .await
        .unwrap();
    let state = store.state.lock().unwrap();
    assert!(!history(&state, &receipt.session_id, &receipt.run_id, &mut proof()).unwrap());
    assert!(
        original(
            &state,
            &receipt.session_id,
            &receipt.run_id,
            1,
            &mut proof()
        )
        .unwrap()
        .is_some()
    );
    assert!(
        !state
            .native_run_protocols
            .contains(&(receipt.session_id.clone(), receipt.run_id.clone()))
    );
    assert!(
        !state
            .native_run_source_heads
            .contains_key(&(receipt.session_id.clone(), receipt.run_id.clone()))
    );
}

#[tokio::test]
async fn protected_source_scope_cannot_hide_under_another_session_key() {
    let store = crate::MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    let mut issuer = state.native_run_issuances
        [&(receipt.session_id.clone(), receipt.run_id.clone(), 1)]
        .clone();
    issuer.state.session_id = crabber_core::SessionId::new();
    let source = JournalSource::new(None, &issuer).unwrap();
    state
        .native_run_sources
        .insert((issuer.state.session_id, receipt.run_id.clone(), 1), source);
    assert!(matches!(
        history(&state, &receipt.session_id, &receipt.run_id, &mut proof()),
        Err(Failure::Error(_))
    ));
    assert!(matches!(
        routes(&state, &receipt.session_id, &mut proof()),
        Err(Failure::Error(_))
    ));
}

#[tokio::test]
async fn missing_fresh_projection_cannot_fall_back_to_legacy_status_write() {
    let store = crate::MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    let previous = crate::native_parent::RunState::from_run(&state.runs[&receipt.run_id]).unwrap();
    let mut next = previous.clone();
    next.status = crabber_core::RunStatus::Completed;
    state
        .native_run_issuances
        .remove(&(receipt.session_id.clone(), receipt.run_id.clone(), 1))
        .unwrap();
    // This is genuinely the old compatibility path: unchanged token and no
    // retained issuer produce no new legacy issuance, even as status changes.
    assert!(
        crate::native_parent::advance(Some(&previous), next.clone(), None)
            .unwrap()
            .is_none()
    );
    assert!(super::super::record_run_states(&mut state, Some(&previous), next).is_err());
    assert_eq!(
        state.native_run_source_heads[&(receipt.session_id, receipt.run_id)].sequence,
        1
    );
}
