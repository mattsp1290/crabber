use crate::session_owner::preparation::{
    ExpiredRunOrigin, ExpiredRunOutcome, PreparationCompletion,
};
use crate::{MemoryStore, Store};
use crabber_core::ManualClock;
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn expired_preparation_faults_and_sources() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    crate::run_control_contract::expired::faults(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    crate::run_control_contract::expired::unkeyed::faults(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    crate::run_control_contract::expired::unkeyed::lost_admission_ack(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    crate::run_control_contract::expired::unstarted::faults(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    verify_begin_guard(&store, &clock).await;
    let pending = crate::run_control_contract::expired::pending(&store, &clock).await;
    let source = &pending.source;
    let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, .. }) =
        Store::complete_preparation(&store, pending.joined.clone())
            .await
            .unwrap()
    else {
        panic!("fresh claim")
    };
    let session = source.request().session_id.clone();
    let run = source.request().expected_fence.run_id.clone();
    let original = store.state.lock().unwrap().runs.get(&run).unwrap().clone();
    let mut oversized = original.clone();
    oversized.checkpoint =
        Some(serde_json::json!({"unused":"x".repeat(crate::MAX_RUN_BOUNDARY_BYTES+32768)}));
    store
        .state
        .lock()
        .unwrap()
        .runs
        .insert(run.clone(), oversized);
    assert_eq!(
        store
            .lookup_expired_run_preparation(&session, &run, source.expected_issuance())
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    let mut request = source.request().clone();
    request
        .expected_fence
        .claim_token
        .clone_from(&original.claim_token);
    clock.set(original.lease_until);
    assert!(
        store
            .inspect_expired_run_preparation(request)
            .await
            .is_err()
    );
    store
        .state
        .lock()
        .unwrap()
        .runs
        .insert(run.clone(), original);
    verify_original_sources(&store, source, receipt).await;
}
async fn verify_original_sources(
    store: &MemoryStore,
    source: &crate::session_owner::preparation::ExpiredRunPreparation,
    receipt: crate::session_owner::preparation::ExpiredRunReceipt,
) {
    let session = source.request().session_id.clone();
    let run = source.request().expected_fence.run_id.clone();
    let ExpiredRunOrigin::Suspended(id) = &source.target().origin else {
        panic!("suspended source")
    };
    let original_receipt = store
        .state
        .lock()
        .unwrap()
        .operation_receipts
        .remove(&(session.clone(), id.clone()))
        .unwrap();
    assert!(
        store
            .lookup_expired_run_preparation(&session, &run, source.expected_issuance())
            .await
            .is_err()
    );
    store
        .state
        .lock()
        .unwrap()
        .operation_receipts
        .insert((session.clone(), id.clone()), original_receipt);
    let key = (session.clone(), run.clone(), source.expected_issuance() + 1);
    let original_issuer = store
        .state
        .lock()
        .unwrap()
        .native_run_issuances
        .get(&key)
        .unwrap()
        .clone();
    let mut changed = original_issuer.clone();
    changed
        .completion_source
        .as_mut()
        .unwrap()
        .native_semantic_digest = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    store
        .state
        .lock()
        .unwrap()
        .native_run_issuances
        .insert(key.clone(), changed);
    assert!(
        store
            .lookup_expired_run_preparation(&session, &run, source.expected_issuance())
            .await
            .is_err()
    );
    store
        .state
        .lock()
        .unwrap()
        .native_run_issuances
        .insert(key, original_issuer);
    // A cold controller has only the original metadata key, no joined proof.
    let cold = store.clone();
    assert_eq!(
        cold.lookup_expired_run_preparation(&session, &run, source.expected_issuance())
            .await
            .unwrap(),
        Some(receipt)
    );
}

async fn verify_begin_guard(store: &MemoryStore, clock: &ManualClock) {
    let (source, reservation, authority) =
        crate::run_control_contract::expired::reserved(store, clock).await;
    let request = crate::run_control_contract::expired::begin_request(&reservation, &authority);
    let run_id = &source.request().expected_fence.run_id;
    let original = store
        .state
        .lock()
        .unwrap()
        .runs
        .get(run_id)
        .unwrap()
        .clone();
    let mut changed = original.clone();
    changed.checkpoint =
        Some(serde_json::json!({"unused":"x".repeat(crate::MAX_RUN_BOUNDARY_BYTES+32768)}));
    store
        .state
        .lock()
        .unwrap()
        .runs
        .insert(run_id.clone(), changed);
    assert!(
        store
            .mutate_session_owner(request.clone(), Some(authority.clone()))
            .await
            .is_err()
    );
    store
        .state
        .lock()
        .unwrap()
        .runs
        .insert(run_id.clone(), original);
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .erase_historical_audit_for_test(&request.session_id, &reservation.owner.id);
    assert!(
        store
            .mutate_session_owner(request, Some(authority))
            .await
            .is_err()
    );
    assert!(
        Store::reserve_preparation(store, reservation)
            .await
            .is_err()
    );
}
