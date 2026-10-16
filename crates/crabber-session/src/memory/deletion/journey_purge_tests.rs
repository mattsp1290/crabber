//! These are physical-storage fixtures, not claims that public deletion is ready.
use super::super::MemoryStore;
use crate::Store;
use crate::public_session::v2::proof::Proof;

#[tokio::test]
async fn physical_purge_erases_real_native_checkpoint_history_and_private_payloads() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let f = crate::tool_continuation_contract::lifecycle(&store).await;
    let session = &f.admitted.session.id;
    let run = &f.admitted.run.id;
    let mut state = store.state.lock().unwrap();
    assert!(state.execution_bindings.contains_key(run));
    assert!(
        state
            .tool_continuations
            .contains_key(&f.suspension.continuation.id)
    );
    assert!(
        state
            .phases
            .values()
            .any(|phase| phase.snapshot.run_id == *run)
    );
    assert!(
        state
            .native_parent_witnesses
            .values()
            .any(|witness| witness.source.run_id == *run)
    );
    state.extension_values.insert(
        (session.clone(), "private-fixture".into()),
        std::collections::BTreeMap::from([("payload".into(), "private".into())]),
    );
    let next = state.events.next_cursor().unwrap();
    super::purge::purge(
        &mut state,
        session,
        &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES),
    )
    .unwrap();
    assert!(!state.runs.contains_key(run));
    assert!(!state.execution_bindings.contains_key(run));
    assert!(!state.admission_executions.contains_key(run));
    assert!(!state.suspension_proofs.contains_key(run));
    assert!(
        state
            .phases
            .values()
            .all(|phase| phase.snapshot.run_id != *run)
    );
    assert!(
        state
            .attempts
            .values()
            .all(|attempt| attempt.snapshot.run_id != *run)
    );
    assert!(state.calls.values().all(|call| call.run_id != *run));
    assert!(
        state
            .native_parent_witnesses
            .values()
            .all(|witness| witness.source.run_id != *run)
    );
    assert!(
        state
            .native_run_issuances
            .keys()
            .all(|(scope, _, _)| scope != session)
    );
    assert!(
        state
            .tool_continuations
            .values()
            .all(|record| &record.session_id != session)
    );
    assert!(
        state
            .operation_receipts
            .keys()
            .all(|(scope, _)| scope != session)
    );
    assert!(
        state
            .extension_values
            .keys()
            .all(|(scope, _)| scope != session)
    );
    assert!(
        state
            .events
            .iter()
            .all(|event| &event.session_id != session)
    );
    assert_eq!(state.events.next_cursor().unwrap(), next);
}

#[tokio::test]
async fn physical_purge_erases_fork_payload_but_preserves_source_and_reserved_ids() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (request, receipt) = super::fork_fixture::imported(&store).await;
    let mut survivor_request = request.clone();
    survivor_request.id = crabber_core::ControlId::new();
    survivor_request.target_session_id = crabber_core::SessionId::new();
    let crate::ForkOutcome::Applied(survivor) = store.fork_session(survivor_request).await.unwrap()
    else {
        panic!("sibling safe fork")
    };
    let source_messages = store
        .list_all_messages(&request.source_session_id)
        .await
        .unwrap();
    let target = &receipt.target.session_id;
    let mut state = store.state.lock().unwrap();
    let ids: Vec<_> = state
        .imported_calls
        .for_session(target)
        .iter()
        .map(|call| call.id.clone())
        .collect();
    let survivor_ids: Vec<_> = state
        .imported_calls
        .for_session(&survivor.target.session_id)
        .iter()
        .map(|call| call.id.clone())
        .collect();
    assert_ne!(ids, [] as [crabber_core::ToolCallId; 0]);
    assert_ne!(state.imported_provenance.for_session(target), []);
    super::purge::purge(
        &mut state,
        target,
        &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES),
    )
    .unwrap();
    assert!(!state.sessions.contains_key(target));
    assert_eq!(state.imported_calls.for_session(target), []);
    assert_eq!(state.imported_provenance.for_session(target), []);
    assert_eq!(state.messages.for_session(target), []);
    assert!(ids.iter().all(|id| !state.imported_call_ids.contains(id)));
    assert!(
        survivor_ids
            .iter()
            .all(|id| state.imported_call_ids.contains(id))
    );
    assert_eq!(
        state.messages.for_session(&request.source_session_id),
        source_messages
    );
    assert!(
        !state
            .fork_audits
            .contains_key(&(target.clone(), request.id.clone()))
    );
    assert!(
        !state
            .operation_receipts
            .contains_key(&(target.clone(), request.id))
    );
}

#[tokio::test]
async fn physical_purge_refuses_missing_imported_registry_before_mutation() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (_, receipt) = super::fork_fixture::imported(&store).await;
    let target = &receipt.target.session_id;
    let mut state = store.state.lock().unwrap();
    let id = state.imported_calls.for_session(target)[0].id.clone();
    state.imported_call_ids.remove(&id);
    let before = state.imported_calls.for_session(target).to_vec();
    assert!(matches!(
        super::purge::purge(
            &mut state,
            target,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(crate::public_session::v2::proof::Failure::Error(_))
    ));
    assert!(state.sessions.contains_key(target));
    assert_eq!(state.imported_calls.for_session(target), before);
    assert!(
        state
            .fork_audits
            .contains_key(&(target.clone(), receipt.operation_id))
    );
}

#[tokio::test]
async fn physical_purge_refuses_cross_session_imported_identity_collision() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (mut request, receipt) = super::fork_fixture::imported(&store).await;
    request.id = crabber_core::ControlId::new();
    request.target_session_id = crabber_core::SessionId::new();
    let crate::ForkOutcome::Applied(survivor) = store.fork_session(request).await.unwrap() else {
        panic!("sibling safe fork")
    };
    let target = &receipt.target.session_id;
    let mut state = store.state.lock().unwrap();
    let id = state.imported_calls.for_session(target)[0].id.clone();
    let collision = state
        .imported_calls
        .iter_mut()
        .find(|call| call.session_id == survivor.target.session_id)
        .unwrap();
    collision.id = id.clone();
    assert!(matches!(
        super::purge::purge(
            &mut state,
            target,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(crate::public_session::v2::proof::Failure::Error(_))
    ));
    assert!(state.sessions.contains_key(target));
    assert!(state.sessions.contains_key(&survivor.target.session_id));
    assert!(state.imported_call_ids.contains(&id));
}

#[tokio::test]
async fn altered_imported_identity_cannot_claim_orphan_reservation_or_native_readiness() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (_, receipt) = super::fork_fixture::imported(&store).await;
    let target = &receipt.target.session_id;
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::coverage(&state, target).unwrap()
    else {
        panic!("protected fork origin")
    };
    assert_eq!(
        super::readiness::read(
            &state,
            &coverage,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        )
        .unwrap(),
        None
    );
    let original = state.imported_calls.for_session(target)[0].id.clone();
    let orphan = crabber_core::ToolCallId::new();
    state.imported_call_ids.insert(orphan.clone());
    state
        .imported_calls
        .iter_mut()
        .find(|call| &call.session_id == target)
        .unwrap()
        .id = orphan.clone();
    let before = state.imported_calls.for_session(target).to_vec();
    let registry = state.imported_call_ids.clone();
    let next = state.events.next_cursor().unwrap();
    assert!(matches!(
        super::readiness::read(
            &state,
            &coverage,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(crate::DeletionError::UnknownStoreOutcome)
    ));
    assert!(matches!(
        super::purge::purge(
            &mut state,
            target,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(crate::public_session::v2::proof::Failure::Error(_))
    ));
    assert_eq!(state.imported_calls.for_session(target), before);
    assert_eq!(state.imported_call_ids, registry);
    assert!(state.imported_call_ids.contains(&original));
    assert!(state.sessions.contains_key(target));
    assert!(
        state
            .fork_audits
            .contains_key(&(target.clone(), receipt.operation_id))
    );
    assert_eq!(state.events.next_cursor().unwrap(), next);
}

#[tokio::test]
async fn altered_imported_provenance_cannot_certify_fork_readiness() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (_, receipt) = super::fork_fixture::imported(&store).await;
    let target = &receipt.target.session_id;
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::coverage(&state, target).unwrap()
    else {
        panic!("protected fork origin")
    };
    state
        .imported_provenance
        .iter_mut()
        .find(|origin| &origin.session_id == target)
        .unwrap()
        .source_message_id = crabber_core::MessageId::new();
    assert!(matches!(
        super::readiness::read(
            &state,
            &coverage,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(crate::DeletionError::UnknownStoreOutcome)
    ));
    assert!(matches!(
        super::purge::purge(
            &mut state,
            target,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(crate::public_session::v2::proof::Failure::Error(_))
    ));
    assert!(state.sessions.contains_key(target));
    assert_ne!(state.imported_provenance.for_session(target), []);
}
