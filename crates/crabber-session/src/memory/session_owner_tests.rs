use crate::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerError, SessionOwnerOutcome,
        SessionOwnerState,
    },
};
use crabber_core::{ManualClock, SessionId, SessionOwnerOperationId};
use std::sync::{Arc, atomic::Ordering};
use time::OffsetDateTime;
#[tokio::test]
async fn session_owner_contract() {
    crate::session_owner_contract::run_contract(|clock| super::MemoryStore::with_clock(clock))
        .await;
}
#[tokio::test]
async fn session_owner_ack_loss_never_reissues_dispatch() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = super::MemoryStore::with_clock(clock.clone());
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let session = &request.binding.session_id;
    store.session_owner_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.reserve_session_owner(request.clone(), None).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .lookup_session_owner(session, &request.id)
            .await
            .unwrap(),
        None
    );
    store.session_owner_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store.reserve_session_owner(request.clone(), None).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    let original = store
        .lookup_session_owner(session, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.reserve_session_owner(request.clone(), None).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved == original)
    );
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: original.owner.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    assert!(matches!(
        store.mutate_session_owner(begin, None).await,
        Err(SessionOwnerError::StaleOwner)
    ));
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
    store.session_owner_fault.store(0, Ordering::SeqCst);
    let reclaim = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: original.owner.target,
        command: SessionOwnerCommand::ReclaimUnstarted {
            owner: "recovered".into(),
            lease_ms: 100,
        },
    };
    let SessionOwnerOutcome::Applied {
        receipt: reclaimed,
        authority,
    } = store.mutate_session_owner(reclaim, None).await.unwrap()
    else {
        panic!("fresh reclaim")
    };
    let authority = authority.unwrap();
    begin_ack_loss(&store, &request, reclaimed, authority).await;
}
async fn begin_ack_loss(
    store: &super::MemoryStore,
    request: &crate::session_owner::ReserveSessionOwner,
    reclaimed: crate::session_owner::SessionOwnerReceipt,
    authority: crate::session_owner::SessionOwnerAuthority,
) {
    let session = &request.binding.session_id;
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: reclaimed.owner.target,
        command: SessionOwnerCommand::Begin,
    };
    store.session_owner_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store
            .mutate_session_owner(begin.clone(), Some(authority.clone()))
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .get_session_owner(session, &request.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::ReservedUnstarted
    );
    store.session_owner_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store
            .mutate_session_owner(begin.clone(), Some(authority))
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .get_session_owner(session, &request.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert!(matches!(
        store.mutate_session_owner(begin, None).await.unwrap(),
        SessionOwnerOutcome::Replayed(_)
    ));
}

#[tokio::test]
async fn session_owner_all_commit_faults() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::session_owner_contract::faults::run(&store, &clock, |value| {
        store.session_owner_fault.store(value, Ordering::SeqCst);
    })
    .await;
}

#[tokio::test]
async fn session_owner_shared_parent_contract() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::session_owner_contract::parent::run(&store, &clock).await;
}

#[tokio::test]
async fn session_owner_shared_capacity() {
    let store = super::MemoryStore::new();
    let (request, authority) = crate::session_owner_contract::capacity::fill(&store).await;
    crate::session_owner_contract::capacity::joined_slot(&store, request, authority).await;
}

#[tokio::test]
async fn preparation_protocol_contract() {
    Box::pin(crate::session_owner::preparation_contract::run(
        &super::MemoryStore::new(),
    ))
    .await;
}

#[tokio::test]
async fn preparation_protocol_commit_faults() {
    let store = super::MemoryStore::new();
    crate::session_owner::preparation_contract::faults::run(
        &store,
        |point| store.session_owner_fault.store(point, Ordering::SeqCst),
        |point| store.binding_fault.store(point, Ordering::SeqCst),
    )
    .await;
}

#[tokio::test]
async fn preparation_absent_order_rejects_deleted_history_plus_foreign_publication() {
    let store = super::MemoryStore::new();
    let (grant, bound, old) =
        crate::session_owner::preparation_contract::absent::fixture_for_order(&store).await;
    store
        .reserve_session_owner(
            crate::session_owner::preparation_contract::absent::foreign_reservation(&bound),
            None,
        )
        .await
        .unwrap();
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .erase_historical_audit_for_test(&bound.binding.request.session_id, &old);
    assert_eq!(
        store
            .reserve_session_owner(
                crate::session_owner::preparation_contract::absent::foreign_reservation(&bound),
                None
            )
            .await
            .unwrap_err(),
        SessionOwnerError::UnknownStoreOutcome
    );
    crate::session_owner::preparation_contract::absent::assert_foreign_refusal(
        &store, grant, bound,
    )
    .await;
}

#[tokio::test]
async fn preparation_absent_order_rejects_foreign_join_relabeled_renew() {
    let store = super::MemoryStore::new();
    let (grant, bound, join) =
        crate::session_owner::preparation_contract::absent::fixture_for_relabel(&store).await;
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .relabel_renew_for_test(&bound.binding.request.session_id, &join);
    crate::session_owner::preparation_contract::absent::relabel_refuses(&store, grant, bound).await;
}

#[tokio::test]
async fn owner_cancellation_contract() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::session_owner::cancellation_contract::run(&store, &clock).await;
    crate::session_owner::cancellation_contract::faults::run(&store, |value| {
        store.session_owner_fault.store(value, Ordering::SeqCst);
    })
    .await;
}

#[tokio::test]
async fn preparation_resource_contract() {
    use crate::session_owner::preparation_contract::resources::guards;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::MemoryStore::with_clock(clock.clone());
    crate::session_owner::preparation_contract::resources::run(&store).await;
    crate::session_owner::preparation_contract::resources::faults(
        &store,
        |point| {
            store
                .session_owner_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
        |point| {
            store
                .binding_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
    guards::run(&store, &clock).await;
    let (grant, bound, original_begin) = guards::source_fixture(&store).await;
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .erase_historical_audit_for_test(&bound.binding.request.session_id, &original_begin);
    guards::source_refuses(&store, grant, bound).await;
}

#[tokio::test]
async fn preparation_handoff_marker_is_bound_to_original_join_digest() {
    use crate::session_owner::preparation_contract::resources::guards;
    let store = super::MemoryStore::new();
    for native in [false, true] {
        let (grant, bound) = guards::handoff_fixture(&store, native).await;
        let target = if native {
            None
        } else {
            Some(
                grant
                    .retained_resources_authority()
                    .unwrap()
                    .target()
                    .clone(),
            )
        };
        store
            .state
            .lock()
            .unwrap()
            .session_owners
            .replace_handoff_for_test(&grant.reconciliation().unwrap(), target);
        guards::handoff_refuses(&store, grant, bound).await;
    }
}

#[tokio::test]
async fn owned_scope_sources_and_join_hash_are_independent() {
    use crate::session_owner::preparation_contract::resources::scope_integrity as check;
    let store = super::MemoryStore::new();
    for original_begin in [false, true] {
        let fixture = check::fixture(&store).await;
        let key = if original_begin {
            &fixture.activation.source.begin
        } else {
            fixture.activation.operation_id()
        };
        store
            .state
            .lock()
            .unwrap()
            .session_owners
            .erase_historical_audit_for_test(fixture.child.session_id(), key);
        check::source_refuses(&store, fixture, original_begin).await;
    }
    for owned in [false, true] {
        let fixture = check::fixture(&store).await;
        check::join_source(&store, &fixture, owned).await;
        let marker = if owned {
            None
        } else {
            Some(fixture.proof.inspection().clone())
        };
        store
            .state
            .lock()
            .unwrap()
            .session_owners
            .replace_scope_join_for_test(&fixture.child, marker);
        check::marker_refuses(&store, fixture).await;
    }
}

#[tokio::test]
async fn erased_activation_cannot_be_replaced_with_a_never_opened_gate() {
    use crate::session_owner::{
        owned_scope::{OwnedCleanupTask, OwnedTaskRegistration},
        preparation_contract::resources::scope_integrity,
    };
    let store = super::MemoryStore::new();
    let original = scope_integrity::fixture(&store).await;
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .rollback_activation_for_test(&original.activation);
    let mut replacement =
        OwnedTaskRegistration::register(original.activation.source.clone(), || async {
            panic!("replacement must never dispatch")
        })
        .unwrap();
    let activation = replacement.activation().clone();
    let observed = replacement.join().await.unwrap();
    let mut cleanup = OwnedCleanupTask::from_host_task(activation, tokio::spawn(async { Ok(()) }));
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    let join = proof.request().unwrap();
    assert!(
        Store::join_owned_scope(&store, proof, original.child.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .get_session_owner(original.child.session_id(), &original.child.target().id)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .lookup_session_owner(&join.session_id, &join.id)
            .await
            .unwrap(),
        None
    );
}
