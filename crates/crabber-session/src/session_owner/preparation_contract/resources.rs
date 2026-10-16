//! Linked resource capacity survives native preparation handoff and replay.
use super::*;
#[path = "resources/guards.rs"]
pub(crate) mod guards;
use crate::session_owner::preparation::{
    OWNED_PREPARATION_COMPONENT, PreparationResourceLink, PreparationResourceOutcome,
};

async fn setup(
    store: &impl Store,
    existing: bool,
) -> (PreparationGrant, crate::BoundKeyedAdmitRequest) {
    let session = SessionId::new();
    let initial = fixture(&session, SessionRevision::INITIAL);
    let revision = if existing {
        create(store, &initial).await
    } else {
        SessionRevision::INITIAL
    };
    let bound = fixture(&session, revision);
    let mut request = reservation(&bound);
    request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    (begin(store, &request).await, bound)
}
pub(crate) async fn run(store: &impl Store) {
    assert!(store.supports_preparation_resources());
    for existing in [false, true] {
        successful(store, existing).await;
    }
    labels_are_not_proof(store).await;
    unrelated_and_cancel_refuse(store).await;
    cleanup_is_not_handoff(store).await;
}
async fn successful(store: &impl Store, existing: bool) {
    let (original, bound) = setup(store, existing).await;
    let session = bound.binding.request.session_id.clone();
    assert!(
        store
            .complete_preparation(joined(original.clone(), bound.clone()).await)
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_admission(&session, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
    let PreparationResourceOutcome::Applied {
        receipt: resources,
        grant,
    } = store
        .retain_preparation_resources(original.clone())
        .await
        .unwrap()
    else {
        panic!("fresh child authority")
    };
    let grant = *grant;
    assert_eq!(
        resources.link,
        PreparationResourceLink::for_grant(&original).unwrap()
    );
    let child = grant.retained_resources_authority().unwrap().clone();
    assert_eq!(
        store
            .get_session_owner(&session, &child.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { receipt, .. }) = store
        .complete_preparation(joined(grant.clone(), bound.clone()).await)
        .await
        .unwrap()
    else {
        panic!("one native successor")
    };
    let mut expected = bound.keyed.request.expected_revision.unwrap();
    for _ in 0..if existing { 6 } else { 1 } {
        expected = expected.next().unwrap();
    }
    assert_eq!(store.session_revision(&session).await.unwrap(), expected);
    assert_eq!(
        store
            .execution_binding(&session, &receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .capsule,
        bound.binding
    );

    assert_eq!(
        store
            .get_session_owner(&session, &original.owner_authority().target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    assert_eq!(
        store
            .get_session_owner(&session, &child.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert!(
        matches!(store.retain_preparation_resources(original).await.unwrap(), PreparationResourceOutcome::Replayed(saved) if saved == resources)
    );
    assert!(
        store
            .complete_preparation(joined(grant, bound).await)
            .await
            .unwrap()
            .is_replayed()
    );
    cleanup_child(
        store,
        &session,
        &child,
        resources.link.identities().unwrap().join,
    )
    .await;
}
async fn labels_are_not_proof(store: &impl Store) {
    let (original, bound) = setup(store, false).await;
    let request = PreparationResourceLink::reservation(&original).unwrap();
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_session_owner(request, None).await.unwrap()
    else {
        panic!("ordinary owner")
    };
    let forged = original.clone().with_retained_resources(authority).unwrap();
    assert!(
        store
            .complete_preparation(joined(forged, bound.clone()).await)
            .await
            .is_err()
    );
    assert!(matches!(
        store.retain_preparation_resources(original).await,
        Err(SessionOwnerError::SemanticConflict)
    ));
    assert!(
        store
            .lookup_admission(&bound.binding.request.session_id, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
}
async fn unrelated_and_cancel_refuse(store: &impl Store) {
    for cancel in [false, true] {
        let (original, bound) = setup(store, false).await;
        let PreparationResourceOutcome::Applied { grant, .. } =
            store.retain_preparation_resources(original).await.unwrap()
        else {
            panic!("fresh child")
        };
        let session = bound.binding.request.session_id.clone();
        if cancel {
            store
                .request_session_owner_cancel(crate::session_owner::RequestSessionOwnerCancel {
                    id: crabber_core::SessionOwnerOperationId::new(),
                    session_id: session.clone(),
                    target: grant
                        .retained_resources_authority()
                        .unwrap()
                        .target()
                        .clone(),
                    expected_revision: None,
                    claim: crate::session_owner::SessionOwnerResolver {
                        resolver_id: "host".into(),
                        authority_claim: "test resource cancellation".into(),
                    },
                })
                .await
                .unwrap();
        } else {
            let mut foreign = grant.original_reservation().owner.clone();
            foreign.id = crabber_core::SessionOwnerOperationId::new();
            foreign.binding.id = crabber_core::SessionOwnerId::new();
            foreign.binding.kind = crate::session_owner::SessionOwnerKind::RetainedCapacity;
            foreign.binding.component_id = "unrelated-resource".into();
            store.reserve_session_owner(foreign, None).await.unwrap();
        }
        assert!(
            store
                .complete_preparation(joined(*grant, bound.clone()).await)
                .await
                .is_err()
        );
        assert!(
            store
                .lookup_admission(&session, &bound.keyed.options.key)
                .await
                .unwrap()
                .is_none()
        );
    }
}

/// Both resource publications commit together; unknown acknowledgement never
/// returns replacement authority, while the genuine retained grant can retry
/// the exact joined native result without redoing factory work.
pub(crate) async fn faults(
    store: &impl Store,
    owner_fault: impl Fn(u8),
    native_fault: impl Fn(u8),
) {
    for point in [1, 2] {
        let (original, bound) = setup(store, true).await;
        let session = bound.binding.request.session_id.clone();
        let ids = PreparationResourceLink::for_grant(&original)
            .unwrap()
            .identities()
            .unwrap();
        let before = store.session_revision(&session).await.unwrap();
        owner_fault(point);
        assert!(matches!(
            store.retain_preparation_resources(original.clone()).await,
            Err(SessionOwnerError::UnknownStoreOutcome)
        ));
        owner_fault(0);
        let reserved = store
            .lookup_session_owner(&session, &ids.reserve)
            .await
            .unwrap();
        let began = store
            .lookup_session_owner(&session, &ids.begin)
            .await
            .unwrap();
        assert_eq!(reserved.is_some(), point == 2);
        assert_eq!(began.is_some(), point == 2);
        if point == 2 {
            assert!(matches!(
                store.retain_preparation_resources(original).await.unwrap(),
                PreparationResourceOutcome::Replayed(_)
            ));
            assert_eq!(
                store.session_revision(&session).await.unwrap(),
                before.next().unwrap().next().unwrap()
            );
        } else {
            assert_eq!(store.session_revision(&session).await.unwrap(), before);
            assert!(matches!(
                store.retain_preparation_resources(original).await.unwrap(),
                PreparationResourceOutcome::Applied { .. }
            ));
        }
        assert!(
            store
                .lookup_admission(&session, &bound.keyed.options.key)
                .await
                .unwrap()
                .is_none()
        );
    }
    for point in [1, 2] {
        let (original, bound) = setup(store, true).await;
        let PreparationResourceOutcome::Applied { grant, .. } =
            store.retain_preparation_resources(original).await.unwrap()
        else {
            panic!("fresh child")
        };
        let session = bound.binding.request.session_id.clone();
        let child = grant.retained_resources_authority().unwrap().clone();
        let observed = joined(*grant, bound.clone()).await;
        native_fault(point);
        assert!(store.complete_preparation(observed.clone()).await.is_err());
        native_fault(0);
        assert_eq!(
            store
                .lookup_admission(&session, &bound.keyed.options.key)
                .await
                .unwrap()
                .is_some(),
            point == 2
        );
        let retried = store.complete_preparation(observed).await.unwrap();
        assert_eq!(retried.is_replayed(), point == 2);
        assert_eq!(
            store
                .get_session_owner(&session, &child.target().id)
                .await
                .unwrap()
                .state,
            SessionOwnerState::Active
        );
    }
}

async fn cleanup_child(
    store: &impl Store,
    session: &SessionId,
    child: &crate::session_owner::SessionOwnerAuthority,
    join: crabber_core::SessionOwnerOperationId,
) {
    // A host joins the actual resource task before asserting child cleanup.
    tokio::spawn(async {}).await.unwrap();
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: join,
                session_id: session.clone(),
                target: child.target().clone(),
                command: SessionOwnerCommand::Join {
                    termination: crate::session_owner::SessionOwnerTermination::Returned,
                },
            },
            Some(child.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_session_owner(session, &child.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
}

async fn cleanup_is_not_handoff(store: &impl Store) {
    let (original, bound) = setup(store, false).await;
    // No factory was dispatched. The host still authenticates its actual local
    // cleanup assertion, rather than deriving it from absent child metadata.
    tokio::spawn(async {}).await.unwrap();
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: original.reconciliation().unwrap().join_id,
                session_id: bound.binding.request.session_id.clone(),
                target: original.owner_authority().target().clone(),
                command: SessionOwnerCommand::Join {
                    termination: crate::session_owner::SessionOwnerTermination::Returned,
                },
            },
            Some(original.owner_authority().clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_session_owner(
                &bound.binding.request.session_id,
                &original.owner_authority().target().id
            )
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    assert!(
        store
            .complete_preparation(joined(original, bound).await)
            .await
            .is_err()
    );
}
