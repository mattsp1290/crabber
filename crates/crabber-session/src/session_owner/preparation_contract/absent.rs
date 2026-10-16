//! The absent clock and independent order cannot hide foreign publications.
use super::super::{SessionOwnerKind, SessionOwnerTermination};
use super::*;
use crabber_core::SessionOwnerOperationId;
pub(crate) async fn fixture_for_order(
    store: &impl Store,
) -> (
    PreparationGrant,
    crate::BoundKeyedAdmitRequest,
    SessionOwnerOperationId,
) {
    let bound = fixture(&SessionId::new(), SessionRevision::INITIAL);
    let request = reservation(&bound);
    let mut old = request.owner.clone();
    old.id = SessionOwnerOperationId::new();
    old.binding.id = crabber_core::SessionOwnerId::new();
    old.binding.kind = SessionOwnerKind::RetainedCapacity;
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_session_owner(old, None).await.unwrap()
    else {
        panic!("fresh old resource")
    };
    let begin_id = SessionOwnerOperationId::new();
    for (id, command) in [
        (begin_id.clone(), SessionOwnerCommand::Begin),
        (
            SessionOwnerOperationId::new(),
            SessionOwnerCommand::Join {
                termination: SessionOwnerTermination::Returned,
            },
        ),
    ] {
        if matches!(command, SessionOwnerCommand::Join { .. }) {
            tokio::spawn(async {}).await.unwrap();
        }
        store
            .mutate_session_owner(
                MutateSessionOwner {
                    id,
                    session_id: bound.binding.request.session_id.clone(),
                    target: authority.target().clone(),
                    command,
                },
                Some(authority.clone()),
            )
            .await
            .unwrap();
    }
    // Historical unrelated Joined publications are a valid baseline.
    let grant = begin(store, &request).await;
    (grant, bound, begin_id)
}
pub(crate) async fn foreign_refuses(
    store: &impl Store,
    grant: PreparationGrant,
    bound: crate::BoundKeyedAdmitRequest,
) {
    store
        .reserve_session_owner(foreign_reservation(&bound), None)
        .await
        .unwrap();
    assert_foreign_refusal(store, grant, bound).await;
}
pub(crate) fn foreign_reservation(
    bound: &crate::BoundKeyedAdmitRequest,
) -> super::super::ReserveSessionOwner {
    let mut foreign = reservation(bound).owner;
    foreign.id = SessionOwnerOperationId::new();
    foreign.binding.id = crabber_core::SessionOwnerId::new();
    foreign.binding.kind = SessionOwnerKind::RetainedCapacity;
    foreign
}
pub(crate) async fn assert_foreign_refusal(
    store: &impl Store,
    grant: PreparationGrant,
    bound: crate::BoundKeyedAdmitRequest,
) {
    let session = bound.binding.request.session_id.clone();
    assert_eq!(
        store
            .complete_preparation(joined(grant.clone(), bound.clone()).await)
            .await
            .unwrap_err(),
        super::super::preparation::PreparationCommitError::Store(
            StoreError::AdmissionRevisionConflict
        )
    );
    assert_eq!(
        store
            .get_session_owner(&session, &grant.owner_authority().target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert!(
        store
            .lookup_admission(&session, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_session(&session).await.unwrap().is_none());
}
pub(super) async fn run(store: &impl Store) {
    let (grant, bound, _) = fixture_for_order(store).await;
    foreign_refuses(store, grant, bound).await;
}

/// An independent owner starts before A, then publishes its real Join after A.
pub(crate) async fn fixture_for_relabel(
    store: &impl Store,
) -> (
    PreparationGrant,
    crate::BoundKeyedAdmitRequest,
    SessionOwnerOperationId,
) {
    let bound = fixture(&SessionId::new(), SessionRevision::INITIAL);
    let request = reservation(&bound);
    let mut foreign = request.owner.clone();
    foreign.id = SessionOwnerOperationId::new();
    foreign.binding.id = crabber_core::SessionOwnerId::new();
    foreign.binding.kind = SessionOwnerKind::RetainedCapacity;
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_session_owner(foreign, None).await.unwrap()
    else {
        panic!("fresh independent resource")
    };
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: SessionOwnerOperationId::new(),
                session_id: bound.binding.request.session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let grant = begin(store, &request).await;
    tokio::spawn(async {}).await.unwrap();
    let join = SessionOwnerOperationId::new();
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: join.clone(),
                session_id: bound.binding.request.session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Returned,
                },
            },
            Some(authority),
        )
        .await
        .unwrap();
    (grant, bound, join)
}
pub(crate) async fn relabel_refuses(
    store: &impl Store,
    grant: PreparationGrant,
    bound: crate::BoundKeyedAdmitRequest,
) {
    assert_eq!(
        store
            .complete_preparation(joined(grant.clone(), bound.clone()).await)
            .await
            .unwrap_err(),
        super::super::preparation::PreparationCommitError::Store(
            StoreError::AdmissionRevisionConflict
        )
    );
    assert!(
        store
            .lookup_admission(&bound.binding.request.session_id, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_session_owner(
                &bound.binding.request.session_id,
                &grant.owner_authority().target().id
            )
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
}
