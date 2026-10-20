//! Neither uncertain acknowledgement supplies dispatch or cleanup authority.
use super::*;
use crate::session_owner::owned_scope::{
    OwnedCleanupTask, OwnedMainDisposition, OwnedScopeSource, OwnedTaskRegistration,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub(super) async fn run(store: &impl Store, fault: &impl Fn(u8)) {
    for point in [1, 2] {
        activation(store, fault, point).await;
        cleanup(store, fault, point).await;
    }
}
async fn child(
    store: &impl Store,
) -> (
    crate::session_owner::SessionOwnerAuthority,
    OwnedScopeSource,
    PreparationGrant,
) {
    let (original, _) = setup(store, true).await;
    let PreparationResourceOutcome::Applied { receipt, grant } =
        store.retain_preparation_resources(original).await.unwrap()
    else {
        panic!("fresh child")
    };
    (
        grant.retained_resources_authority().unwrap().clone(),
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        *grant,
    )
}
async fn activation(store: &impl Store, fault: &impl Fn(u8), point: u8) {
    let (authority, source, grant) = child(store).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let called = effects.clone();
    let mut task = OwnedTaskRegistration::register(source, move || {
        called.fetch_add(1, Ordering::SeqCst);
        async { 42 }
    })
    .unwrap();
    let request = task.activation().clone();
    fault(point);
    assert!(matches!(
        store
            .activate_owned_scope(
                request.clone(),
                authority.clone(),
                Some(
                    crate::session_owner::owned_scope::OwnedScopeParentAuthority::preparation(
                        grant.clone()
                    )
                )
            )
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    fault(0);
    assert_eq!(
        store
            .lookup_session_owner(request.session_id(), request.operation_id())
            .await
            .unwrap()
            .is_some(),
        point == 2
    );
    // No permit was acknowledged. Both rollback and committed-unknown paths
    // close and physically join the exact waiting gate without invoking work.
    let observed = task.join().await.unwrap();
    assert_eq!(observed.disposition(), OwnedMainDisposition::NoDispatch);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let mut cleanup =
        OwnedCleanupTask::from_host_task(request.clone(), tokio::spawn(async { Ok(()) }));
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    assert!(matches!(
        store
            .join_owned_scope(proof.clone(), authority.clone())
            .await
            .unwrap(),
        SessionOwnerOutcome::Applied {
            authority: None,
            ..
        }
    ));
    assert_eq!(
        store
            .get_session_owner(request.session_id(), &authority.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    assert!(matches!(
        store
            .join_owned_scope(proof, authority.clone())
            .await
            .unwrap(),
        SessionOwnerOutcome::Replayed(_)
    ));
    if point == 2 {
        assert!(matches!(
            store
                .activate_owned_scope(request, authority, None)
                .await
                .unwrap(),
            crate::session_owner::owned_scope::OwnedScopeActivationOutcome::Replayed(_)
        ));
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}
async fn cleanup(store: &impl Store, fault: &impl Fn(u8), point: u8) {
    let (authority, source, grant) = child(store).await;
    let mut task = OwnedTaskRegistration::register(source, || async { 42 }).unwrap();
    let request = task.activation().clone();
    let crate::session_owner::owned_scope::OwnedScopeActivationOutcome::Applied { permit, .. } =
        store
            .activate_owned_scope(
                request.clone(),
                authority.clone(),
                Some(
                    crate::session_owner::owned_scope::OwnedScopeParentAuthority::preparation(
                        grant.clone(),
                    ),
                ),
            )
            .await
            .unwrap()
    else {
        panic!("fresh permit")
    };
    task.dispatch(*permit).unwrap();
    let observed = task.join().await.unwrap();
    let mut cleanup =
        OwnedCleanupTask::from_host_task(request.clone(), tokio::spawn(async { Ok(()) }));
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    fault(point);
    assert!(matches!(
        store
            .join_owned_scope(proof.clone(), authority.clone())
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    fault(0);
    assert_eq!(
        store
            .get_session_owner(request.session_id(), &authority.target().id)
            .await
            .unwrap()
            .state,
        if point == 1 {
            SessionOwnerState::Active
        } else {
            SessionOwnerState::Joined
        }
    );
    let result = store.join_owned_scope(proof, authority).await.unwrap();
    assert_eq!(
        matches!(result, SessionOwnerOutcome::Replayed(_)),
        point == 2
    );
    assert_eq!(observed.value(), Some(&42));
}
