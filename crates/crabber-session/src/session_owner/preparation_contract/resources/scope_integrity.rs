//! Original activation and owned Join hashes cannot be relabelled as generic cleanup.
use super::*;
use crate::session_owner::{
    SessionOwnerAuthority,
    owned_scope::{
        JoinedOwnedScope, OwnedCleanupTask, OwnedScopeActivation, OwnedScopeActivationOutcome,
        OwnedScopeSource, OwnedTaskRegistration,
    },
};
pub(crate) struct Fixture {
    pub grant: PreparationGrant,
    pub bound: crate::BoundKeyedAdmitRequest,
    pub activation: OwnedScopeActivation,
    pub proof: JoinedOwnedScope,
    pub child: SessionOwnerAuthority,
}
pub(crate) async fn fixture(store: &impl Store) -> Fixture {
    let (original, bound) = setup(store, true).await;
    let PreparationResourceOutcome::Applied { receipt, grant } =
        store.retain_preparation_resources(original).await.unwrap()
    else {
        panic!("fresh child")
    };
    let child = grant.retained_resources_authority().unwrap().clone();
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        || async { 42 },
    )
    .unwrap();
    let activation = task.activation().clone();
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            activation.clone(),
            child.clone(),
            Some(
                crate::session_owner::owned_scope::OwnedScopeParentAuthority::preparation(
                    (*grant).clone(),
                ),
            ),
        )
        .await
        .unwrap()
    else {
        panic!("fresh activation")
    };
    task.dispatch(*permit).unwrap();
    let observed = task.join().await.unwrap();
    let mut cleanup =
        OwnedCleanupTask::from_host_task(activation.clone(), tokio::spawn(async { Ok(()) }));
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    Fixture {
        grant: *grant,
        bound,
        activation,
        proof,
        child,
    }
}
pub(crate) async fn source_refuses(store: &impl Store, fixture: Fixture, activation_exists: bool) {
    assert!(
        store
            .get_session_owner(fixture.child.session_id(), &fixture.child.target().id)
            .await
            .is_err()
    );
    assert!(
        store
            .complete_preparation(joined(fixture.grant, fixture.bound).await)
            .await
            .is_err()
    );
    assert!(
        store
            .join_owned_scope(fixture.proof, fixture.child.clone())
            .await
            .is_err()
    );
    let replay = store
        .activate_owned_scope(fixture.activation, fixture.child, None)
        .await;
    assert_eq!(
        matches!(replay, Ok(OwnedScopeActivationOutcome::Replayed(_))),
        activation_exists
    );
}
pub(crate) async fn join_source(store: &impl Store, fixture: &Fixture, owned: bool) {
    if owned {
        store
            .join_owned_scope(fixture.proof.clone(), fixture.child.clone())
            .await
            .unwrap();
    } else {
        store
            .mutate_session_owner(
                fixture.proof.request().unwrap(),
                Some(fixture.child.clone()),
            )
            .await
            .unwrap();
    }
}
pub(crate) async fn marker_refuses(store: &impl Store, fixture: Fixture) {
    assert!(
        store
            .get_session_owner(fixture.child.session_id(), &fixture.child.target().id)
            .await
            .is_err()
    );
    assert!(
        store
            .join_owned_scope(fixture.proof, fixture.child.clone())
            .await
            .is_err()
    );
    assert!(matches!(
        store
            .activate_owned_scope(fixture.activation, fixture.child, None)
            .await
            .unwrap(),
        OwnedScopeActivationOutcome::Replayed(_)
    ));
}
