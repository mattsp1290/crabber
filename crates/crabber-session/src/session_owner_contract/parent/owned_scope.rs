//! Acceptance is parent-fenced; independently owned joins never settle its call.
use super::*;
use crate::session_owner::owned_scope::{
    OwnedCleanupTask, OwnedMainDisposition, OwnedScopeActivationOutcome, OwnedScopeParentAuthority,
    OwnedScopeSource, OwnedTaskRegistration,
};
use std::sync::atomic::{AtomicUsize, Ordering};
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let (mut request, parent, admitted, _) = fixture(store).await;
    request.lease_ms = 60_000;
    let (reserved, authority) = applied(
        store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    let (active, _) = applied(
        store
            .mutate_session_owner(
                mutation(&reserved.owner, SessionOwnerCommand::Begin),
                Some(authority.clone()),
            )
            .await
            .unwrap(),
    );
    let effects = Arc::new(AtomicUsize::new(0));
    let invoked = effects.clone();
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&reserved, &active).unwrap(),
        move || {
            invoked.fetch_add(1, Ordering::SeqCst);
            async { 42 }
        },
    )
    .unwrap();
    let activation = task.activation().clone();
    assert!(
        store
            .activate_owned_scope(activation.clone(), authority.clone(), None)
            .await
            .is_err()
    );
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            activation.clone(),
            authority.clone(),
            Some(OwnedScopeParentAuthority::Native(parent.clone())),
        )
        .await
        .unwrap()
    else {
        panic!("fresh native-backed permit")
    };
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    task.dispatch(*permit).unwrap();
    let observed = task.join().await.unwrap();
    assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let mut cleanup = OwnedCleanupTask::from_host_task(activation, tokio::spawn(async { Ok(()) }));
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    store.join_owned_scope(proof, authority).await.unwrap();
    late_parent(store, clock, request, parent, &admitted, &effects).await;
}
async fn late_parent(
    store: &impl Store,
    clock: &ManualClock,
    mut request: ReserveSessionOwner,
    parent: SessionOwnerParentAuthority,
    admitted: &crate::AdmitOutcome,
    effects: &Arc<AtomicUsize>,
) {
    // The next owner is still live when its genuine native parent expires.
    request.id = SessionOwnerOperationId::new();
    request.binding.id = SessionOwnerId::new();
    let (reserved, authority) = applied(
        store
            .reserve_session_owner(request, Some(parent.clone()))
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    let (active, _) = applied(
        store
            .mutate_session_owner(
                mutation(&reserved.owner, SessionOwnerCommand::Begin),
                Some(authority.clone()),
            )
            .await
            .unwrap(),
    );
    let invoked = effects.clone();
    let mut late = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&reserved, &active).unwrap(),
        move || {
            invoked.fetch_add(1, Ordering::SeqCst);
            async { 99 }
        },
    )
    .unwrap();
    clock.set(admitted.run.lease_until + time::Duration::seconds(1));
    assert!(
        store
            .activate_owned_scope(
                late.activation().clone(),
                authority,
                Some(OwnedScopeParentAuthority::Native(parent))
            )
            .await
            .is_err()
    );
    assert_eq!(
        late.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()[0]
            .status,
        ToolCallStatus::Running
    );
    clock.set(OffsetDateTime::UNIX_EPOCH);
}
