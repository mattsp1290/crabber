//! Rollback and acknowledgement loss never recreate dispatch authority.
use super::*;
fn change(owner: &SessionOwnerSnapshot, command: SessionOwnerCommand) -> MutateSessionOwner {
    mutation(owner, command)
}
pub(crate) async fn run(store: &dyn Store, clock: &ManualClock, set: impl Fn(u8)) {
    let request = reservation(SessionId::new());
    let session = &request.binding.session_id;
    set(1);
    assert!(matches!(
        store.reserve_session_owner(request.clone(), None).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert!(
        store
            .lookup_session_owner(session, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    set(2);
    assert!(matches!(
        store.reserve_session_owner(request.clone(), None).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    let receipt = store
        .lookup_session_owner(session, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.reserve_session_owner(request.clone(),None).await.unwrap(),SessionOwnerOutcome::Replayed(saved) if saved==receipt)
    );
    set(0);
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
    let reclaim = change(
        &receipt.owner,
        SessionOwnerCommand::ReclaimUnstarted {
            owner: "after-unknown".into(),
            lease_ms: 100,
        },
    );
    // Unknown reclaim acknowledgements provide no replacement token.
    fault_mutation(store, reclaim, None, &set).await;
    let request = reservation(SessionId::new());
    let (receipt, authority) = applied(store.reserve_session_owner(request, None).await.unwrap());
    let authority = authority.unwrap();
    let mut owner = receipt.owner;
    for command in [
        SessionOwnerCommand::Begin,
        SessionOwnerCommand::Renew { lease_ms: 100 },
        SessionOwnerCommand::MarkUncertain { claim: resolver() },
        SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    ] {
        let receipt = fault_mutation(
            store,
            change(&owner, command),
            Some(authority.clone()),
            &set,
        )
        .await;
        owner = receipt.owner;
    }
    let request = reservation(SessionId::new());
    let (receipt, _) = applied(store.reserve_session_owner(request, None).await.unwrap());
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2));
    fault_mutation(
        store,
        change(
            &receipt.owner,
            SessionOwnerCommand::ResolveJoined {
                termination: SessionOwnerTermination::Cancelled,
                claim: resolver(),
            },
        ),
        None,
        &set,
    )
    .await;
}
async fn fault_mutation(
    store: &dyn Store,
    request: MutateSessionOwner,
    authority: Option<SessionOwnerAuthority>,
    set: &impl Fn(u8),
) -> SessionOwnerReceipt {
    let before = store
        .get_session_owner(&request.session_id, &request.target.id)
        .await
        .unwrap();
    set(1);
    assert!(matches!(
        store
            .mutate_session_owner(request.clone(), authority.clone())
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .get_session_owner(&request.session_id, &request.target.id)
            .await
            .unwrap(),
        before
    );
    assert!(
        store
            .lookup_session_owner(&request.session_id, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    set(2);
    assert!(matches!(
        store.mutate_session_owner(request.clone(), authority).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    let saved = store
        .lookup_session_owner(&request.session_id, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.mutate_session_owner(request,None).await.unwrap(),SessionOwnerOutcome::Replayed(receipt) if receipt==saved)
    );
    set(0);
    saved
}
