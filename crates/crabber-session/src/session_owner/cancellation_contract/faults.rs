use super::*;
pub(crate) async fn run(store: &impl Store, set: impl Fn(u8)) {
    let (owner, authority) = reserve(store).await;
    create(store, &owner).await;
    let session = &owner.binding.session_id;
    let revision = store.session_revision(session).await.unwrap();
    let request = request(&owner, Some(revision));
    set(1);
    assert_eq!(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .get_session_owner(session, &owner.target.id)
            .await
            .unwrap(),
        owner
    );
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
    assert!(
        store
            .lookup_session_owner(session, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    set(2);
    assert_eq!(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::UnknownStoreOutcome
    );
    let original = store
        .lookup_session_owner(session, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        revision.next().unwrap()
    );
    assert!(
        matches!(store.request_session_owner_cancel(request.clone()).await.unwrap(),SessionOwnerOutcome::Replayed(receipt) if receipt==original)
    );
    set(0);
    observation_faults(store, &set, &owner, authority, &request).await;
}
async fn observation_faults(
    store: &impl Store,
    set: &impl Fn(u8),
    owner: &SessionOwnerSnapshot,
    authority: SessionOwnerAuthority,
    request: &RequestSessionOwnerCancel,
) {
    let session = &owner.binding.session_id;
    let observation = observe(request);
    let before = store
        .get_session_owner(session, &owner.target.id)
        .await
        .unwrap();
    let revision = store.session_revision(session).await.unwrap();
    set(1);
    assert_eq!(
        store
            .observe_session_owner_cancel(observation.clone(), authority.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .get_session_owner(session, &owner.target.id)
            .await
            .unwrap(),
        before
    );
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
    assert!(
        store
            .lookup_session_owner(session, &observation.id)
            .await
            .unwrap()
            .is_none()
    );
    set(2);
    assert_eq!(
        store
            .observe_session_owner_cancel(observation.clone(), authority.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::UnknownStoreOutcome
    );
    let original = store
        .lookup_session_owner(session, &observation.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.observe_session_owner_cancel(observation,authority).await.unwrap(),SessionOwnerOutcome::Replayed(receipt) if receipt==original)
    );
    set(0);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        revision.next().unwrap()
    );
}
