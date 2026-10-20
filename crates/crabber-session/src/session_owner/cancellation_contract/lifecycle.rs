use super::*;
pub(super) async fn absent(store: &impl Store, clock: &ManualClock) {
    let (owner, authority) = reserve(store).await;
    let request = request(&owner, None);
    wrong_presence(store, &owner, Some(SessionRevision::INITIAL)).await;
    let accepted = applied(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(accepted.owner.state, SessionOwnerState::ReservedUnstarted);
    assert_eq!(accepted.owner.target, owner.target);
    create(store, &owner).await;
    super::aggregate::check(store, &owner).await;
    store
        .mutate_session_owner(
            mutation(&owner, SessionOwnerCommand::Begin),
            Some(authority.clone()),
        )
        .await
        .unwrap();
    clock.set(clock.now() + time::Duration::seconds(1));
    let blocked = mutation(
        &owner,
        SessionOwnerCommand::ReclaimUnstarted {
            owner: "replacement".into(),
            lease_ms: 100,
        },
    );
    assert_eq!(
        store.mutate_session_owner(blocked, None).await.unwrap_err(),
        SessionOwnerError::CancellationPending
    );
    let current = store
        .pending_session_owner_cancel(&authority)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.request_id, request.id);
    assert!(current.observed.is_none());
    let observation = observe(&request);
    let observed = applied(
        store
            .observe_session_owner_cancel(observation.clone(), authority.clone())
            .await
            .unwrap(),
    );
    assert_eq!(observed.owner.state, SessionOwnerState::Active);
    super::aggregate::check(store, &owner).await;
    expired_still_cannot_renew(store, &owner, &authority).await;
    assert!(
        observed
            .owner
            .cancellation
            .as_ref()
            .unwrap()
            .observed
            .is_some()
    );
    let joined = store
        .mutate_session_owner(
            mutation(
                &owner,
                SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Cancelled,
                },
            ),
            Some(authority),
        )
        .await
        .unwrap();
    assert!(
        matches!(joined, SessionOwnerOutcome::Applied { receipt, authority:None } if receipt.owner.state == SessionOwnerState::Joined)
    );
    super::aggregate::check(store, &owner).await;
    assert!(
        matches!(store.request_session_owner_cancel(request.clone()).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved==accepted)
    );
    assert!(
        matches!(store.observe_session_owner_cancel(observation, SessionOwnerAuthority::from_store(request.session_id.clone(), request.target.clone(), "untrusted-stale-token".into()).unwrap()).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved==observed)
    );
    let mut changed = request;
    changed.claim.authority_claim.push('!');
    assert_eq!(
        store
            .request_session_owner_cancel(changed)
            .await
            .unwrap_err(),
        SessionOwnerError::SemanticConflict
    );
}
pub(super) async fn existing(store: &impl Store, clock: &ManualClock) {
    let (owner, authority) = reserve(store).await;
    create(store, &owner).await;
    wrong_presence(store, &owner, None).await;
    store
        .mutate_session_owner(
            mutation(&owner, SessionOwnerCommand::Begin),
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let session = &owner.binding.session_id;
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: store.session_revision(session).await.unwrap(),
            change: SessionChange::SetArchived { archived: true },
        })
        .await
        .unwrap();
    let revision = store.session_revision(session).await.unwrap();
    let request = request(&owner, Some(revision));
    let accepted = applied(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        revision.next().unwrap()
    );
    assert_eq!(accepted.owner.state, SessionOwnerState::Active);
    let mut repeated = request.clone();
    repeated.id = SessionOwnerOperationId::new();
    repeated.expected_revision = Some(store.session_revision(session).await.unwrap());
    assert_eq!(
        store
            .request_session_owner_cancel(repeated)
            .await
            .unwrap_err(),
        SessionOwnerError::CancellationPending
    );
    let mut wrong = observe(&request);
    wrong.request_id = SessionOwnerOperationId::new();
    assert_eq!(
        store
            .observe_session_owner_cancel(wrong, authority.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::SemanticConflict
    );
    let observation = observe(&request);
    let wrong = SessionOwnerAuthority::from_store(
        session.clone(),
        owner.target.clone(),
        "wrong-token".into(),
    )
    .unwrap();
    assert_eq!(
        store
            .observe_session_owner_cancel(observation.clone(), wrong)
            .await
            .unwrap_err(),
        SessionOwnerError::StaleOwner
    );
    clock.set(clock.now() + time::Duration::seconds(1));
    let before = store.session_revision(session).await.unwrap();
    let observed = applied(
        store
            .observe_session_owner_cancel(observation.clone(), authority)
            .await
            .unwrap(),
    );
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        before.next().unwrap()
    );
    let mut fresh = observation;
    fresh.id = SessionOwnerOperationId::new();
    let fake = SessionOwnerAuthority::from_store(
        session.clone(),
        owner.target.clone(),
        "wrong-token".into(),
    )
    .unwrap();
    assert_eq!(
        store
            .observe_session_owner_cancel(fresh, fake)
            .await
            .unwrap_err(),
        SessionOwnerError::StaleOwner
    );
    assert!(format!("{request:?}").contains("redacted"));
    assert!(!format!("{observed:?}").contains("authenticated cancellation assertion"));
}
pub(super) async fn stale(store: &impl Store, clock: &ManualClock) {
    let (owner, authority) = reserve(store).await;
    clock.set(clock.now() + time::Duration::seconds(1));
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: Some(replacement),
    } = store
        .mutate_session_owner(
            mutation(
                &owner,
                SessionOwnerCommand::ReclaimUnstarted {
                    owner: "new".into(),
                    lease_ms: 100,
                },
            ),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reclaim")
    };
    assert_eq!(
        store
            .pending_session_owner_cancel(&authority)
            .await
            .unwrap_err(),
        SessionOwnerError::StaleOwner
    );
    let request = request(&receipt.owner, None);
    applied(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(
        store
            .observe_session_owner_cancel(observe(&request), authority)
            .await
            .unwrap_err(),
        SessionOwnerError::StaleOwner
    );
    applied(
        store
            .observe_session_owner_cancel(observe(&request), replacement)
            .await
            .unwrap(),
    );
}

async fn expired_still_cannot_renew(
    store: &impl Store,
    owner: &SessionOwnerSnapshot,
    authority: &SessionOwnerAuthority,
) {
    assert_eq!(
        store
            .mutate_session_owner(
                mutation(owner, SessionOwnerCommand::Renew { lease_ms: 30_000 }),
                Some(authority.clone())
            )
            .await
            .unwrap_err(),
        SessionOwnerError::StaleOwner
    );
}

async fn wrong_presence(
    store: &impl Store,
    owner: &SessionOwnerSnapshot,
    revision: Option<SessionRevision>,
) {
    assert_eq!(
        store
            .request_session_owner_cancel(request(owner, revision))
            .await
            .unwrap_err(),
        SessionOwnerError::RevisionConflict
    );
}
