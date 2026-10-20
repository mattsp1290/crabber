use super::*;
async fn create_race(store: &impl Store) {
    let (owner, _) = reserve(store).await;
    let cancel = request(&owner, None);
    let create_request = ManageSession {
        id: ControlId::new(),
        session_id: owner.binding.session_id.clone(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: owner.binding.workspace_id.clone(),
            directory: owner.binding.directory.clone(),
            title: "racing creation".into(),
            defaults: SessionDefaults::default(),
        },
    };
    let (requested, created) = futures::join!(
        store.request_session_owner_cancel(cancel.clone()),
        store.manage_session(create_request)
    );
    created.unwrap();
    match requested {
        Ok(outcome) => {
            applied(outcome);
            assert!(
                store
                    .lookup_session_owner(&cancel.session_id, &cancel.id)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        Err(SessionOwnerError::RevisionConflict) => assert!(
            store
                .lookup_session_owner(&cancel.session_id, &cancel.id)
                .await
                .unwrap()
                .is_none()
        ),
        other => panic!("serialized presence CAS: {other:?}"),
    }
}
pub(super) async fn run(store: &impl Store) {
    create_race(store).await;
    let (owner, authority) = reserve(store).await;
    create(store, &owner).await;
    let revision = store
        .session_revision(&owner.binding.session_id)
        .await
        .unwrap();
    let first = request(&owner, Some(revision));
    let second = request(&owner, Some(revision));
    let (a, b) = futures::join!(
        store.request_session_owner_cancel(first),
        store.request_session_owner_cancel(second)
    );
    assert!(matches!(
        (a, b),
        (
            Ok(SessionOwnerOutcome::Applied {
                authority: None,
                ..
            }),
            Err(SessionOwnerError::RevisionConflict)
        ) | (
            Err(SessionOwnerError::RevisionConflict),
            Ok(SessionOwnerOutcome::Applied {
                authority: None,
                ..
            })
        )
    ));
    let (owner, authority2) = reserve(store).await;
    create(store, &owner).await;
    let cancel = request(
        &owner,
        Some(
            store
                .session_revision(&owner.binding.session_id)
                .await
                .unwrap(),
        ),
    );
    let join = mutation(
        &owner,
        SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    );
    let (requested, joined) = futures::join!(
        store.request_session_owner_cancel(cancel.clone()),
        store.mutate_session_owner(join, Some(authority2))
    );
    assert!(
        matches!(joined.unwrap(),SessionOwnerOutcome::Applied{receipt,..} if receipt.owner.state==SessionOwnerState::Joined)
    );
    assert!(matches!(
        requested,
        Ok(SessionOwnerOutcome::Applied {
            authority: None,
            ..
        }) | Err(SessionOwnerError::InvalidTransition)
    ));
    // Keeping another exact authority alive must not imply this owner joined.
    assert!(
        store
            .pending_session_owner_cancel(&authority)
            .await
            .unwrap()
            .is_some()
    );
}
