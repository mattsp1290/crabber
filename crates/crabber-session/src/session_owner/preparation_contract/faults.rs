//! Every ownership/native commit boundary can lose its acknowledgement.
use super::*;
pub(crate) async fn run(store: &impl Store, owner_fault: impl Fn(u8), native_fault: impl Fn(u8)) {
    reserve_faults(store, &owner_fault).await;
    begin_faults(store, &owner_fault).await;
    native_faults(store, &native_fault).await;
    super::retry::retained(store, &native_fault).await;
}
async fn reserve_faults(store: &impl Store, owner_fault: &impl Fn(u8)) {
    for point in [1, 2] {
        let bound = fixture(&SessionId::new(), SessionRevision::INITIAL);
        let request = reservation(&bound);
        owner_fault(point);
        assert!(matches!(
            store.reserve_preparation(request.clone()).await,
            Err(SessionOwnerError::UnknownStoreOutcome)
        ));
        owner_fault(0);
        let source = store
            .lookup_preparation(&request.owner.binding.session_id, &request.key)
            .await
            .unwrap();
        assert_eq!(source.is_some(), point == 2);
        if point == 2 {
            assert_eq!(source, Some(request.reconciliation().unwrap()));
            assert!(matches!(
                store.reserve_preparation(request).await.unwrap(),
                SessionOwnerOutcome::Replayed(_)
            ));
        }
    }
}
async fn begin_faults(store: &impl Store, owner_fault: &impl Fn(u8)) {
    for point in [1, 2] {
        let bound = fixture(&SessionId::new(), SessionRevision::INITIAL);
        let request = reservation(&bound);
        let SessionOwnerOutcome::Applied {
            authority: Some(authority),
            ..
        } = store.reserve_preparation(request.clone()).await.unwrap()
        else {
            panic!("fresh reserve")
        };
        let begin = MutateSessionOwner {
            id: request.reconciliation().unwrap().begin_id,
            session_id: request.owner.binding.session_id.clone(),
            target: authority.target().clone(),
            command: SessionOwnerCommand::Begin,
        };
        owner_fault(point);
        assert!(matches!(
            store
                .mutate_session_owner(begin.clone(), Some(authority))
                .await,
            Err(SessionOwnerError::UnknownStoreOutcome)
        ));
        owner_fault(0);
        assert_eq!(
            store
                .lookup_session_owner(&begin.session_id, &begin.id)
                .await
                .unwrap()
                .is_some(),
            point == 2
        );
        assert!(
            store
                .lookup_admission(&begin.session_id, &bound.keyed.options.key)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_session_owner(&begin.session_id, &request.owner.binding.id)
                .await
                .unwrap()
                .state,
            if point == 1 {
                SessionOwnerState::ReservedUnstarted
            } else {
                SessionOwnerState::Active
            }
        );
    }
}
async fn native_faults(store: &impl Store, native_fault: &impl Fn(u8)) {
    for point in [1, 2] {
        let session = SessionId::new();
        let initial = fixture(&session, SessionRevision::INITIAL);
        let revision = create(store, &initial).await;
        let bound = fixture(&session, revision);
        let request = reservation(&bound);
        let grant = begin(store, &request).await;
        let before = store.session_revision(&session).await.unwrap();
        let effects = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = effects.clone();
        let original = bound.clone();
        let prepared = JoinedPreparation::observe(
            grant.clone(),
            tokio::spawn(async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, ()>(((), PreparedOperation::AdmitKeyed(Box::new(original))))
            }),
        )
        .await
        .unwrap()
        .unwrap()
        .1;
        native_fault(point);
        assert!(store.complete_preparation(prepared).await.is_err());
        native_fault(0);
        let receipt = store
            .lookup_admission(&session, &bound.keyed.options.key)
            .await
            .unwrap();
        let owner = store
            .get_session_owner(&session, &request.owner.binding.id)
            .await
            .unwrap();
        assert_eq!(receipt.is_some(), point == 2);
        assert_eq!(
            owner.state,
            if point == 1 {
                SessionOwnerState::Active
            } else {
                SessionOwnerState::Joined
            }
        );
        assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
        if point == 1 {
            assert_eq!(store.session_revision(&session).await.unwrap(), before);
            // Explicit cleanup uses retained authority after the actual task join;
            // it never retries business work or returns a native execution grant.
            let cleanup = MutateSessionOwner {
                id: request.reconciliation().unwrap().join_id,
                session_id: session.clone(),
                target: owner.target,
                command: SessionOwnerCommand::Join {
                    termination: super::super::SessionOwnerTermination::Returned,
                },
            };
            store
                .mutate_session_owner(cleanup, Some(grant.owner_authority().clone()))
                .await
                .unwrap();
            assert!(
                store
                    .lookup_admission(&session, &bound.keyed.options.key)
                    .await
                    .unwrap()
                    .is_none()
            );
            super::retry::after_cleanup(store, &session).await;
        } else {
            let after = store.session_revision(&session).await.unwrap();
            assert!(
                matches!(store.complete_preparation(joined(grant, bound).await).await.unwrap(),
                PreparationCompletion::Keyed(KeyedAdmitOutcome::Replayed(saved)) if Some(&saved) == receipt.as_ref())
            );
            assert_eq!(store.session_revision(&session).await.unwrap(), after);
        }
    }
}
