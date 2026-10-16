//! Fresh grants require live original scope; original metadata remains readable.
use super::*;
pub(crate) async fn run(store: &impl Store, clock: &crabber_core::ManualClock) {
    for linked in [false, true] {
        clock.set(OffsetDateTime::UNIX_EPOCH);
        let (original, bound) = setup(store, false).await;
        let grant = if linked {
            let PreparationResourceOutcome::Applied { grant, .. } = store
                .retain_preparation_resources(original.clone())
                .await
                .unwrap()
            else {
                panic!("fresh child")
            };
            *grant
        } else {
            original.clone()
        };
        clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(31));
        if linked {
            assert!(matches!(
                store.retain_preparation_resources(original).await.unwrap(),
                PreparationResourceOutcome::Replayed(_)
            ));
        } else {
            assert!(store.retain_preparation_resources(original).await.is_err());
        }
        assert!(
            store
                .complete_preparation(joined(grant, bound.clone()).await)
                .await
                .is_err()
        );
        assert!(
            store
                .lookup_admission(&bound.binding.request.session_id, &bound.keyed.options.key)
                .await
                .unwrap()
                .is_none()
        );
    }
    clock.set(OffsetDateTime::UNIX_EPOCH);
    let (original, _) = setup(store, false).await;
    let (a, b) = tokio::join!(
        store.retain_preparation_resources(original.clone()),
        store.retain_preparation_resources(original)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(matches!(
        (&a, &b),
        (
            PreparationResourceOutcome::Applied { .. },
            PreparationResourceOutcome::Replayed(_)
        ) | (
            PreparationResourceOutcome::Replayed(_),
            PreparationResourceOutcome::Applied { .. }
        )
    ));
}
pub(crate) async fn source_fixture(
    store: &impl Store,
) -> (
    PreparationGrant,
    crate::BoundKeyedAdmitRequest,
    crabber_core::SessionOwnerOperationId,
) {
    let (original, bound) = setup(store, false).await;
    let begin = original.reconciliation().unwrap().begin_id;
    let PreparationResourceOutcome::Applied { grant, .. } =
        store.retain_preparation_resources(original).await.unwrap()
    else {
        panic!("fresh child")
    };
    (*grant, bound, begin)
}
pub(crate) async fn source_refuses(
    store: &impl Store,
    grant: PreparationGrant,
    bound: crate::BoundKeyedAdmitRequest,
) {
    let session = &bound.binding.request.session_id;
    assert!(matches!(
        store
            .reserve_preparation(grant.original_reservation().clone())
            .await
            .unwrap(),
        SessionOwnerOutcome::Replayed(_)
    ));
    assert!(
        store
            .get_session_owner(
                session,
                &grant.retained_resources_authority().unwrap().target().id
            )
            .await
            .is_err()
    );
    assert!(
        store
            .retain_preparation_resources(grant.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .complete_preparation(joined(grant, bound.clone()).await)
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_admission(session, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
}

pub(crate) async fn handoff_fixture(
    store: &impl Store,
    native: bool,
) -> (PreparationGrant, crate::BoundKeyedAdmitRequest) {
    let (original, bound) = setup(store, false).await;
    let PreparationResourceOutcome::Applied { grant, .. } =
        store.retain_preparation_resources(original).await.unwrap()
    else {
        panic!("fresh child")
    };
    if native {
        store
            .complete_preparation(joined((*grant).clone(), bound.clone()).await)
            .await
            .unwrap();
    } else {
        tokio::spawn(async {}).await.unwrap();
        store
            .mutate_session_owner(
                MutateSessionOwner {
                    id: grant.reconciliation().unwrap().join_id,
                    session_id: bound.binding.request.session_id.clone(),
                    target: grant.owner_authority().target().clone(),
                    command: SessionOwnerCommand::Join {
                        termination: crate::session_owner::SessionOwnerTermination::Returned,
                    },
                },
                Some(grant.owner_authority().clone()),
            )
            .await
            .unwrap();
        // A separate native writer wins after real preparation cleanup. Its
        // receipt cannot turn that cleanup into an authenticated handoff.
        store.admit_bound_keyed_run(bound.clone()).await.unwrap();
        assert!(
            store
                .complete_preparation(joined((*grant).clone(), bound.clone()).await)
                .await
                .is_err()
        );
    }
    (*grant, bound)
}
pub(crate) async fn handoff_refuses(
    store: &impl Store,
    grant: PreparationGrant,
    bound: crate::BoundKeyedAdmitRequest,
) {
    let metadata = grant.reconciliation().unwrap();
    assert!(
        store
            .get_session_owner(&metadata.session_id, &metadata.owner.id)
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_session_owner(&metadata.session_id, &metadata.join_id)
            .await
            .is_err()
    );
    assert!(
        store
            .complete_preparation(joined(grant, bound.clone()).await)
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_admission(&metadata.session_id, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_some()
    );
}
