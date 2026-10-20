//! A physically joined task still must return its originally reserved intent.
use super::*;
async fn observe(grant: PreparationGrant, operation: PreparedOperation) -> JoinedPreparation {
    JoinedPreparation::observe(
        grant,
        tokio::spawn(async move { Ok::<_, ()>(((), operation)) }),
    )
    .await
    .unwrap()
    .unwrap()
    .1
}
pub(crate) async fn run(store: &impl Store) {
    for mutation in 0..3 {
        let session = SessionId::new();
        let original = fixture(&session, SessionRevision::INITIAL);
        let request = reservation(&original);
        let grant = begin(store, &request).await;
        let mut returned = original.clone();
        let operation = match mutation {
            0 => {
                returned = crate::typed_admission_contract::prepare(
                    returned,
                    InputEnvelope::text("different original input", SessionRevision::INITIAL)
                        .unwrap(),
                    OffsetDateTime::UNIX_EPOCH,
                );
                PreparedOperation::AdmitKeyed(Box::new(returned))
            }
            1 => {
                returned.keyed.options.key =
                    crabber_core::AdmissionKey::new("another-original-key").unwrap();
                returned = crate::typed_admission_contract::prepare(
                    returned,
                    original.binding.input_envelope.clone().unwrap(),
                    OffsetDateTime::UNIX_EPOCH,
                );
                // Even existing native metadata for B cannot reconcile grant A.
                store.admit_bound_keyed_run(returned.clone()).await.unwrap();
                PreparedOperation::AdmitKeyed(Box::new(returned))
            }
            _ => PreparedOperation::Admit(Box::new(crate::BoundAdmitRequest {
                request: returned.keyed.request,
                binding: returned.binding,
            })),
        };
        let before = store.session_revision(&session).await.ok();
        assert!(
            store
                .complete_preparation(observe(grant, operation).await)
                .await
                .is_err(),
            "mutation {mutation}"
        );
        assert_eq!(store.session_revision(&session).await.ok(), before);
        assert!(
            store
                .lookup_admission(&session, &original.keyed.options.key)
                .await
                .unwrap()
                .is_none()
        );
        let descriptor = request.reconciliation().unwrap();
        assert!(
            store
                .lookup_session_owner(&session, &descriptor.join_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_session_owner(&session, &descriptor.owner.id)
                .await
                .unwrap()
                .state,
            SessionOwnerState::Active
        );
    }
    external_winner(store).await;
}
async fn external_winner(store: &impl Store) {
    let session = SessionId::new();
    let original = fixture(&session, SessionRevision::INITIAL);
    let request = reservation(&original);
    let grant = begin(store, &request).await;
    let joined = observe(
        grant.clone(),
        PreparedOperation::AdmitKeyed(Box::new(original.clone())),
    )
    .await;
    let retry = joined.clone();
    let KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_bound_keyed_run(original).await.unwrap()
    else {
        panic!("external winner")
    };
    let before = store.session_revision(&session).await.unwrap();
    assert!(store.complete_preparation(joined).await.is_err());
    assert_eq!(store.session_revision(&session).await.unwrap(), before);
    assert_eq!(
        store
            .get_session_owner(&session, &request.owner.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let cleanup = MutateSessionOwner {
        id: request.reconciliation().unwrap().join_id,
        session_id: session.clone(),
        target: grant.owner_authority().target().clone(),
        command: SessionOwnerCommand::Join {
            termination: super::super::SessionOwnerTermination::Returned,
        },
    };
    store
        .mutate_session_owner(cleanup, Some(grant.owner_authority().clone()))
        .await
        .unwrap();
    let after_cleanup = store.session_revision(&session).await.unwrap();
    assert!(matches!(store.complete_preparation(retry).await.unwrap(),
        PreparationCompletion::Keyed(KeyedAdmitOutcome::Replayed(saved)) if saved == receipt));
    assert_eq!(
        store.session_revision(&session).await.unwrap(),
        after_cleanup
    );
}
