//! An owned unkeyed commit can reconcile metadata without replaying dispatch.
use super::*;
use crate::session_owner::preparation::{PreparationKey, admission_intent};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
async fn setup(store: &impl Store) -> (PreparationGrant, crate::BoundAdmitRequest) {
    let session = SessionId::new();
    let revision = create(store, &fixture(&session, SessionRevision::INITIAL)).await;
    let keyed = fixture(&session, revision);
    let mut request = reservation(&keyed);
    request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    request.intent = admission_intent(
        &keyed.binding.request,
        keyed.binding.input_envelope.as_ref().unwrap(),
        None,
    )
    .unwrap();
    let request = request
        .bind_key(PreparationKey::Unkeyed(ControlId::new()))
        .unwrap();
    let original = begin(store, &request).await;
    let PreparationResourceOutcome::Applied { grant, .. } =
        store.retain_preparation_resources(original).await.unwrap()
    else {
        panic!("fresh child");
    };
    (
        *grant,
        crate::BoundAdmitRequest {
            request: keyed.keyed.request,
            binding: keyed.binding,
        },
    )
}
async fn observed(
    grant: PreparationGrant,
    bound: crate::BoundAdmitRequest,
    calls: Arc<AtomicUsize>,
) -> JoinedPreparation {
    JoinedPreparation::observe(
        grant,
        tokio::spawn(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(((), PreparedOperation::Admit(Box::new(bound))))
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .1
}
pub(super) async fn run(store: &impl Store) {
    let (grant, bound) = setup(store).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let proof = observed(grant.clone(), bound.clone(), calls.clone()).await;
    let PreparationCompletion::Admitted(first) =
        store.complete_preparation(proof.clone()).await.unwrap()
    else {
        panic!("fresh admission authority");
    };
    let revision = store.session_revision(&first.session.id).await.unwrap();
    let replay = store.complete_preparation(proof).await.unwrap();
    assert!(replay.is_replayed());
    let PreparationCompletion::AdmissionReplayed(saved) = replay else {
        panic!("metadata only");
    };
    assert_eq!(saved.session_id, first.session.id);
    assert_eq!(saved.run_id, first.run.id);
    let PreparationKey::Unkeyed(id) = &grant.original_reservation().key else {
        panic!();
    };
    assert_eq!(&saved.operation_id, id);
    assert_eq!(
        store.session_revision(&saved.session_id).await.unwrap(),
        revision
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut changed = bound;
    changed.binding.plan_fingerprint = InputFingerprint::new("c".repeat(64)).unwrap();
    changed.request.plan_fingerprint = changed.binding.plan_fingerprint.as_str().to_owned();
    assert!(
        store
            .complete_preparation(observed(grant, changed, Arc::new(AtomicUsize::new(0))).await)
            .await
            .is_err()
    );
    assert_eq!(
        store.session_revision(&saved.session_id).await.unwrap(),
        revision
    );
}
pub(super) async fn faults(store: &impl Store, native_fault: &impl Fn(u8)) {
    for point in [1, 2] {
        let (grant, bound) = setup(store).await;
        let session = bound.binding.request.session_id.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let proof = observed(grant.clone(), bound, calls.clone()).await;
        native_fault(point);
        assert!(store.complete_preparation(proof.clone()).await.is_err());
        native_fault(0);
        let retry = store.complete_preparation(proof.clone()).await.unwrap();
        assert_eq!(retry.is_replayed(), point == 2);
        let run = match retry {
            PreparationCompletion::Admitted(admitted) => admitted.run.id,
            PreparationCompletion::AdmissionReplayed(receipt) => receipt.run_id,
            _ => panic!("unkeyed completion"),
        };
        let revision = store.session_revision(&session).await.unwrap();
        assert!(
            matches!(store.complete_preparation(proof).await.unwrap(), PreparationCompletion::AdmissionReplayed(saved) if saved.run_id == run)
        );
        assert_eq!(store.session_revision(&session).await.unwrap(), revision);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .get_session_owner(
                    &session,
                    &grant.retained_resources_authority().unwrap().target().id
                )
                .await
                .unwrap()
                .state,
            SessionOwnerState::Active
        );
    }
}
