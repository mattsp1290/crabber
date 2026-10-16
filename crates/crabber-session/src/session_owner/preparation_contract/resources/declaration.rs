//! Configuration declarations cannot retrofit legacy or bypass registered dispatch.
use super::*;
use crate::session_owner::preparation::{bind_resource_contract, resource_contract};

pub(super) fn bind(bound: &mut crate::BoundKeyedAdmitRequest, contract: &InputFingerprint) {
    bind_resource_contract(&mut bound.binding.runtime_semantics, Some(contract)).unwrap();
    let hash = crate::admission_config_hash(&bound.binding.runtime_semantics);
    bound.binding.config_hash = InputFingerprint::new(hash.clone()).unwrap();
    bound.keyed.request.config_hash.clone_from(&hash);
    let execution = bound.keyed.execution.as_mut().unwrap();
    execution.runtime_semantics = bound.binding.runtime_semantics.clone();
    execution.config_hash = hash;
    bound.keyed.execution.as_mut().unwrap().semantic_digest =
        bound.keyed.semantic_digest().unwrap();
    bound.binding.input_fingerprint = bound.binding.computed_input_fingerprint().unwrap();
    bound.binding.validate_keyed(&bound.keyed).unwrap();
}

pub(crate) async fn setup(store: &impl Store) -> (PreparationGrant, crate::BoundKeyedAdmitRequest) {
    let session = SessionId::new();
    let initial = fixture(&session, SessionRevision::INITIAL);
    let revision = create(store, &initial).await;
    let mut bound = fixture(&session, revision);
    let contract = InputFingerprint::new("d".repeat(64)).unwrap();
    bind(&mut bound, &contract);
    let mut request = reservation(&bound);
    request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    request.resource_contract = Some(contract);
    let grant = begin(store, &request).await;
    (grant, bound)
}

pub(super) async fn run(store: &impl Store) {
    let (root, bound) = setup(store).await;
    let PreparationResourceOutcome::Applied { grant, .. } =
        store.retain_preparation_resources(root).await.unwrap()
    else {
        panic!("fresh child")
    };
    let before = store
        .session_revision(&bound.binding.request.session_id)
        .await
        .unwrap();
    // Even a valid declared capsule and physically joined unrelated task do not
    // substitute for the registered child Activate source.
    assert!(
        store
            .complete_preparation(joined(*grant, bound.clone()).await)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .session_revision(&bound.binding.request.session_id)
            .await
            .unwrap(),
        before
    );
    assert!(
        store
            .lookup_admission(&bound.binding.request.session_id, &bound.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );

    let (root, mut legacy) = super::setup(store, true).await;
    assert!(root.original_reservation().resource_contract.is_none());
    bind(&mut legacy, &InputFingerprint::new("d".repeat(64)).unwrap());
    assert!(
        resource_contract(&legacy.binding.runtime_semantics)
            .unwrap()
            .is_some()
    );
    // A legacy original reservation cannot acquire a declaration after its
    // factory has already run, even if every other key/input field is identical.
    let before = store
        .session_revision(&legacy.binding.request.session_id)
        .await
        .unwrap();
    assert!(
        store
            .complete_preparation(joined(root, legacy.clone()).await)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .session_revision(&legacy.binding.request.session_id)
            .await
            .unwrap(),
        before
    );
    assert!(
        store
            .lookup_admission(
                &legacy.binding.request.session_id,
                &legacy.keyed.options.key
            )
            .await
            .unwrap()
            .is_none()
    );
}
