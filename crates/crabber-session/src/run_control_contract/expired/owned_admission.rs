//! A real public unkeyed preparation/admission; no private proof constructor.
use super::{
    Clock, ControlId, InputEnvelope, JoinedPreparation, ManualClock, MutateSessionOwner,
    PreparationCompletion, PreparationGrant, PreparationKey, PreparationPurpose, PreparedOperation,
    ReservePreparation, SessionId, SessionOwnerCommand, SessionOwnerOutcome, SessionRevision,
    Store, bound,
};
use crate::session_owner::preparation::{OWNED_PREPARATION_COMPONENT, bind_resource_contract};
pub(super) async fn admit(store: &impl Store, clock: &ManualClock) -> crate::AdmitOutcome {
    let joined = pending(store, clock, std::sync::Arc::default()).await;
    let PreparationCompletion::Admitted(admitted) =
        Box::pin(store.complete_preparation(joined)).await.unwrap()
    else {
        panic!("fresh unkeyed admission");
    };
    admitted
}
pub(super) async fn pending(
    store: &impl Store,
    clock: &ManualClock,
    effects: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> JoinedPreparation {
    let session = SessionId::new();
    let initial = bound(&session, clock.now());
    let revision = store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: initial.binding.request.workspace_id,
                directory: initial.binding.request.directory,
                title: "unkeyed source".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap()
        .revision;
    let mut bound = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            &session,
            clock.now(),
            "expired-owned-unkeyed",
            'b',
        ),
        InputEnvelope::text("original unkeyed input", revision).unwrap(),
        clock.now(),
    );
    let declaration = crabber_core::InputFingerprint::new("d".repeat(64)).unwrap();
    bind_resource_contract(&mut bound.binding.runtime_semantics, Some(&declaration)).unwrap();
    let config = crate::admission_config_hash(&bound.binding.runtime_semantics);
    bound.binding.config_hash = crabber_core::InputFingerprint::new(config.clone()).unwrap();
    bound.keyed.request.config_hash = config;
    bound.binding.input_fingerprint = bound.binding.computed_input_fingerprint().unwrap();
    let root = reserve(store, &bound.binding, revision, declaration).await;
    let operation = PreparedOperation::Admit(Box::new(crate::BoundAdmitRequest {
        request: bound.keyed.request,
        binding: bound.binding,
    }));
    super::owned::observe_counted(store, root, operation, effects).await
}

async fn reserve(
    store: &impl Store,
    binding: &crate::ExecutionBindingCapsule,
    revision: SessionRevision,
    contract: crabber_core::InputFingerprint,
) -> PreparationGrant {
    let mut owner = crate::session_owner_contract::reservation(binding.request.session_id.clone());
    owner
        .binding
        .workspace_id
        .clone_from(&binding.request.workspace_id);
    owner
        .binding
        .directory
        .clone_from(&binding.request.directory);
    owner
        .binding
        .behavior_fingerprint
        .clone_from(&binding.behavior_fingerprint);
    owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    owner.lease_ms = 30_000;
    let reservation = ReservePreparation {
        owner,
        key: PreparationKey::Unkeyed(ControlId::new()),
        purpose: PreparationPurpose::OrdinaryAdmission,
        expected_revision: revision,
        source: None,
        resource_contract: Some(contract),
        intent: crate::session_owner::preparation::admission_intent(
            &binding.request,
            binding.input_envelope.as_ref().unwrap(),
            None,
        )
        .unwrap(),
    };
    let reservation = reservation.clone().bind_key(reservation.key).unwrap();
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store
        .reserve_preparation(reservation.clone())
        .await
        .unwrap()
    else {
        panic!("fresh unkeyed preparation");
    };
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: reservation.reconciliation().unwrap().begin_id,
                session_id: binding.request.session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    PreparationGrant::from_fresh_begin(reservation, authority).unwrap()
}
