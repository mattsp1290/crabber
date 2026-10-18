//! Actual preparation reservations made only after freezing the fixed profile.
use super::*;
use crate::{
    AdmissionSelectionPolicy, BoundAdmitRequest, BoundKeyedAdmitRequest, ManageSession,
    SessionChange, SessionDefaults, Store,
    session_owner::preparation::{
        OWNED_PREPARATION_COMPONENT, PreparationKey, PreparationPurpose, ReservePreparation,
        admission_intent,
    },
};
use crabber_core::{ControlId, SessionContext, SessionId, SessionRevision};

pub(crate) fn bound(
    session: &SessionId,
    revision: SessionRevision,
    source: &LibraryScriptedSource,
    policy: AdmissionSelectionPolicy,
) -> BoundKeyedAdmitRequest {
    let mut bound = crate::session_owner::preparation_contract::fixture(session, revision);
    let (selection, responses) = source.recipe();
    let mut proof = Proof::bounded(32 << 20);
    let descriptor = describe(
        &selection.provider_id,
        &selection.model_id,
        responses,
        &mut proof,
    )
    .unwrap();
    let contract = InputFingerprint::new("d".repeat(64)).unwrap();
    let profile = profile(&descriptor, &policy, Some(&contract), &mut proof).unwrap();
    bound.binding.provider = descriptor;
    bound
        .binding
        .request
        .provider_id
        .clone_from(&selection.provider_id);
    bound
        .binding
        .request
        .model_id
        .clone_from(&selection.model_id);
    bound.binding.request.selection_policy = policy;
    bound.binding.request.system_prompt = None;
    bound.binding.request.max_output_tokens = None;
    bound.binding.runtime_semantics = profile.semantics;
    bound.binding.behavior_fingerprint = profile.behavior;
    bound.binding.plan_fingerprint = InputFingerprint::new(profile.plan).unwrap();
    synchronize(&mut bound);
    bound
}

/// Recompute normal binding/capsule commitments before reservation. This is
/// useful for coherent unsupported-profile fixtures; it supplies no authority.
pub(crate) fn synchronize(bound: &mut BoundKeyedAdmitRequest) {
    let config = crate::admission_config_hash(&bound.binding.runtime_semantics);
    bound.binding.config_hash = InputFingerprint::new(config.clone()).unwrap();
    bound.binding.input_fingerprint = bound.binding.computed_input_fingerprint().unwrap();
    bound.keyed.request.config_hash = config;
    bound.keyed.request.plan_fingerprint = bound.binding.plan_fingerprint.as_str().into();
    bound.keyed.options.behavior_fingerprint = bound.binding.behavior_fingerprint.clone();
    let execution = bound.keyed.execution.as_mut().unwrap();
    execution.request = bound.binding.request.clone();
    execution.runtime_semantics = bound.binding.runtime_semantics.clone();
    execution.config_hash = bound.binding.config_hash.as_str().into();
    execution.plan_fingerprint = bound.binding.plan_fingerprint.as_str().into();
    execution.behavior_fingerprint = bound.binding.behavior_fingerprint.clone();
    let digest = bound.keyed.semantic_digest().unwrap();
    bound.keyed.execution.as_mut().unwrap().semantic_digest = digest;
    bound.binding.validate_keyed(&bound.keyed).unwrap();
}

pub(crate) async fn observe(
    store: &impl Store,
    keyed: bool,
    bound: BoundKeyedAdmitRequest,
) -> (SessionContext, JoinedPreparation) {
    let request = &bound.binding.request;
    let context = SessionContext::new(
        request.session_id.clone(),
        request.workspace_id.clone(),
        request.directory.clone(),
    );
    let mut owner = crate::session_owner_contract::reservation(request.session_id.clone());
    owner.binding.workspace_id.clone_from(&request.workspace_id);
    owner.binding.directory.clone_from(&request.directory);
    owner.binding.behavior_fingerprint = bound.binding.behavior_fingerprint.clone();
    owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    owner.lease_ms = 30_000;
    let reservation = ReservePreparation {
        owner,
        key: PreparationKey::Unkeyed(ControlId::new()),
        source: None,
        resource_contract: crate::session_owner::preparation::resource_contract(
            &bound.binding.runtime_semantics,
        )
        .unwrap(),
        expected_revision: bound.keyed.request.expected_revision.unwrap(),
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent: admission_intent(
            request,
            bound.binding.input_envelope.as_ref().unwrap(),
            keyed.then_some(&bound.keyed.options),
        )
        .unwrap(),
    };
    let reservation = if keyed {
        reservation
            .bind_key(PreparationKey::Admission(bound.keyed.options.key.clone()))
            .unwrap()
    } else {
        reservation
            .bind_key(PreparationKey::Unkeyed(ControlId::new()))
            .unwrap()
    };
    let root = crate::session_owner::preparation_contract::begin(store, &reservation).await;
    let operation = if keyed {
        PreparedOperation::AdmitKeyed(Box::new(bound))
    } else {
        PreparedOperation::Admit(Box::new(BoundAdmitRequest {
            request: bound.keyed.request,
            binding: bound.binding,
        }))
    };
    let original =
        crate::session_owner::preparation_contract::resources::initial_driver_observation(
            store, root, operation,
        )
        .await;
    (context, original)
}

pub(crate) async fn prepared(
    store: &impl Store,
    keyed: bool,
    source: &LibraryScriptedSource,
) -> (SessionContext, JoinedPreparation) {
    prepared_with(store, keyed, source, |_| ()).await
}

pub(crate) async fn prepared_with(
    store: &impl Store,
    keyed: bool,
    source: &LibraryScriptedSource,
    change: impl FnOnce(&mut BoundKeyedAdmitRequest),
) -> (SessionContext, JoinedPreparation) {
    let session = SessionId::new();
    let initial =
        crate::session_owner::preparation_contract::fixture(&session, SessionRevision::INITIAL);
    let revision = store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: initial.binding.request.workspace_id,
                directory: initial.binding.request.directory,
                title: "fixed scripted preparation".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap()
        .revision;
    let mut bound = bound(
        &session,
        revision,
        source,
        AdmissionSelectionPolicy::Explicit,
    );
    change(&mut bound);
    synchronize(&mut bound);
    observe(store, keyed, bound).await
}
