//! Actual native service setup shared with adapter fault tests.
use crate::session_owner::preparation::{
    PreparationGrant, PreparationKey, PreparationPurpose, ReservePreparation,
};
use crate::session_owner::service::{
    SessionServiceCleanupPolicy, SessionServiceGeneration, SessionServiceGenerationScope,
};
use crate::session_owner::{MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome};
use crate::{MemoryStore, Store};
use crabber_core::{ControlId, InputFingerprint, SessionId, SessionRevision};

pub(crate) async fn grant() -> PreparationGrant {
    setup().await.1
}

pub(crate) async fn setup() -> (
    MemoryStore,
    PreparationGrant,
    crate::session_owner::preparation::PreparationResourceReceipt,
) {
    let store = MemoryStore::new();
    let mut owner = crate::session_owner_contract::reservation(SessionId::new());
    owner.lease_ms = 30_000;
    owner.binding.component_id =
        crate::session_owner::preparation::OWNED_PREPARATION_COMPONENT.into();
    let key = PreparationKey::Unkeyed(ControlId::new());
    let request = ReservePreparation {
        owner,
        key: key.clone(),
        source: None,
        resource_contract: Some(InputFingerprint::new("a".repeat(64)).unwrap()),
        expected_revision: SessionRevision::INITIAL,
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
    .bind_key(key)
    .unwrap();
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_preparation(request.clone()).await.unwrap()
    else {
        panic!("fresh root");
    };
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: request.reconciliation().unwrap().begin_id,
                session_id: request.owner.binding.session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let grant = PreparationGrant::from_fresh_begin(request, authority).unwrap();
    let crate::session_owner::preparation::PreparationResourceOutcome::Applied { grant, receipt } =
        store.retain_preparation_resources(grant).await.unwrap()
    else {
        panic!("fresh resources");
    };
    (store, *grant, *receipt)
}

pub(crate) fn generation() -> SessionServiceGeneration {
    SessionServiceGeneration {
        id: "actual-local-generation".into(),
        scope: SessionServiceGenerationScope::Shared,
        registration_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
        cleanup_policy: SessionServiceCleanupPolicy::LegacySharedMount,
    }
}

pub(crate) async fn activated_setup() -> (MemoryStore, PreparationGrant) {
    use crate::session_owner::owned_scope::{
        OwnedScopeParentAuthority, OwnedScopeSource, OwnedTaskRegistration,
    };
    let (store, grant, receipt) = setup().await;
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        || async {},
    )
    .unwrap();
    let crate::session_owner::owned_scope::OwnedScopeActivationOutcome::Applied { permit, .. } =
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.retained_resources_authority().unwrap().clone(),
                Some(OwnedScopeParentAuthority::preparation(grant.clone())),
            )
            .await
            .unwrap()
    else {
        panic!("fresh retained activation")
    };
    task.dispatch(*permit).unwrap();
    task.join().await.unwrap();
    (store, grant)
}
