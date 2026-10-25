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
    let (grant, receipt) = setup_on(&store).await;
    (store, grant, receipt)
}

pub(crate) async fn setup_on(
    store: &dyn Store,
) -> (
    PreparationGrant,
    crate::session_owner::preparation::PreparationResourceReceipt,
) {
    setup_in(store, SessionId::new()).await
}
pub(crate) async fn setup_in(
    store: &dyn Store,
    session: SessionId,
) -> (
    PreparationGrant,
    crate::session_owner::preparation::PreparationResourceReceipt,
) {
    let mut owner = crate::session_owner_contract::reservation(session);
    owner.lease_ms = 30_000;
    owner.binding.component_id =
        crate::session_owner::preparation::OWNED_PREPARATION_COMPONENT.into();
    let key = PreparationKey::Unkeyed(ControlId::new());
    let expected_revision = match store.session_revision(&owner.binding.session_id).await {
        Ok(revision) => revision,
        Err(crate::StoreError::NotFound) => SessionRevision::INITIAL,
        Err(error) => panic!("session revision: {error:?}"),
    };
    let request = ReservePreparation {
        owner,
        key: key.clone(),
        source: None,
        resource_contract: Some(InputFingerprint::new("a".repeat(64)).unwrap()),
        expected_revision,
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
    (*grant, *receipt)
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
    let store = MemoryStore::new();
    let grant = activated_setup_on(&store).await;
    (store, grant)
}

pub(crate) async fn activated_setup_on(store: &dyn Store) -> PreparationGrant {
    activated_in(store, SessionId::new()).await
}
pub(crate) async fn activated_in(store: &dyn Store, session: SessionId) -> PreparationGrant {
    use crate::session_owner::owned_scope::{
        OwnedScopeParentAuthority, OwnedScopeSource, OwnedTaskRegistration,
    };
    let (grant, receipt) = setup_in(store, session).await;
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
    grant
}

pub(crate) async fn lifecycle(store: &dyn Store) {
    use crate::session_owner::owned_scope::{
        OwnedCleanupTask, OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
        OwnedTaskRegistration,
    };
    use crate::session_owner::service::{
        RegisterSessionServiceDependency, SessionServiceDependencyAccess,
        SessionServiceDependencyOutcome,
    };
    assert!(store.supports_session_service_dependencies());
    let grant = activated_setup_on(store).await;
    let request = RegisterSessionServiceDependency {
        grant: grant.clone(),
        generation: generation(),
    };
    let SessionServiceDependencyOutcome::Applied { receipt, authority } = store
        .register_session_service_dependency(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh service")
    };
    let access = SessionServiceDependencyAccess {
        grant: grant.clone(),
        source: receipt.source.clone(),
        authority: authority.clone(),
    };
    assert!(
        store
            .check_session_service_dependency_access(access.clone())
            .await
            .is_err()
    );
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let closing = std::sync::Arc::new(tokio::sync::Notify::new());
    let effects = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started, stop, count) = (entered.clone(), closing.clone(), effects.clone());
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        move || async move {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            started.notify_one();
            stop.notified().await;
        },
    )
    .unwrap();
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            task.activation().clone(),
            authority.clone(),
            Some(OwnedScopeParentAuthority::ServicePreparation(Box::new(
                grant,
            ))),
        )
        .await
        .unwrap()
    else {
        panic!("fresh activation")
    };
    task.dispatch(*permit).unwrap();
    entered.notified().await;
    store
        .check_session_service_dependency_access(access.clone())
        .await
        .unwrap();
    store
        .check_session_service_dependency_access(access)
        .await
        .unwrap();
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
    closing.notify_one();
    let mut cleanup =
        OwnedCleanupTask::from_host_task(task.activation().clone(), tokio::spawn(async { Ok(()) }));
    let proof = task
        .join()
        .await
        .unwrap()
        .observe_resources(&mut cleanup)
        .await
        .unwrap();
    store.join_owned_scope(proof, authority).await.unwrap();
    let SessionServiceDependencyOutcome::Replayed(saved) = store
        .register_session_service_dependency(request)
        .await
        .unwrap()
    else {
        panic!("historical metadata only")
    };
    assert_eq!(saved, receipt);
}

pub(crate) async fn faults(store: &dyn Store, set: impl Fn(u8)) {
    use crate::session_owner::SessionOwnerError;
    use crate::session_owner::service::{
        RegisterSessionServiceDependency, SessionServiceDependencyOutcome,
    };
    let request = RegisterSessionServiceDependency {
        grant: activated_setup_on(store).await,
        generation: generation(),
    };
    let source = request.source().unwrap();
    let id = source.identities().unwrap().reserve;
    set(1);
    assert!(matches!(
        store
            .register_session_service_dependency(request.clone())
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert!(
        store
            .lookup_session_owner(&source.root.session_id, &id)
            .await
            .unwrap()
            .is_none()
    );
    set(2);
    assert!(matches!(
        store
            .register_session_service_dependency(request.clone())
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    let SessionServiceDependencyOutcome::Replayed(receipt) = store
        .register_session_service_dependency(request.clone())
        .await
        .unwrap()
    else {
        panic!("no authority on ACK retry")
    };
    receipt.verify().unwrap();
    let mut changed = request;
    changed.generation.registration_fingerprint = InputFingerprint::new("e".repeat(64)).unwrap();
    assert!(matches!(
        store.register_session_service_dependency(changed).await,
        Err(SessionOwnerError::SemanticConflict)
    ));
    set(0);
}

#[path = "services/handoff.rs"]
pub(crate) mod handoff;
pub(crate) use handoff::run as handoff;

#[path = "services/cohort.rs"]
pub(crate) mod cohort;
pub(crate) use cohort::run as cohort;
