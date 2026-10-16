use super::*;
use crate::Store;
use crate::session_owner_contract::services::{activated_setup, generation, grant, setup};
#[tokio::test]
async fn stable_service_keys_do_not_rebind_changed_original_root_or_choice() {
    let grant = grant().await;
    let original = SessionServiceLink::for_grant(&grant, generation()).unwrap();
    let request = original.reservation(&grant).unwrap();
    let ids = original.identities().unwrap();
    let digest = original.fingerprint(&request).unwrap();
    let mut changed = original.clone();
    changed.root.root_reserve = SessionOwnerOperationId::new();
    assert_eq!(changed.identities().unwrap(), ids);
    assert_ne!(changed.fingerprint(&request).unwrap(), digest);
    assert!(matches!(
        changed.reservation(&grant),
        Err(Error::InvalidRequest)
    ));
    changed = original;
    changed.generation.registration_fingerprint = InputFingerprint::new("d".repeat(64)).unwrap();
    assert_eq!(changed.identities().unwrap(), ids);
    assert!(matches!(
        changed.fingerprint(&request),
        Err(Error::InvalidRequest)
    ));
    assert_ne!(
        changed
            .fingerprint(&changed.reservation(&grant).unwrap())
            .unwrap(),
        digest
    );
}

#[tokio::test]
async fn service_scope_and_physical_cleanup_policy_cannot_drift() {
    let grant = grant().await;
    let mut captured = generation();
    captured.scope = SessionServiceGenerationScope::Workspace {
        workspace_id: grant
            .original_reservation()
            .owner
            .binding
            .workspace_id
            .clone(),
        directory: "/different-root".into(),
    };
    captured.cleanup_policy = SessionServiceCleanupPolicy::SealedFactoryLegacyMount;
    assert!(matches!(
        SessionServiceLink::for_grant(&grant, captured),
        Err(Error::InvalidRequest)
    ));
    let mut captured = generation();
    captured.cleanup_policy = SessionServiceCleanupPolicy::SealedFactoryLegacyMount;
    assert!(matches!(
        SessionServiceLink::for_grant(&grant, captured),
        Err(Error::InvalidRequest)
    ));
}

#[tokio::test]
async fn removing_current_service_source_cannot_turn_typed_origin_into_generic_owner() {
    use crate::session_owner::{
        SessionOwnerOperation,
        audit::{Audit, Publication, RevisionState, verify_current},
        record::Record,
    };
    let grant = grant().await;
    let link = SessionServiceLink::for_grant(&grant, generation()).unwrap();
    let request = link.reservation(&grant).unwrap();
    let mut record = Record::reserve(&request, None, time::OffsetDateTime::UNIX_EPOCH).unwrap();
    // None keeps the exact legacy codec; a generic reservation cannot attach a
    // typed source merely by copying the Runtime component/contract labels.
    assert!(
        serde_json::to_value(&record)
            .unwrap()
            .get("service")
            .is_none()
    );
    record.service = Some(Box::new(link.clone()));
    let origin = Audit {
        version: 2,
        receipt: record.receipt(
            request.id.clone(),
            link.fingerprint(&request).unwrap(),
            SessionOwnerOperation::Reserve,
        ),
        record: record.clone(),
        publication: Some(Publication::new(RevisionState::Absent(0), true).unwrap()),
    };
    verify_current(&record, &origin, &origin).unwrap();
    record.service = None;
    let mut rolled_back_latest = origin.clone();
    rolled_back_latest.record = record.clone();
    assert!(matches!(
        verify_current(&record, &origin, &rolled_back_latest),
        Err(Error::UnknownStoreOutcome)
    ));
}

#[tokio::test]
async fn service_register_requires_resource_activation_and_replays_original_receipts() {
    let (store, grant, _) = setup().await;
    let request = RegisterSessionServiceDependency {
        grant,
        generation: generation(),
    };
    assert!(matches!(
        store.register_session_service_dependency(request).await,
        Err(Error::StaleOwner)
    ));
    let (store, grant) = activated_setup().await;
    let request = RegisterSessionServiceDependency {
        grant,
        generation: generation(),
    };
    let SessionServiceDependencyOutcome::Applied { receipt, .. } = store
        .register_session_service_dependency(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh service")
    };
    receipt.verify().unwrap();
    let SessionServiceDependencyOutcome::Replayed(replayed) = store
        .register_session_service_dependency(request.clone())
        .await
        .unwrap()
    else {
        panic!("metadata replay only")
    };
    assert_eq!(receipt, replayed);
    let mut changed = request;
    changed.generation.registration_fingerprint = InputFingerprint::new("e".repeat(64)).unwrap();
    assert!(matches!(
        store.register_session_service_dependency(changed).await,
        Err(Error::SemanticConflict)
    ));
}

#[tokio::test]
async fn only_exact_service_activation_enters_preparation_lineage() {
    use crate::session_owner::owned_scope::{
        OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
        OwnedTaskRegistration,
    };
    let (store, grant) = activated_setup().await;
    let request = RegisterSessionServiceDependency {
        grant: grant.clone(),
        generation: generation(),
    };
    let SessionServiceDependencyOutcome::Applied { receipt, authority } = store
        .register_session_service_dependency(request)
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
    let effects = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = effects.clone();
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        move || async move {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .unwrap();
    assert!(matches!(
        store
            .activate_owned_scope(task.activation().clone(), authority.clone(), None)
            .await,
        Err(Error::InvalidRequest)
    ));
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            task.activation().clone(),
            authority,
            Some(OwnedScopeParentAuthority::ServicePreparation(Box::new(
                grant,
            ))),
        )
        .await
        .unwrap()
    else {
        panic!("fresh service activation")
    };
    task.dispatch(*permit).unwrap();
    task.join().await.unwrap();
    store
        .check_session_service_dependency_access(access.clone())
        .await
        .unwrap();
    store
        .check_session_service_dependency_access(access.clone())
        .await
        .unwrap();
    store
        .request_session_owner_cancel(crate::session_owner::RequestSessionOwnerCancel {
            id: SessionOwnerOperationId::new(),
            session_id: receipt.source.root.session_id.clone(),
            target: access.authority.target().clone(),
            expected_revision: None,
            claim: crate::session_owner::SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "cancel this exact retained dependency".into(),
            },
        })
        .await
        .unwrap();
    // Sharing the original preparation is not permission to absorb an external
    // cancellation into its own revision lineage or expose the service again.
    assert!(
        store
            .check_session_service_dependency_access(access)
            .await
            .is_err()
    );
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn oversized_service_generation_is_refused_before_copying() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (store, grant) = runtime.block_on(activated_setup());
    let mut generation = generation();
    generation.id = "x".repeat(8 * 1024 * 1024);
    let request = RegisterSessionServiceDependency { grant, generation };
    let measured = allocation_counter::measure(|| {
        assert!(matches!(
            runtime.block_on(store.register_session_service_dependency(request)),
            Err(Error::InvalidRequest)
        ));
    });
    assert!(
        measured.bytes_max < 131_072,
        "generation copied before bounds: {measured:?}"
    );
}

#[tokio::test]
async fn fully_sealed_policy_is_metadata_for_workspace_and_session_dependencies() {
    let grant = grant().await;
    let binding = &grant.original_reservation().owner.binding;
    let mut generation = generation();
    generation.cleanup_policy = SessionServiceCleanupPolicy::FullySealedFactory;
    assert!(SessionServiceLink::for_grant(&grant, generation.clone()).is_err());
    for scope in [
        SessionServiceGenerationScope::Workspace {
            workspace_id: binding.workspace_id.clone(),
            directory: binding.directory.clone(),
        },
        SessionServiceGenerationScope::Session {
            session_id: binding.session_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            directory: binding.directory.clone(),
        },
    ] {
        generation.scope = scope;
        // Accepting captured policy does not classify physical service cleanup.
        SessionServiceLink::for_grant(&grant, generation.clone()).unwrap();
    }
}

#[tokio::test]
async fn cancelled_service_activation_is_known_nondispatch() {
    use crate::session_owner::owned_scope::{
        OwnedScopeParentAuthority, OwnedScopeSource, OwnedTaskRegistration,
    };
    let (store, grant) = activated_setup().await;
    let request = RegisterSessionServiceDependency {
        grant: grant.clone(),
        generation: generation(),
    };
    let SessionServiceDependencyOutcome::Applied { receipt, authority } = store
        .register_session_service_dependency(request)
        .await
        .unwrap()
    else {
        panic!("fresh service")
    };
    store
        .request_session_owner_cancel(crate::session_owner::RequestSessionOwnerCancel {
            id: SessionOwnerOperationId::new(),
            session_id: receipt.source.root.session_id.clone(),
            target: authority.target().clone(),
            expected_revision: None,
            claim: crate::session_owner::SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "stop this exact dependency before dispatch".into(),
            },
        })
        .await
        .unwrap();
    let effects = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = effects.clone();
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        move || async move {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .unwrap();
    assert!(matches!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                authority,
                Some(OwnedScopeParentAuthority::ServicePreparation(Box::new(
                    grant
                )))
            )
            .await,
        Err(Error::CancellationPending)
    ));
    assert_eq!(
        task.join().await.unwrap().disposition(),
        crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
    );
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 0);
}
