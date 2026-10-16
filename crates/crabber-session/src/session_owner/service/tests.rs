use super::*;
use crate::session_owner::preparation::{PreparationKey, PreparationPurpose, ReservePreparation};
use crate::session_owner::{MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome};
use crate::{MemoryStore, Store};
use crabber_core::{ControlId, SessionRevision};

async fn grant() -> PreparationGrant {
    let store = MemoryStore::new();
    let mut owner = crate::session_owner_contract::reservation(SessionId::new());
    owner.binding.component_id = super::super::preparation::OWNED_PREPARATION_COMPONENT.into();
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
    let super::super::preparation::PreparationResourceOutcome::Applied { grant, .. } =
        store.retain_preparation_resources(grant).await.unwrap()
    else {
        panic!("fresh resources");
    };
    *grant
}

fn generation() -> SessionServiceGeneration {
    SessionServiceGeneration {
        id: "actual-local-generation".into(),
        scope: SessionServiceGenerationScope::Shared,
        registration_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
        cleanup_policy: SessionServiceCleanupPolicy::LegacySharedMount,
    }
}

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
