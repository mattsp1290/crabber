//! Tombstone groundwork only; these fixtures do not certify erasure readiness.
use crate::deletion::Tombstone;
use crate::{DeleteSession, DeletionReceipt, SessionDisposition};
use crate::{DeletionError, ManagementError, Store, StoreError};
use crabber_core::{ControlId, SessionId, SessionRevision};
use time::OffsetDateTime;

pub(crate) fn deleted() -> Tombstone {
    deleted_for(&SessionId::new())
}
pub(crate) fn deleted_for(session: &SessionId) -> Tombstone {
    deleted_at_revision(session, SessionRevision::INITIAL)
}
pub(crate) fn deleted_at_revision(session: &SessionId, revision: SessionRevision) -> Tombstone {
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: session.clone(),
        expected_revision: revision,
    };
    Tombstone {
        format_version: 1,
        expected_revision: request.expected_revision,
        receipt: DeletionReceipt {
            session_id: request.session_id.clone(),
            operation_id: request.id.clone(),
            semantic_digest: request.fingerprint().unwrap(),
            final_revision: request.expected_revision.next().unwrap(),
            deleted_at: OffsetDateTime::UNIX_EPOCH.replace_nanosecond(1).unwrap(),
        },
    }
}

pub(crate) async fn metadata(store: &dyn Store, tombstone: &Tombstone) {
    payload_guards(store, &tombstone.receipt.session_id).await;
    queue_guards(store, &tombstone.receipt.session_id, None).await;
    let receipt = &tombstone.receipt;
    assert_eq!(
        store
            .lookup_session_deletion(&receipt.session_id, &receipt.operation_id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        store
            .session_disposition(&receipt.session_id)
            .await
            .unwrap(),
        SessionDisposition::Deleted {
            final_revision: receipt.final_revision,
            deleted_at: receipt.deleted_at
        }
    );
    assert_eq!(
        store
            .lookup_session_deletion(&receipt.session_id, &ControlId::new())
            .await
            .unwrap_err(),
        DeletionError::Deleted
    );
    let missing = SessionId::new();
    assert_eq!(
        store.session_disposition(&missing).await.unwrap(),
        SessionDisposition::Missing
    );
    assert_eq!(
        store
            .lookup_session_deletion(&missing, &ControlId::new())
            .await
            .unwrap(),
        None
    );
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    assert_eq!(
        store
            .session_disposition(&created.session_id)
            .await
            .unwrap(),
        SessionDisposition::Present {
            revision: created.revision
        }
    );
}

pub(crate) async fn creation_guards(store: &dyn Store, tombstone: &Tombstone) {
    let session = &tombstone.receipt.session_id;
    let mut create = crate::management_contract::create();
    create.session_id = session.clone();
    assert_eq!(
        store.manage_session(create).await.unwrap_err(),
        ManagementError::Deleted
    );
    assert_eq!(
        store
            .reserve_session_owner(
                crate::session_owner_contract::reservation(session.clone()),
                None
            )
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );
    let mut request = admission(session);
    assert!(matches!(
        store.admit_run(request.clone()).await,
        Err(StoreError::SessionDeleted)
    ));
    request.session_id = Some(session.clone());
    let keyed = crate::KeyedAdmitRequest {
        request,
        execution: None,
        options: crabber_core::AdmissionOptions {
            key: crabber_core::AdmissionKey::new("deleted-key").unwrap(),
            fingerprint: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
        },
    };
    assert!(matches!(
        store.admit_keyed_run(keyed).await,
        Err(StoreError::SessionDeleted)
    ));
    assert!(store.get_session(session).await.unwrap().is_none());
    assert_eq!(
        store.list_all_messages(session).await.unwrap(),
        Vec::<crabber_core::Message>::new()
    );
    fork_target_guard(store, session).await;
}

async fn fork_target_guard(store: &dyn Store, target: &SessionId) {
    let source = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap()
        .summary;
    let fork = crate::ForkSession {
        id: ControlId::new(),
        source_session_id: source.session_id,
        workspace_id: source.workspace_id,
        directory: source.directory,
        expected_source_revision: source.revision,
        boundary: crate::ForkBoundary::Empty,
        target_session_id: target.clone(),
        title: "refused".into(),
        defaults: source.defaults,
        limits: crate::SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 256,
            text_bytes: 1024 * 1024,
            encoded_bytes: 1024 * 1024,
        },
        max_proof_bytes: 1024 * 1024,
    };
    assert_eq!(
        store.fork_session(fork).await.unwrap_err(),
        crate::ForkError::Deleted
    );
}

pub(crate) fn admission(session: &SessionId) -> crate::AdmitRequest {
    let id = crabber_core::MessageId::new();
    crate::AdmitRequest {
        expected_revision: None,
        additional_messages: Vec::new(),
        input_envelope: None,
        session_id: None,
        workspace_id: "workspace".into(),
        directory: "/root".into(),
        title: "refused".into(),
        config_hash: "config".into(),
        plan_fingerprint: "plan".into(),
        owner: "host".into(),
        lease: std::time::Duration::from_secs(30),
        user_message: crabber_core::Message {
            id: id.clone(),
            session_id: session.clone(),
            run_id: None,
            role: crabber_core::Role::User,
            parent_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            parts: vec![crabber_core::Part {
                id: crabber_core::PartId::new(),
                message_id: id,
                ordinal: 0,
                kind: crabber_core::PartKind::UserInputText,
                content: crabber_core::ContentBlock::Text {
                    text: "refused".into(),
                },
            }],
        },
    }
}

pub(crate) async fn payload_guards(store: &dyn Store, deleted: &SessionId) {
    assert_eq!(
        store
            .get_extension_state("extension", deleted)
            .await
            .unwrap_err(),
        StoreError::SessionDeleted
    );
    for kind in [crate::InboxKind::Steer, crate::InboxKind::FollowUp] {
        let message = admission(deleted).user_message;
        assert_eq!(
            store
                .enqueue_inbox(deleted, kind, message)
                .await
                .unwrap_err(),
            StoreError::SessionDeleted
        );
        let foreign = admission(&SessionId::new()).user_message;
        assert_eq!(
            store
                .enqueue_inbox(deleted, kind, foreign)
                .await
                .unwrap_err(),
            StoreError::SessionDeleted
        );
    }
    let missing = SessionId::new();
    assert_eq!(
        store
            .get_extension_state("extension", &missing)
            .await
            .unwrap(),
        std::collections::BTreeMap::new()
    );
    assert_eq!(
        store
            .enqueue_inbox(
                &missing,
                crate::InboxKind::FollowUp,
                admission(&missing).user_message
            )
            .await
            .unwrap_err(),
        StoreError::NotFound
    );
}

pub(crate) async fn seed_payloads(store: &dyn Store, session: &SessionId) -> SessionRevision {
    let admitted = store.admit_run(admission(session)).await.unwrap();
    store
        .execution(admitted.fence)
        .await
        .unwrap()
        .put_extension_state(
            "extension",
            vec![("key".into(), Some("retained fixture value".into()))],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_extension_state("extension", session)
            .await
            .unwrap(),
        std::collections::BTreeMap::from([("key".into(), "retained fixture value".into())])
    );
    store
        .enqueue_inbox(
            session,
            crate::InboxKind::FollowUp,
            admission(session).user_message,
        )
        .await
        .unwrap();
    store.session_revision(session).await.unwrap()
}

pub(crate) async fn queue_guards(
    store: &dyn Store,
    session: &SessionId,
    fixture: Option<&crate::queue_contract::Fixture>,
) {
    let queue = fixture.map_or_else(crabber_core::QueueId::new, |f| f.queue.clone());
    let operation = fixture.map_or_else(ControlId::new, |f| f.enqueue.id.clone());
    assert_eq!(
        store.lookup_queue(session, &operation).await.unwrap_err(),
        StoreError::SessionDeleted
    );
    assert_eq!(
        store.get_queue(session, &queue).await.unwrap_err(),
        StoreError::SessionDeleted
    );
    assert_eq!(
        store.retained_queue(session, &queue).await.unwrap_err(),
        StoreError::SessionDeleted
    );
    assert_eq!(
        store.queue_preparation_snapshot(session).await.unwrap_err(),
        StoreError::SessionDeleted
    );
    assert_eq!(
        store
            .list_queue(crate::QueuePageRequest {
                session_id: session.clone(),
                limit: 2,
                after_sequence: None,
                expected_revision: None
            })
            .await
            .unwrap_err(),
        StoreError::SessionDeleted
    );
    let request = fixture.map_or_else(
        || crate::QueueMutation {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: SessionRevision::INITIAL,
            action: crate::QueueAction::Cancel { queue_id: queue },
        },
        |f| f.enqueue.clone(),
    );
    assert_eq!(
        store.mutate_queue(request).await.unwrap_err(),
        StoreError::SessionDeleted
    );
    let missing = SessionId::new();
    assert!(
        store
            .lookup_queue(&missing, &ControlId::new())
            .await
            .unwrap()
            .is_none()
    );
}

pub(crate) async fn cancelled_queue(store: &dyn Store) -> crate::queue_contract::Fixture {
    let fixture = crate::queue_contract::fixture(store).await;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let cancel = crate::queue_contract::mutation(
        store,
        &fixture,
        crate::QueueAction::Cancel {
            queue_id: fixture.queue.clone(),
        },
    )
    .await;
    store.mutate_queue(cancel).await.unwrap();
    fixture
}
