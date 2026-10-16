//! Shared management contract; credentials and execution factories are unnecessary.
use crate::{
    ListSessions, ManageSession, ManagementError as Error, SessionChange, SessionDefaults, Store,
};
use crabber_core::{ControlId, SessionId, SessionRevision};
pub(crate) fn create() -> ManageSession {
    ManageSession {
        id: ControlId::new(),
        session_id: SessionId::new(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: uuid::Uuid::new_v4().to_string(),
            directory: "/workspace".into(),
            title: "empty".into(),
            defaults: SessionDefaults {
                provider_id: Some("host-provider".into()),
                model_id: Some("host-model".into()),
            },
        },
    }
}
/// Prove keyed metadata control, revision CAS, safe listing and archive admission.
/// # Panics
/// Panics when the supplied store violates the management contract.
#[allow(clippy::too_many_lines)] // One reusable end-to-end transaction contract.
pub async fn run_contract(store: &dyn Store) {
    let request = create();
    let receipt = store.manage_session(request.clone()).await.unwrap();
    assert_eq!(receipt.revision.get(), 1);
    assert_eq!(
        receipt.summary.defaults.model_id.as_deref(),
        Some("host-model")
    );
    assert!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .into_iter()
            .all(|run| run.session_id != request.session_id)
    );
    assert_eq!(
        store.list_all_messages(&request.session_id).await.unwrap(),
        Vec::<crabber_core::Message>::new()
    );
    assert_eq!(
        store
            .list_events(&request.session_id, None, 10)
            .await
            .unwrap(),
        Vec::<crabber_core::EventRecord>::new()
    );
    assert_eq!(
        store.manage_session(request.clone()).await.unwrap(),
        receipt
    );
    assert_eq!(
        store
            .lookup_session_management(&request.session_id, &request.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    let mut changed = request.clone();
    changed.change = SessionChange::Rename {
        title: "changed".into(),
    };
    assert_eq!(
        store.manage_session(changed).await.unwrap_err(),
        Error::SemanticConflict
    );
    let mut conflicting_create = request.clone();
    conflicting_create.id = ControlId::new();
    assert_eq!(
        store.manage_session(conflicting_create).await.unwrap_err(),
        Error::SemanticConflict
    );
    let rename = ManageSession {
        id: ControlId::new(),
        session_id: request.session_id.clone(),
        expected_revision: receipt.revision,
        change: SessionChange::Rename {
            title: "renamed".into(),
        },
    };
    let mut defaults = rename.clone();
    defaults.id = ControlId::new();
    defaults.change = SessionChange::SetDefaults {
        defaults: SessionDefaults::default(),
    };
    let (a, b) = futures::join!(
        store.manage_session(rename.clone()),
        store.manage_session(defaults)
    );
    assert!(matches!(
        (&a, &b),
        (Ok(_), Err(Error::RevisionConflict)) | (Err(Error::RevisionConflict), Ok(_))
    ));
    let current = store.session_summary(&request.session_id).await.unwrap();
    let archive = ManageSession {
        id: ControlId::new(),
        session_id: request.session_id.clone(),
        expected_revision: current.revision,
        change: SessionChange::SetArchived { archived: true },
    };
    let archived = store.manage_session(archive.clone()).await.unwrap();
    assert!(archived.summary.archived);
    let mut list = ListSessions {
        workspace_id: receipt.summary.workspace_id.clone(),
        include_archived: false,
        limit: 1,
        cursor: None,
    };
    assert_eq!(
        store.list_sessions(list.clone()).await.unwrap().sessions,
        Vec::<crate::SessionSummary>::new()
    );
    list.include_archived = true;
    assert_eq!(
        store.list_sessions(list.clone()).await.unwrap().sessions,
        vec![archived.summary.clone()]
    );
    // Fresh ordinary admission refuses archive; identical old controls still replay.
    assert_eq!(
        store
            .admit_run(admission(&archived.summary))
            .await
            .unwrap_err(),
        crate::StoreError::Conflict
    );
    assert_eq!(
        store.manage_session(request.clone()).await.unwrap(),
        receipt
    );
    let unarchive = ManageSession {
        id: ControlId::new(),
        session_id: request.session_id.clone(),
        expected_revision: archived.revision,
        change: SessionChange::SetArchived { archived: false },
    };
    let active = store.manage_session(unarchive).await.unwrap();
    assert!(!active.summary.archived);
    let admitted = store.admit_run(admission(&active.summary)).await.unwrap();
    assert_eq!(admitted.session.directory, receipt.summary.directory);
    assert_eq!(
        store
            .lookup_public_state_mutation(&request.session_id, &request.id)
            .await
            .unwrap_err(),
        crate::PublicStateError::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&request.session_id, &request.id)
            .await
            .unwrap_err(),
        crate::AttemptError::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_phase_mutation(&request.session_id, &request.id)
            .await
            .unwrap_err(),
        crate::PhaseError::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_approval(&request.session_id, &request.id)
            .await
            .unwrap_err(),
        crate::ApprovalError::SemanticConflict
    );
    let state = crate::PublicStateMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(&request.session_id).await.unwrap(),
        contents: crabber_core::PublicStateContents::default(),
    };
    store
        .execution(admitted.fence)
        .await
        .unwrap()
        .mutate_public_state(state.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .lookup_session_management(&request.session_id, &state.id)
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
    // Admission-created sessions use a lazy empty metadata baseline.
    let mut implicit = admission(&receipt.summary);
    implicit.workspace_id = uuid::Uuid::new_v4().to_string();
    implicit.session_id = None;
    implicit.user_message.session_id = SessionId::new();
    let implicit = store.admit_run(implicit).await.unwrap();
    let implicit_summary = store.session_summary(&implicit.session.id).await.unwrap();
    assert!(!implicit_summary.archived);
    assert_eq!(implicit_summary.defaults, SessionDefaults::default());
    paging(store, &receipt.summary.workspace_id).await;
    keyed_archive_replay(store).await;
    archive_admission_race(store).await;
}
pub(crate) fn admission(summary: &crate::SessionSummary) -> crate::AdmitRequest {
    crate::AdmitRequest {
        expected_revision: None,
        input_envelope: None,
        additional_messages: Vec::new(),
        session_id: Some(summary.session_id.clone()),
        workspace_id: summary.workspace_id.clone(),
        directory: summary.directory.clone(),
        title: summary.title.clone(),
        user_message: crabber_core::Message {
            id: crabber_core::MessageId::new(),
            session_id: summary.session_id.clone(),
            run_id: None,
            role: crabber_core::Role::User,
            parent_id: None,
            created_at: time::OffsetDateTime::now_utc(),
            parts: Vec::new(),
        },
        config_hash: "config".into(),
        plan_fingerprint: "plan".into(),
        owner: "owner".into(),
        lease: std::time::Duration::from_secs(30),
    }
}
async fn paging(store: &dyn Store, workspace: &str) {
    for _ in 0..3 {
        let mut request = create();
        let SessionChange::Create { workspace_id, .. } = &mut request.change else {
            unreachable!()
        };
        *workspace_id = workspace.into();
        store.manage_session(request).await.unwrap();
    }
    let mut list = ListSessions {
        workspace_id: workspace.into(),
        include_archived: false,
        limit: 2,
        cursor: None,
    };
    let first = store.list_sessions(list.clone()).await.unwrap();
    assert_eq!(first.sessions.len(), 2);
    list.cursor = first.next;
    let second = store.list_sessions(list.clone()).await.unwrap();
    assert_eq!(second.sessions.len(), 2);
    assert!(second.next.is_none());
    assert!(first.sessions.last().unwrap().session_id < second.sessions[0].session_id);
    let mut wrong_visibility = list.clone();
    wrong_visibility.include_archived = true;
    assert_eq!(
        store.list_sessions(wrong_visibility).await.unwrap_err(),
        Error::InvalidRequest
    );
    for limit in [0, 101] {
        let mut invalid = list.clone();
        invalid.limit = limit;
        assert_eq!(
            store.list_sessions(invalid).await.unwrap_err(),
            Error::InvalidRequest
        );
    }
    list.workspace_id.push('x');
    assert_eq!(
        store.list_sessions(list).await.unwrap_err(),
        Error::InvalidRequest
    );
}
#[cfg(test)]
pub(crate) async fn faults(store: &dyn Store, arm: impl Fn(u8)) {
    let mut request = create();
    for next_change in [
        None,
        Some(SessionChange::Rename {
            title: "fault rename".into(),
        }),
        Some(SessionChange::SetDefaults {
            defaults: SessionDefaults::default(),
        }),
        Some(SessionChange::SetArchived { archived: true }),
    ] {
        if let Some(change) = next_change {
            request.id = ControlId::new();
            request.expected_revision = store.session_revision(&request.session_id).await.unwrap();
            request.change = change;
        }
        let before = store.session_summary(&request.session_id).await;
        arm(1);
        assert_eq!(
            store.manage_session(request.clone()).await.unwrap_err(),
            Error::UnknownStoreOutcome
        );
        arm(0);
        assert_eq!(store.session_summary(&request.session_id).await, before);
        assert!(
            store
                .lookup_session_management(&request.session_id, &request.id)
                .await
                .unwrap()
                .is_none()
        );
        arm(2);
        assert_eq!(
            store.manage_session(request.clone()).await.unwrap_err(),
            Error::UnknownStoreOutcome
        );
        // Receipt replay must remain available while acknowledgement loss is
        // still injected; replay itself is never a fresh committing write.
        let replayed = store.manage_session(request.clone()).await.unwrap();
        arm(0);
        let saved = store
            .lookup_session_management(&request.session_id, &request.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed, saved);
        assert_eq!(store.manage_session(request.clone()).await.unwrap(), saved);
    }
}
async fn keyed_archive_replay(store: &dyn Store) {
    use crabber_core::{AdmissionKey, AdmissionOptions, InputFingerprint};
    let created = store.manage_session(create()).await.unwrap();
    let keyed = crate::KeyedAdmitRequest {
        request: admission(&created.summary),
        execution: None,
        options: AdmissionOptions {
            key: AdmissionKey::new("management-key").unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
        },
    };
    let crate::KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!("fresh admission");
    };
    let archived = store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: created.session_id.clone(),
            expected_revision: store.session_revision(&created.session_id).await.unwrap(),
            change: SessionChange::SetArchived { archived: true },
        })
        .await
        .unwrap();
    let crate::KeyedAdmitOutcome::Replayed(replayed) =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!("archive replay must not grant authority");
    };
    assert_eq!(receipt, replayed);
    let mut fresh = keyed;
    fresh.options.key = AdmissionKey::new("management-fresh-key").unwrap();
    assert_eq!(
        store.admit_keyed_run(fresh).await.unwrap_err(),
        crate::StoreError::Conflict
    );
    assert_eq!(
        store.session_revision(&created.session_id).await.unwrap(),
        archived.revision
    );
}
async fn archive_admission_race(store: &dyn Store) {
    let created = store.manage_session(create()).await.unwrap();
    let archive = ManageSession {
        id: ControlId::new(),
        session_id: created.session_id.clone(),
        expected_revision: created.revision,
        change: SessionChange::SetArchived { archived: true },
    };
    let (control, admitted) = futures::join!(
        store.manage_session(archive),
        store.admit_run(admission(&created.summary))
    );
    match (control, admitted) {
        (Ok(archived), Err(crate::StoreError::Conflict)) => assert!(archived.summary.archived),
        (Err(Error::RevisionConflict), Ok(admitted)) => {
            assert_eq!(admitted.session.id, created.session_id);
        }
        other => panic!("archive/admission must serialize with one revision winner: {other:?}"),
    }
}

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "management_contract/process.rs"]
pub(crate) mod process;

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
pub(crate) async fn corrupt_admission_refused(store: &dyn Store, saved: &crate::ManagementReceipt) {
    let before = store.get_session(&saved.session_id).await.unwrap();
    let error = store
        .admit_run(admission(&saved.summary))
        .await
        .unwrap_err();
    assert!(matches!(error, crate::StoreError::Validation(_)));
    assert_eq!(
        store.session_summary(&saved.session_id).await.unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert_eq!(store.get_session(&saved.session_id).await.unwrap(), before);
    assert_eq!(
        store.session_revision(&saved.session_id).await.unwrap(),
        saved.revision
    );
    assert_eq!(
        store.list_all_messages(&saved.session_id).await.unwrap(),
        Vec::<crabber_core::Message>::new()
    );
    assert_eq!(
        store
            .list_events(&saved.session_id, None, 10)
            .await
            .unwrap(),
        Vec::<crabber_core::EventRecord>::new()
    );
    assert!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .iter()
            .all(|run| run.session_id != saved.session_id)
    );
    assert_eq!(
        store
            .lookup_session_management(&saved.session_id, &saved.operation_id)
            .await
            .unwrap(),
        Some(saved.clone())
    );
}

/// The raw history cursor authenticates its captured public revision even when
/// unrelated metadata changes leave the immutable history cutoff usable.
#[cfg(test)]
pub(crate) async fn captured_history_revision(store: &dyn Store) {
    use crate::{SnapshotLimits, SnapshotOutcome, SnapshotRequest};
    let session = SessionId::new();
    let mut fixture = crate::execution_binding_contract::fixture(
        &session,
        time::OffsetDateTime::now_utc(),
        "captured-revision",
        'b',
    );
    fixture.keyed.request.session_id = None;
    let admitted = store.admit_run(fixture.keyed.request).await.unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    let mut message = crate::execution_binding_contract::fixture(
        &session,
        time::OffsetDateTime::now_utc(),
        "captured-revision",
        'b',
    )
    .keyed
    .request
    .user_message;
    message.run_id = Some(admitted.run.id);
    message.role = crabber_core::Role::Assistant;
    message.parts[0].kind = crabber_core::PartKind::AssistantText;
    execution.append_message(message).await.unwrap();
    let mut request = SnapshotRequest {
        session_id: session.clone(),
        continuation: None,
        limits: SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 100,
            text_bytes: 100_000,
            encoded_bytes: 1_000_000,
        },
    };
    let SnapshotOutcome::Page(first) = store.snapshot(request.clone()).await.unwrap() else {
        panic!("fixture first page")
    };
    assert_eq!(
        first.captured_revision,
        store.session_revision(&session).await.unwrap()
    );
    assert!(first.continuation.is_some());
    let changed = store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: first.captured_revision,
            change: SessionChange::Rename {
                title: "renamed".into(),
            },
        })
        .await
        .unwrap();
    assert!(changed.revision > first.captured_revision);
    request.continuation = first.continuation;
    let SnapshotOutcome::Page(second) = store.snapshot(request.clone()).await.unwrap() else {
        panic!("metadata preserves raw history cutoff")
    };
    assert_eq!(second.captured_revision, first.captured_revision);
    assert_eq!(second.high_water, first.high_water);
    let original = request.continuation.as_ref().unwrap().0.clone();
    let signed = original.strip_prefix("v2:").unwrap();
    request.continuation = Some(crate::SnapshotContinuation(signed.into()));
    assert!(store.snapshot(request.clone()).await.is_err());
    request.continuation = Some(crate::SnapshotContinuation(format!("v3:{signed}")));
    assert!(store.snapshot(request.clone()).await.is_err());
    let (signature, json) = signed.split_once(':').unwrap();
    let mut boundary: serde_json::Value = serde_json::from_str(json).unwrap();
    boundary["public_revision"] = serde_json::json!(changed.revision);
    request.continuation = Some(crate::SnapshotContinuation(format!(
        "v2:{signature}:{boundary}"
    )));
    assert!(store.snapshot(request.clone()).await.is_err());
    request.continuation = None;
    let SnapshotOutcome::Page(fresh) = store.snapshot(request).await.unwrap() else {
        panic!("new history capture")
    };
    assert_eq!(fresh.captured_revision, changed.revision);
}
