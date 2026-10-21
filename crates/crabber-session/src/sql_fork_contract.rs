//! Native fork metadata survives independent controllers and original retry races.
use crate::{
    ForkError, ForkOutcome, ForkSession, ManageSession, SessionChange, SessionDefaults,
    SessionSummary, Store,
};
use crabber_core::{ControlId, SessionId, SessionRevision};
use crabber_test_support::ChildDatabase;

pub(crate) fn create() -> ManageSession {
    ManageSession {
        id: ControlId::new(),
        session_id: SessionId::new(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: uuid::Uuid::new_v4().to_string(),
            directory: "/fork-workspace".into(),
            title: "source".into(),
            defaults: SessionDefaults::default(),
        },
    }
}
pub(crate) fn request(source: &SessionSummary) -> ForkSession {
    crate::fork_pending_contract::request(source)
}

#[allow(clippy::too_many_lines)] // Independent controller races and immutable replay boundaries.
pub(crate) async fn guards(store: &dyn Store, cold: &dyn Store) {
    let source = store.manage_session(create()).await.unwrap().summary;
    let mut fork = request(&source);
    let rename = ManageSession {
        id: ControlId::new(),
        session_id: source.session_id.clone(),
        expected_revision: source.revision,
        change: SessionChange::Rename {
            title: "later source".into(),
        },
    };
    let updated = cold.manage_session(rename).await.unwrap();
    assert_eq!(
        store.fork_session(fork.clone()).await.unwrap_err(),
        ForkError::RevisionConflict
    );
    assert!(
        store
            .get_session(&fork.target_session_id)
            .await
            .unwrap()
            .is_none()
    );
    fork.expected_source_revision = updated.revision;
    let (a, b) = tokio::join!(
        store.fork_session(fork.clone()),
        cold.fork_session(fork.clone())
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    let saved = match (&a, &b) {
        (ForkOutcome::Applied(a), ForkOutcome::Replayed(b))
        | (ForkOutcome::Replayed(a), ForkOutcome::Applied(b))
            if a == b =>
        {
            a.clone()
        }
        _ => panic!("one application and one immutable replay"),
    };
    assert_eq!(saved.messages, 0);
    assert_eq!(saved.imported_calls, 0);
    assert_eq!(
        store
            .list_all_messages(&fork.target_session_id)
            .await
            .unwrap(),
        Vec::<crabber_core::Message>::new()
    );
    assert_eq!(
        store
            .list_events(&fork.target_session_id, None, 10)
            .await
            .unwrap(),
        Vec::<crabber_core::EventRecord>::new()
    );
    assert!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .into_iter()
            .all(|run| run.session_id != fork.target_session_id)
    );
    assert_eq!(
        cold.lookup_session_fork(&fork.target_session_id, &fork.id)
            .await
            .unwrap(),
        Some(saved.clone())
    );
    let current = cold.session_summary(&fork.target_session_id).await.unwrap();
    cold.manage_session(ManageSession {
        id: ControlId::new(),
        session_id: fork.target_session_id.clone(),
        expected_revision: current.revision,
        change: SessionChange::Rename {
            title: "later target".into(),
        },
    })
    .await
    .unwrap();
    assert!(
        matches!(store.fork_session(fork.clone()).await.unwrap(),ForkOutcome::Replayed(value) if value==saved)
    );
    let mut changed = fork.clone();
    changed.title.push('!');
    assert_eq!(
        cold.fork_session(changed).await.unwrap_err(),
        ForkError::SemanticConflict
    );
    let mut reused = fork.clone();
    reused.id = ControlId::new();
    assert_eq!(
        cold.fork_session(reused).await.unwrap_err(),
        ForkError::TargetExists
    );
    let mut context = fork.clone();
    context.id = ControlId::new();
    context.target_session_id = SessionId::new();
    context.directory.push('!');
    assert_eq!(
        cold.fork_session(context).await.unwrap_err(),
        ForkError::SemanticConflict
    );
    let mut competing = request(&updated.summary);
    let mut other = competing.clone();
    other.id = ControlId::new();
    other.title = "other choice".into();
    let (a, b) = tokio::join!(
        store.fork_session(competing.clone()),
        cold.fork_session(other)
    );
    assert!(matches!(
        (&a, &b),
        (Ok(ForkOutcome::Applied(_)), Err(ForkError::TargetExists))
            | (Err(ForkError::TargetExists), Ok(ForkOutcome::Applied(_)))
    ));
    // A reserved pre-session identity remains unavailable, even without a Run.
    competing.id = ControlId::new();
    competing.target_session_id = SessionId::new();
    store
        .reserve_session_owner(
            crate::session_owner::ReserveSessionOwner {
                id: crabber_core::SessionOwnerOperationId::new(),
                binding: crate::session_owner::SessionOwnerBinding {
                    session_id: competing.target_session_id.clone(),
                    workspace_id: competing.workspace_id.clone(),
                    directory: competing.directory.clone(),
                    id: crabber_core::SessionOwnerId::new(),
                    kind: crate::session_owner::SessionOwnerKind::Preparation,
                    component_id: "fork-guard".into(),
                    behavior_fingerprint: crabber_core::InputFingerprint::new("f".repeat(64))
                        .unwrap(),
                    resource_contract: None,
                    parent: None,
                },
                owner: "fork-fixture".into(),
                lease_ms: 30_000,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        cold.fork_session(competing).await.unwrap_err(),
        ForkError::TargetExists
    );
}

pub(crate) fn child_request() -> Option<ForkSession> {
    std::env::var("CRABBER_FORK_CHILD")
        .ok()
        .map(|raw| serde_json::from_str(&raw).unwrap())
}
pub(crate) async fn child(store: &dyn Store, request: ForkSession) {
    assert!(matches!(
        store.fork_session(request).await.unwrap(),
        ForkOutcome::Applied(_)
    ));
    // No operation receipt is returned across the process boundary.
}
pub(crate) async fn verify_cold(store: &dyn Store, request: ForkSession) {
    let saved = store
        .lookup_session_fork(&request.target_session_id, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.fork_session(request).await.unwrap(),ForkOutcome::Replayed(value) if value==saved)
    );
}
pub(crate) async fn spawn(helper: &str, request: &ForkSession, path: ChildDatabase<'_>) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_FORK_CHILD",
        serde_json::to_string(request).unwrap(),
    );
    path.configure(&mut command, "CRABBER_SQLITE_CHILD_PATH");
    let mut child = command.spawn().unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(result.expect("fork process timed out").success());
}
