use super::fixture::*;
use crabber::{core::*, session::*};
#[tokio::test]
async fn foreign_management_fork_and_delete_preserve_rows() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let b = f.create(false).await;
    let id = b.session().session_id();
    let rename = ManageSession {
        id: ControlId::new(),
        session_id: id.clone(),
        expected_revision: f.native.session_revision(id).await.unwrap(),
        change: SessionChange::Rename {
            title: "renamed".into(),
        },
    };
    f.host.manage_session(rename.clone()).await.unwrap();
    let before = f.snapshot(id).await;
    assert_eq!(
        f.worker.manage_session(rename).await.unwrap_err(),
        ManagementError::NotFound
    );
    f.unchanged(id, before).await;
    let fork = ForkSession {
        id: ControlId::new(),
        source_session_id: id.clone(),
        workspace_id: f.workspace_b.clone(),
        directory: "/roles".into(),
        expected_source_revision: f.native.session_revision(id).await.unwrap(),
        boundary: ForkBoundary::Empty,
        target_session_id: SessionId::new(),
        title: "fork".into(),
        defaults: SessionDefaults::default(),
        limits: public_request(id).limits.history,
        max_proof_bytes: 32 << 20,
    };
    assert!(matches!(
        f.host.fork_session(fork.clone()).await.unwrap(),
        ForkOutcome::Applied(_)
    ));
    let before = f.snapshot(id).await;
    assert!(matches!(
        f.worker.fork_session(fork).await,
        Err(ForkError::NotFound)
    ));
    f.unchanged(id, before).await;
    let delete = DeleteSession {
        id: ControlId::new(),
        session_id: id.clone(),
        expected_revision: f.native.session_revision(id).await.unwrap(),
    };
    let before = f.snapshot(id).await;
    assert_eq!(
        f.worker.delete_session(delete.clone()).await.unwrap_err(),
        DeletionError::NotFound
    );
    f.unchanged(id, before).await;
    // Deletion's positive control must follow the integrity check so the
    // worker denial is tested while this exact target still exists.
    assert!(matches!(
        f.host.delete_session(delete).await.unwrap(),
        DeletionOutcome::Applied(_)
    ));
    let a = f.create(true).await;
    let aid = a.session().session_id();
    let rev = f.worker_native.session_revision(aid).await.unwrap();
    f.worker
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: aid.clone(),
            expected_revision: rev,
            change: SessionChange::Rename {
                title: "own".into(),
            },
        })
        .await
        .unwrap();
    super::positive::fork(&f, aid).await;
    assert!(matches!(
        f.worker
            .delete_session(DeleteSession {
                id: ControlId::new(),
                session_id: aid.clone(),
                expected_revision: f.worker_native.session_revision(aid).await.unwrap()
            })
            .await
            .unwrap(),
        DeletionOutcome::Applied(_)
    ));
    f.cleanup().await;
}
