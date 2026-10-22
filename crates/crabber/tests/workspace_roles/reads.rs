use super::fixture::*;
use crabber::{session::*, *};
#[tokio::test]
async fn foreign_reads_and_admissions_are_hidden_without_provider_calls() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let b = f.context(false);
    let Admission::Started { handle, .. } = f
        .host
        .prompt_keyed_in(&b, "original", options(), None)
        .await
        .unwrap()
    else {
        panic!("new")
    };
    handle.done().await.unwrap();
    let id = b.session().session_id();
    let list = ListSessions {
        workspace_id: f.workspace_b.clone(),
        include_archived: false,
        limit: 100,
        cursor: None,
    };
    host_reads(&f, id, list.clone()).await;
    let before = f.snapshot(id).await;
    assert_eq!(
        f.worker.session_summary(id).await.unwrap_err(),
        ManagementError::NotFound
    );
    assert!(
        f.worker
            .lookup_admission(id, &options().key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.worker.list_sessions(list).await.unwrap().sessions, vec![]);
    assert!(matches!(
        f.worker
            .public_session_snapshot_v2(public_request(id))
            .await,
        Err(PublicSessionError::NotFound)
    ));
    denied(
        f.worker
            .recover_admission(id.clone(), "original", options())
            .await,
    );
    denied(
        f.worker
            .prompt_keyed_in(&b, "original", options(), None)
            .await,
    );
    denied(f.worker.prompt_in(&b, "foreign", None).await);
    let forged = ExecutionContext::new(
        SessionContext::new(id.clone(), &f.workspace_a, "/roles"),
        super::fixture::selection(),
    );
    let mut fresh = options();
    fresh.key = AdmissionKey::new("forged-birth").unwrap();
    assert!(matches!(
        f.worker
            .prompt_keyed_in(&forged, "fresh", fresh, None)
            .await,
        Err(RuntimeError::Store(StoreError::SessionIdentityMismatch))
    ));
    let report = f.worker.recover().await.unwrap();
    assert!(report.recovered.is_empty() && report.skipped.is_empty());
    let report = f.worker.recover_workspace(&f.workspace_b).await.unwrap();
    assert!(report.recovered.is_empty() && report.skipped.is_empty());
    assert_eq!(
        f.worker_native
            .list_workspace_unfinished_runs(WorkspaceRunsRequest {
                workspace_id: f.workspace_b.clone()
            })
            .await
            .unwrap(),
        vec![]
    );
    f.unchanged(id, before).await;
    f.host
        .prompt_in(&b, "host positive control", None)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    super::positive::reads(&f).await;
    f.cleanup().await;
}

async fn host_reads(f: &Fixture, id: &crabber::SessionId, list: ListSessions) {
    assert!(f.host.session_summary(id).await.is_ok());
    assert!(
        f.host
            .lookup_admission(id, &options().key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        f.host
            .recover_admission(id.clone(), "original", options())
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert!(matches!(
        f.host
            .public_session_snapshot_v2(public_request(id))
            .await
            .unwrap(),
        PublicSessionOutcomeV2::Page(_)
    ));
    assert_eq!(f.host.list_sessions(list).await.unwrap().sessions.len(), 1);
}
