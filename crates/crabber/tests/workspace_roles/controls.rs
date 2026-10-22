use super::{fixture::*, provider::Gate};
use crabber::{core::*, session::*, *};
use std::sync::Arc;
#[tokio::test]
async fn foreign_controls_and_abandonment_are_hidden() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let b = f.create(false).await;
    let id = b.session().session_id();
    let gate = Gate::new(1);
    let mut config = AgentConfig::new(selection());
    config.workspace_id = f.workspace_b.clone();
    config.directory = "/roles".into();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::now_utc()));
    let native = Arc::new(f.native.as_ref().clone().with_clock(clock.clone()));
    let host = Agent::builder()
        .behavior_version("workspace-roles.v1")
        .store(native)
        .provider(Arc::new(gate.clone()))
        .config(config)
        .build()
        .unwrap();
    let handle = host.prompt_in(&b, "held", None).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified())
        .await
        .unwrap();
    let target = host
        .run_control_binding(id, handle.run_id())
        .await
        .unwrap()
        .target;
    assert!(host.run_controls(&target).await.is_ok());
    let request = RunControlRequest {
        id: ControlId::new(),
        target: target.clone(),
        expected_revision: f.native.session_revision(id).await.unwrap(),
        action: RunControlAction::Cancel {
            resolver: ApprovalResolverClaim {
                resolver_id: "host".into(),
                authority: AuthorityRequirement::new("host-cancel", "1").unwrap(),
                authority_claim: "authorized host".into(),
            },
        },
    };
    // Keep the host's legitimate lease heartbeat out of the integrity window.
    // Worker queries still run normally and must be rejected by row visibility.
    let mut lease_guard = f.admin.begin().await.unwrap();
    sqlx::query("SELECT id FROM runs WHERE session_id=$1 FOR UPDATE")
        .bind(&id.0)
        .fetch_all(&mut *lease_guard)
        .await
        .unwrap();
    let before = f.snapshot(id).await;
    assert_eq!(
        f.worker.run_controls(&target).await.unwrap_err(),
        RunControlError::NotFound
    );
    let denied_control = f.worker.control_run(request.clone()).await;
    assert!(
        matches!(
            denied_control,
            Err(RuntimeError::RunControl(RunControlError::NotFound))
        ),
        "unexpected hidden control result: {denied_control:?}"
    );
    f.unchanged(id, before).await;
    lease_guard.rollback().await.unwrap();
    assert!(matches!(
        host.control_run(request).await.unwrap(),
        RunControlOutcome::Applied(_)
    ));
    gate.release.notify_one();
    handle.done().await.unwrap();
    host.close_extensions().await.unwrap();
    let admitted = legacy(&f.native, &f.workspace_b).await;
    let abandon = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::HostStoppedOwner,
    };
    let before = f.snapshot(&admitted.session.id).await;
    assert_eq!(
        f.worker.abandon(abandon.clone()).await.unwrap_err(),
        AbandonError::NotFound
    );
    f.unchanged(&admitted.session.id, before).await;
    f.host.abandon(abandon).await.unwrap();
    let own = legacy(&f.worker_native, &f.workspace_a).await;
    f.worker
        .abandon(AbandonRequest {
            expected: own.fence,
            expected_owner: own.run.owner,
            authority: AbandonAuthority::HostStoppedOwner,
        })
        .await
        .unwrap();
    super::positive::controls(&f).await;
    f.cleanup().await;
}
#[tokio::test]
async fn scoped_recovery_has_a_real_foreign_candidate() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let admitted = legacy(&f.native, &f.workspace_b).await;
    assert!(
        f.native
            .list_workspace_unfinished_runs(WorkspaceRunsRequest {
                workspace_id: f.workspace_b.clone()
            })
            .await
            .unwrap()
            .iter()
            .any(|run| run.id == admitted.run.id)
    );
    let before = f.snapshot(&admitted.session.id).await;
    for report in [
        f.worker.recover().await.unwrap(),
        f.worker.recover_workspace(&f.workspace_b).await.unwrap(),
    ] {
        assert!(report.skipped.is_empty() && report.recovered.is_empty());
    }
    f.unchanged(&admitted.session.id, before).await;
    let host_report = f.host.recover_workspace(&f.workspace_b).await.unwrap();
    super::recovery::assert_legacy_skip(&host_report, &admitted.run.id);
    assert!(
        f.native
            .list_workspace_unfinished_runs(WorkspaceRunsRequest {
                workspace_id: f.workspace_b.clone()
            })
            .await
            .unwrap()
            .iter()
            .any(|run| run.id == admitted.run.id)
    );
    f.cleanup().await;
}
