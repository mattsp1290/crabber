//! Own-workspace controls for each API denied by the foreign-workspace tests.
use super::{fixture::*, provider::Gate};
use crabber::{core::*, session::*, *};
use std::{sync::Arc, time::Duration};

pub(super) async fn reads(f: &Fixture) {
    let a = f.create(true).await;
    let id = a.session().session_id();
    f.worker
        .prompt_in(&a, "own", None)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let Admission::Started { handle, .. } = f
        .worker
        .prompt_keyed_in(&a, "keyed", options(), None)
        .await
        .unwrap()
    else {
        panic!("fresh own admission")
    };
    handle.done().await.unwrap();
    assert!(f.worker.session_summary(id).await.is_ok());
    assert!(
        f.worker
            .lookup_admission(id, &options().key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        f.worker
            .recover_admission(id.clone(), "keyed", options())
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert_eq!(
        f.worker
            .list_sessions(ListSessions {
                workspace_id: f.workspace_a.clone(),
                include_archived: false,
                limit: 100,
                cursor: None
            })
            .await
            .unwrap()
            .sessions
            .len(),
        1
    );
    assert!(matches!(
        f.worker
            .public_session_snapshot_v2(public_request(id))
            .await
            .unwrap(),
        PublicSessionOutcomeV2::Page(_)
    ));
    assert_eq!(f.worker_provider.requests().len(), 2);
}
pub(super) async fn queue(f: &Fixture) {
    let a = f.create(true).await;
    let id = a.session().session_id();
    let queue = QueueId::new();
    let input = InputEnvelope::text(
        "own queue",
        f.worker_native.session_revision(id).await.unwrap(),
    )
    .unwrap();
    f.worker
        .enqueue_session_input_in(
            a.session(),
            input,
            queue.clone(),
            ControlId::new(),
            options(),
        )
        .await
        .unwrap();
    assert_eq!(
        f.worker
            .list_queue_heads(QueueHeadsRequest {
                workspace_id: f.workspace_a.clone(),
                limit: 1,
                cursor: None
            })
            .await
            .unwrap()
            .heads
            .len(),
        1
    );
    let QueueOutcome::Applied {
        authority: Some(authority),
        receipt,
        ..
    } = f
        .worker
        .mutate_queue(QueueMutation {
            session_id: id.clone(),
            id: ControlId::new(),
            expected_revision: f.worker_native.session_revision(id).await.unwrap(),
            action: QueueAction::Claim {
                owner: "worker".into(),
                lease: Duration::from_secs(30),
            },
        })
        .await
        .unwrap()
    else {
        panic!("own claim")
    };
    let prepare = QueueControl {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        authority,
    };
    let prepared = f
        .worker
        .prepare_queue_in(a.session(), prepare.clone())
        .await
        .unwrap();
    let admission_id = ControlId::new();
    let Admission::Started { handle, .. } = f
        .worker
        .dispatch_queue_in(a.session(), prepare.clone(), admission_id.clone(), None)
        .await
        .unwrap()
    else {
        panic!("own dispatch")
    };
    handle.done().await.unwrap();
    assert!(matches!(
        f.worker
            .admit_queue_in(
                a.session(),
                QueueControl {
                    id: admission_id,
                    expected_revision: prepared.revision,
                    authority: prepare.authority
                },
                None
            )
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert!(matches!(
        f.worker
            .recover_queue_in(a.session(), &queue, None)
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
}
pub(super) async fn controls(f: &Fixture) {
    let a = f.create(true).await;
    let id = a.session().session_id();
    let gate = Gate::new(1);
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::now_utc()));
    let store = Arc::new(f.worker_native.as_ref().clone().with_clock(clock.clone()));
    let mut config = AgentConfig::new(selection());
    config.workspace_id = f.workspace_a.clone();
    config.directory = "/roles".into();
    let own = Agent::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(store)
        .provider(Arc::new(gate.clone()))
        .config(config)
        .build()
        .unwrap();
    let handle = own.prompt_in(&a, "own control", None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .unwrap();
    let target = f
        .worker
        .run_control_binding(id, handle.run_id())
        .await
        .unwrap()
        .target;
    assert_eq!(
        f.worker.run_controls(&target).await.unwrap().controls,
        vec![]
    );
    assert!(matches!(
        f.worker
            .control_run(RunControlRequest {
                id: ControlId::new(),
                target,
                expected_revision: f.worker_native.session_revision(id).await.unwrap(),
                action: RunControlAction::Cancel {
                    resolver: ApprovalResolverClaim {
                        resolver_id: "worker".into(),
                        authority: AuthorityRequirement::new("cancel", "1").unwrap(),
                        authority_claim: "authorized own workspace".into()
                    }
                }
            })
            .await
            .unwrap(),
        RunControlOutcome::Applied(_)
    ));
    gate.release.notify_one();
    handle.done().await.unwrap();
    own.close_extensions().await.unwrap();
}
pub(super) async fn fork(f: &Fixture, session: &SessionId) {
    let summary = f.worker.session_summary(session).await.unwrap();
    let request = ForkSession {
        id: ControlId::new(),
        source_session_id: session.clone(),
        workspace_id: summary.workspace_id,
        directory: summary.directory,
        expected_source_revision: summary.revision,
        boundary: ForkBoundary::Empty,
        target_session_id: SessionId::new(),
        title: "own fork".into(),
        defaults: summary.defaults,
        limits: public_request(session).limits.history,
        max_proof_bytes: 32 << 20,
    };
    assert!(matches!(
        f.worker.fork_session(request).await.unwrap(),
        ForkOutcome::Applied(_)
    ));
}
