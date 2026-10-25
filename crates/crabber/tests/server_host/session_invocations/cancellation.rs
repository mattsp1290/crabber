//! A cold controller requests; the actual retained supervisor signals and joins.
use super::*;
use crabber::core::SessionOwnerOperationId;
use crabber::session::session_owner::{
    RequestSessionOwnerCancel, SessionOwnerOutcome, SessionOwnerResolver,
};
const POLL_BOUND: Duration = Duration::from_secs(12);
async fn start(
    store: Arc<dyn Store>,
    fixture: Arc<Fixture>,
) -> (Agent, SessionId, SessionInvocationRequest) {
    let id = ToolCallId::new();
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: "background".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: id },
            StreamDelta::Completed,
        ],
        done(),
    ]);
    let agent = make_agent(store, fixture.clone(), provider);
    let handle = agent.prompt(None, "durable cancellation").await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    let request = fixture.request.lock().unwrap().clone().unwrap();
    (agent, session, request)
}
async fn journey(store: Arc<dyn Store>, cold: Arc<dyn Store>) {
    for mode in [
        Mode::Cooperative,
        Mode::RetainedCleanup,
        Mode::IgnoreCancellation,
    ] {
        let fixture = Arc::new(Fixture {
            mode,
            ..Fixture::default()
        });
        let (agent, session, invocation) = start(store.clone(), fixture.clone()).await;
        if matches!(mode, Mode::RetainedCleanup) {
            fixture.release.notify_one();
            let report = agent
                .drain_session_invocations(&session, false, Duration::from_millis(1))
                .await;
            assert_eq!(report[0].resources.pending, 1);
        }
        let controller = make_agent(
            cold.clone(),
            Arc::new(Fixture::default()),
            FakeProvider::scripted(vec![]),
        );
        // External control is metadata-only, including after local extension close.
        controller.close_extensions().await.unwrap();
        let owner = controller
            .session_owner_snapshot(&session, &invocation.id)
            .await
            .unwrap();
        let request = RequestSessionOwnerCancel {
            id: SessionOwnerOperationId::new(),
            session_id: session.clone(),
            target: owner.target,
            expected_revision: Some(cold.session_revision(&session).await.unwrap()),
            claim: SessionOwnerResolver {
                resolver_id: "cold controller".into(),
                authority_claim: "host-authorized stop".into(),
            },
        };
        let SessionOwnerOutcome::Applied {
            receipt,
            authority: None,
        } = controller
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap()
        else {
            panic!("fresh metadata intent")
        };
        assert!(
            receipt
                .owner
                .cancellation
                .as_ref()
                .unwrap()
                .observed
                .is_none()
        );
        let observed = wait_observed(&*cold, &session, &invocation).await;
        assert_eq!(fixture.starts.load(Ordering::SeqCst), 1);
        if !matches!(mode, Mode::Cooperative) {
            assert_eq!(observed.state, SessionOwnerState::Active);
            aggregate(&controller, &*cold, &session, Some(&observed)).await;
            let report = agent
                .drain_session_invocations(&session, false, Duration::from_millis(1))
                .await;
            assert!(!report[0].local_joined);
            if matches!(mode, Mode::RetainedCleanup) {
                assert_eq!(report[0].resources.pending, 1);
                fixture.resource_release.notify_one();
            } else {
                fixture.release.notify_one();
            }
        }
        let report = agent
            .drain_session_invocations(&session, false, BOUND)
            .await;
        assert!(report[0].local_joined);
        assert_eq!(report[0].status, crabber::SessionInvocationStatus::Joined);
        let joined = cold
            .get_session_owner(&session, &invocation.id)
            .await
            .unwrap();
        assert_eq!(joined.state, SessionOwnerState::Joined);
        assert!(joined.cancellation.unwrap().observed.is_some());
        aggregate(&controller, &*cold, &session, None).await;
        assert!(
            matches!(controller.request_session_owner_cancel(request).await.unwrap(),SessionOwnerOutcome::Replayed(saved) if saved==receipt)
        );
        agent.close_extensions().await.unwrap();
    }
}
async fn aggregate(
    controller: &Agent,
    store: &dyn Store,
    session: &SessionId,
    expected: Option<&crabber::session::session_owner::SessionOwnerSnapshot>,
) {
    let revision = store.session_revision(session).await.unwrap();
    let crabber::PublicSessionOutcomeV2::Page(page) = controller
        .public_session_snapshot_v2(crabber::PublicSessionRequestV2 {
            session_id: session.clone(),
            continuation: None,
            limits: crate::aggregate_limits(),
        })
        .await
        .unwrap()
    else {
        panic!("closed controller aggregate")
    };
    assert_eq!(page.captured_revision, revision);
    if let Some(expected) = expected {
        let owner = page
            .header
            .owners
            .iter()
            .find(|owner| owner.target == expected.target)
            .unwrap();
        assert_eq!(owner.state, SessionOwnerState::Active);
        assert_eq!(owner.cancellation, expected.cancellation);
        assert!(owner.cancellation.as_ref().unwrap().observed.is_some());
    } else {
        assert_eq!(
            page.header.owners,
            [] as [crabber::PublicRegisteredOwner; 0]
        );
    }
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
}
async fn wait_observed(
    cold: &dyn Store,
    session: &SessionId,
    invocation: &SessionInvocationRequest,
) -> crabber::session::session_owner::SessionOwnerSnapshot {
    timeout(POLL_BOUND, async {
        loop {
            let owner = cold
                .get_session_owner(session, &invocation.id)
                .await
                .unwrap();
            if owner.cancellation.as_ref().unwrap().observed.is_some() {
                break owner;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn memory_durable_owner_cancellation_supervisor() {
    let store = Arc::new(MemoryStore::new());
    Box::pin(journey(store.clone(), store)).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_durable_owner_cancellation_supervisor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let live = Arc::new(crabber::session::SqliteStore::connect(&path).await.unwrap());
    let cold = Arc::new(crabber::session::SqliteStore::connect(&path).await.unwrap());
    Box::pin(journey(live, cold)).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_durable_owner_cancellation_supervisor() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let live = Arc::new(
            crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap(),
        );
        let cold = Arc::new(
            crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap(),
        );
        Box::pin(journey(live, cold)).await;
    })
    .await
    .unwrap();
}
