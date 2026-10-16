//! Independent job survives parent terminal and keeps exact session ownership.
use super::*;
use crabber::extension::{SessionInvocationAdmission, SessionInvocationRequest};
use crabber::session::session_owner::SessionOwnerState;
use tokio::{
    sync::Notify,
    time::{Duration, timeout},
};
const BOUND: Duration = Duration::from_secs(5);
#[derive(Default, Clone, Copy)]
enum Mode {
    #[default]
    Blocking,
    Cooperative,
    FailedMain,
    FailedResource,
    RetainedCleanup,
    IgnoreCancellation,
}
#[derive(Default)]
struct Fixture {
    release: Notify,
    entered: Notify,
    starts: AtomicUsize,
    request: Mutex<Option<SessionInvocationRequest>>,
    capability: Mutex<Option<crabber::extension::SessionInvocations>>,
    mode: Mode,
    resource_release: Arc<Notify>,
}
struct Tool(Arc<Fixture>);
#[async_trait]
impl JsonToolExecutor for Tool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("native context required")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let request = SessionInvocationRequest::new(
            "background.fixture".into(),
            InputFingerprint::new("a".repeat(64)).unwrap(),
        );
        *self.0.request.lock().unwrap() = Some(request.clone());
        *self.0.capability.lock().unwrap() = Some(context.session_invocations().clone());
        let fixture = self.0.clone();
        assert_eq!(
            context
                .session_invocations()
                .spawn(request.clone(), move |context| async move {
                    fixture.starts.fetch_add(1, Ordering::SeqCst);
                    if matches!(
                        fixture.mode,
                        Mode::FailedMain | Mode::FailedResource | Mode::RetainedCleanup
                    ) {
                        let release = fixture.resource_release.clone();
                        let failed = matches!(fixture.mode, Mode::FailedResource);
                        context
                            .cleanup
                            .spawn_checked(async move {
                                release.notified().await;
                                if failed {
                                    Err(crabber::extension::ResourceFailure::CleanupFailed)
                                } else {
                                    Ok(())
                                }
                            })
                            .unwrap();
                    }
                    fixture.entered.notify_one();
                    if matches!(fixture.mode, Mode::Cooperative) {
                        context.cancel.cancelled().await;
                    } else {
                        fixture.release.notified().await;
                        assert!(
                            matches!(fixture.mode, Mode::IgnoreCancellation)
                                || !context.cancel.is_cancelled()
                        );
                    }
                    if matches!(fixture.mode, Mode::FailedMain) {
                        Err(crabber::extension::ResourceFailure::CleanupFailed)
                    } else {
                        Ok(())
                    }
                })
                .await
                .unwrap(),
            SessionInvocationAdmission::Started
        );
        assert_eq!(
            context
                .session_invocations()
                .spawn(request, |_| async { panic!("replay must not dispatch") })
                .await
                .unwrap(),
            SessionInvocationAdmission::Replayed
        );
        Ok(json!({"accepted":true}))
    }
}
fn make_agent(store: Arc<dyn Store>, fixture: Arc<Fixture>, provider: FakeProvider) -> Agent {
    Agent::builder()
        .behavior_version("session-invocations.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "background".into(),
                description: "independent fixture".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Tool(fixture)),
        }))
        .build()
        .unwrap()
}
async fn journey(store: Arc<dyn Store>) {
    let fixture = Arc::new(Fixture::default());
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
    let agent = make_agent(store.clone(), fixture.clone(), provider.clone());
    let handle = agent.prompt(None, "background work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    timeout(BOUND, fixture.entered.notified()).await.unwrap();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    let request = fixture.request.lock().unwrap().clone().unwrap();
    let owner = store
        .get_session_owner(&session, &request.id)
        .await
        .unwrap();
    assert_eq!(owner.state, SessionOwnerState::Active);
    let cold = make_agent(
        store.clone(),
        Arc::new(Fixture::default()),
        FakeProvider::scripted(vec![]),
    );
    assert_eq!(
        cold.drain_session_invocations(&session, false, Duration::ZERO)
            .await,
        [] as [crabber::SessionInvocationReport; 0]
    );
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    cold.close_extensions().await.unwrap();
    assert_eq!(owner.binding.parent.as_ref().unwrap().run_id, run);
    assert_eq!(fixture.starts.load(Ordering::SeqCst), 1);
    let report = agent
        .drain_session_invocations(&session, false, Duration::from_millis(1))
        .await;
    assert_eq!(report.len(), 1);
    assert!(!report[0].local_joined);
    assert!(!report[0].resources.sealed);
    assert_eq!(
        agent
            .drain_session_invocations(&SessionId::new(), true, Duration::from_millis(1))
            .await,
        [] as [crabber::SessionInvocationReport; 0]
    );
    let capability = fixture.capability.lock().unwrap().clone().unwrap();
    assert_retired_parent(&capability, &request, &*store, &session).await;
    agent.close_extensions().await.unwrap();
    fixture.release.notify_one();
    let report = agent
        .drain_session_invocations(&session, false, BOUND)
        .await;
    let joined = report
        .iter()
        .find(|report| report.id == request.id)
        .unwrap();
    assert!(joined.local_joined);
    assert_eq!(joined.status, crabber::SessionInvocationStatus::Joined);
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(provider.requests().len(), 2);
}
#[tokio::test]
async fn memory_detached_job_outlives_parent() {
    journey(Arc::new(MemoryStore::new())).await;
}

async fn finish_parent(
    fixture: Arc<Fixture>,
) -> (Agent, Arc<MemoryStore>, SessionId, SessionInvocationRequest) {
    finish_parent_with_store(fixture, Arc::new(MemoryStore::new())).await
}
async fn finish_parent_with_store(
    fixture: Arc<Fixture>,
    store: Arc<MemoryStore>,
) -> (Agent, Arc<MemoryStore>, SessionId, SessionInvocationRequest) {
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
    let agent = make_agent(store.clone(), fixture.clone(), provider);
    let handle = agent.prompt(None, "independent job").await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    let request = fixture.request.lock().unwrap().clone().unwrap();
    (agent, store, session, request)
}
#[tokio::test]
async fn session_cancel_is_independent_and_joins_the_selected_job() {
    let fixture = Arc::new(Fixture {
        mode: Mode::Cooperative,
        ..Fixture::default()
    });
    let (agent, store, session, request) = finish_parent(fixture).await;
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let report = agent.drain_session_invocations(&session, true, BOUND).await;
    assert_eq!(report.len(), 1);
    assert!(report[0].local_joined);
    assert_eq!(report[0].status, crabber::SessionInvocationStatus::Joined);
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn failed_main_task_keeps_child_join_and_uncertain_durable_owner() {
    for failed_main in [true, false] {
        let fixture = Arc::new(Fixture {
            mode: if failed_main {
                Mode::FailedMain
            } else {
                Mode::FailedResource
            },
            ..Fixture::default()
        });
        let (agent, store, session, request) = finish_parent(fixture.clone()).await;
        fixture.release.notify_one();
        let report = agent
            .drain_session_invocations(&session, false, Duration::from_millis(10))
            .await;
        assert!(!report[0].local_joined);
        assert_eq!(report[0].resources.pending, 1);
        assert_ne!(
            store
                .get_session_owner(&session, &request.id)
                .await
                .unwrap()
                .state,
            SessionOwnerState::Joined
        );
        fixture.resource_release.notify_one();
        let report = agent
            .drain_session_invocations(&session, false, BOUND)
            .await;
        assert!(report[0].local_joined);
        assert_eq!(report[0].resources.pending, 0);
        assert_eq!(
            report[0].status,
            crabber::SessionInvocationStatus::Uncertain
        );
        assert_eq!(
            store
                .get_session_owner(&session, &request.id)
                .await
                .unwrap()
                .state,
            SessionOwnerState::Uncertain
        );
        agent.close_extensions().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn retained_child_cleanup_renews_its_independent_owner_without_public_revision() {
    use crabber::core::{Clock, ManualClock, SessionOwnerOperationId};
    use crabber::session::session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerError, SessionOwnerResolver,
    };
    let origin = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(origin));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fixture = Arc::new(Fixture {
        mode: Mode::RetainedCleanup,
        ..Fixture::default()
    });
    let (agent, store, session, request) = finish_parent_with_store(fixture.clone(), store).await;
    fixture.release.notify_one();
    let report = agent
        .drain_session_invocations(&session, false, Duration::from_millis(1))
        .await;
    assert_eq!(report[0].resources.pending, 1);
    let revision = store.session_revision(&session).await.unwrap();
    for seconds in (5..=60).step_by(5) {
        clock.set(origin + time::Duration::seconds(seconds));
        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(clock.now(), origin + time::Duration::seconds(60));
    let owner = store
        .get_session_owner(&session, &request.id)
        .await
        .unwrap();
    assert_eq!(owner.state, SessionOwnerState::Active);
    let mark = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: owner.target,
        command: SessionOwnerCommand::MarkUncertain {
            claim: SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "checking whether owner has expired".into(),
            },
        },
    };
    assert!(matches!(
        store.mutate_session_owner(mark, None).await,
        Err(SessionOwnerError::LiveOwner)
    ));
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    fixture.resource_release.notify_one();
    let report = agent
        .drain_session_invocations(&session, false, BOUND)
        .await;
    assert!(report[0].local_joined);
    assert_eq!(report[0].status, crabber::SessionInvocationStatus::Joined);
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn noncooperative_session_job_remains_owned_after_cancel_deadline() {
    let fixture = Arc::new(Fixture {
        mode: Mode::IgnoreCancellation,
        ..Fixture::default()
    });
    let (agent, store, session, request) = finish_parent(fixture.clone()).await;
    let report = agent
        .drain_session_invocations(&session, true, Duration::from_millis(1))
        .await;
    assert!(!report[0].local_joined);
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    fixture.release.notify_one();
    let report = agent
        .drain_session_invocations(&session, false, BOUND)
        .await;
    assert!(report[0].local_joined);
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    agent.close_extensions().await.unwrap();
}

async fn assert_retired_parent(
    capability: &crabber::extension::SessionInvocations,
    request: &SessionInvocationRequest,
    store: &dyn Store,
    session: &SessionId,
) {
    // Original metadata remains available after the parent ended; a fresh owner
    // through that old native capability must fail without invoking its factory.
    assert_eq!(
        capability
            .spawn(request.clone(), |_| async {
                panic!("closed-parent replay")
            })
            .await
            .unwrap(),
        SessionInvocationAdmission::Replayed
    );
    let fresh = SessionInvocationRequest::new(
        "background.fixture".into(),
        InputFingerprint::new("a".repeat(64)).unwrap(),
    );
    assert!(
        capability
            .spawn(fresh.clone(), |_| async { panic!("old parent dispatch") })
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_session_owner(session, &fresh.reserve_id)
            .await
            .unwrap()
            .is_none()
    );
    // Rejected completed supervisors must not consume the controller's finite
    // active capacity forever. This exceeds the complete local capacity.
    for _ in 0..1025 {
        let rejected = SessionInvocationRequest::new(
            "background.fixture".into(),
            InputFingerprint::new("a".repeat(64)).unwrap(),
        );
        assert_eq!(
            capability
                .spawn(rejected, |_| async { panic!("stale parent") })
                .await,
            Err(crabber::extension::SessionInvocationError::Rejected)
        );
    }
}
