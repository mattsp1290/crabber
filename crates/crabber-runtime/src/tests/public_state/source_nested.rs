//! Native nested owners keep independent fences after the parent exchange closes.
use super::*;
use crabber_extension::{SessionInvocationAdmission as Admission, SessionInvocationRequest};
use crabber_session::session_owner::SessionOwnerState;
use serde::Deserialize;
#[derive(Default)]
struct Signals {
    after_run: Notify,
    after_parent: Notify,
    child_started: Notify,
    ids: Mutex<Vec<crabber_core::SessionOwnerId>>,
}
struct NestedTool(Arc<Signals>);
struct Release(Arc<Signals>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.after_run.notify_one();
        self.0.after_parent.notify_one();
    }
}
fn invocation() -> SessionInvocationRequest {
    SessionInvocationRequest::new(
        "runtime.public-source.nested".into(),
        crabber_core::InputFingerprint::new("c".repeat(64)).unwrap(),
    )
}
#[async_trait]
impl JsonToolExecutor for NestedTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("native context required")
    }
    async fn execute_with_context(
        &self,
        context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let parent = invocation();
        self.0.ids.lock().unwrap().push(parent.id.clone());
        let signals = self.0.clone();
        assert_eq!(
            context
                .session_invocations()
                .spawn(parent, move |parent| async move {
                    signals.after_run.notified().await;
                    let child = invocation();
                    signals.ids.lock().unwrap().push(child.id.clone());
                    let child_signals = signals.clone();
                    assert_eq!(
                        parent
                            .invocations
                            .spawn(child, move |child| async move {
                                child_signals.child_started.notify_one();
                                child_signals.after_parent.notified().await;
                                let state = child.public_source.snapshot().await.unwrap();
                                child
                                    .public_source
                                    .publish(
                                        crabber_core::ControlId::new(),
                                        state.revision,
                                        crabber_core::PublicSourceChange::StateSnapshot {
                                            state: json!({"late_child":null}),
                                        },
                                    )
                                    .await
                                    .unwrap();
                                Ok(())
                            })
                            .await,
                        Ok(Admission::Started)
                    );
                    Ok(())
                })
                .await,
            Ok(Admission::Started)
        );
        Ok(json!({"detached":true}))
    }
}
#[derive(Default)]
struct Capture(Mutex<Vec<EventRecord>>);
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.0.lock().unwrap().push(event.clone());
    }
}
async fn late_nested(store: Arc<dyn Store>) {
    let signals = Arc::new(Signals::default());
    let _release = Release(signals.clone());
    let capture = Arc::new(Capture::default());
    let call = ToolCallId::new();
    let runtime = Orchestrator::builder()
        .behavior_version("nested-public-source.v1")
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(call.clone(), r#"{"text":"nested"}"#),
            text_script("parent done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(NestedTool(signals.clone())))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let observed = capture.0.lock().unwrap().clone();
    signals.after_run.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        signals.child_started.notified(),
    )
    .await
    .unwrap();
    let ids = signals.ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store
                .get_session_owner(&session, &ids[0])
                .await
                .unwrap()
                .state
                == SessionOwnerState::Joined
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before = store.session_revision(&session).await.unwrap();
    signals.after_parent.notify_one();
    let reports = runtime
        .drain_session_invocations(&session, false, std::time::Duration::from_secs(2))
        .await;
    assert_eq!(reports.len(), 2);
    assert!(reports.iter().all(
        |report| report.local_joined && report.status == crate::SessionInvocationStatus::Joined
    ));
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert!(store.session_revision(&session).await.unwrap() > before);
    assert_eq!(
        capture.0.lock().unwrap().as_slice(),
        observed.as_slice(),
        "detached late work cannot append to completed parent observer"
    );
    let events = store.list_events(&session, None, 256).await.unwrap();
    assert_late_source(&events, &ids, call);
}
fn assert_late_source(
    events: &[EventRecord],
    ids: &[crabber_core::SessionOwnerId],
    call: ToolCallId,
) {
    let source = events
        .iter()
        .find(|event| event.kind == EventKind::PublicStateSnapshot)
        .unwrap();
    let source = crabber_core::PublicSourceEvent::deserialize(&source.payload).unwrap();
    let owner = source.source.invocation.unwrap();
    assert_eq!(owner.id.0, ids[1].0);
    assert_eq!(owner.parent_invocation.unwrap().0, ids[0].0);
    assert_eq!(
        owner.parent_generation,
        Some(crabber_core::SessionOwnerGeneration::INITIAL)
    );
    assert_eq!(owner.parent_call_id, Some(call));
    assert_eq!(source.contents.state(), &json!({"late_child":null}));
    let terminal = events
        .iter()
        .position(|event| event.kind == EventKind::RunSettled)
        .unwrap();
    let source_index = events
        .iter()
        .position(|event| event.kind == EventKind::PublicStateSnapshot)
        .unwrap();
    assert!(terminal < source_index);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::InvocationStarted)
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::InvocationFinished)
            .count(),
        2
    );
}
#[tokio::test]
async fn memory_nested_public_source_is_independent_after_both_parents_join() {
    late_nested(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_nested_public_source_is_independent_after_both_parents_join() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested.db");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    late_nested(Arc::new(
        crabber_session::SqliteStore::connect(path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_nested_public_source_is_independent_after_both_parents_join() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        late_nested(Arc::new(
            crabber_session::PostgresStore::connect(&url).await.unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}
