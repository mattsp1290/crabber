//! A Store panic cannot discard a genuinely active detached main's join.
use super::*;
#[derive(Default)]
struct Fixture {
    gate: Arc<SettlementGate>,
    request: Mutex<Option<SessionInvocationRequest>>,
    release_main: Notify,
    release_child: Notify,
    main_returned: AtomicUsize,
    child_returned: AtomicUsize,
    factories: AtomicUsize,
}
struct FaultTool(Arc<Fixture>);
#[async_trait]
impl JsonToolExecutor for FaultTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("context required")
    }
    async fn execute_with_context(
        &self,
        context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let request = SessionInvocationRequest::new(
            "runtime.detached.panic".into(),
            crabber_core::InputFingerprint::new("e".repeat(64)).unwrap(),
        );
        *self.0.request.lock().unwrap() = Some(request.clone());
        let fixture = self.0.clone();
        assert_eq!(
            context
                .session_invocations()
                .spawn(request, move |scope| async move {
                    fixture.factories.fetch_add(1, Ordering::SeqCst);
                    let child = fixture.clone();
                    scope
                        .cleanup
                        .spawn_checked(async move {
                            child.release_child.notified().await;
                            child.child_returned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .unwrap();
                    fixture
                        .gate
                        .invocation_poll_panic
                        .store(true, Ordering::SeqCst);
                    fixture.release_main.notified().await;
                    fixture.main_returned.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await,
            Ok(SessionInvocationAdmission::Started)
        );
        Ok(json!({"detached":true}))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn detached_store_panic_retains_original_main_and_resource_joins() {
    let memory = Arc::new(MemoryStore::new());
    let fixture = Arc::new(Fixture::default());
    fixture.gate.release.notify_one();
    let runtime = orchestrator(
        Arc::new(DelayedTerminalStore {
            inner: memory.clone(),
            gate: fixture.gate.clone(),
        }),
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"detach"}"#),
            text_script("done"),
        ]),
        Arc::new(FaultTool(fixture.clone())),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    tokio::time::timeout(
        Duration::from_secs(7),
        fixture.gate.invocation_poll_entered.notified(),
    )
    .await
    .unwrap();
    let blocked = runtime
        .drain_session_invocations(&session, true, Duration::from_millis(20))
        .await;
    assert_eq!(blocked.len(), 1);
    assert!(
        !blocked[0].local_joined,
        "actual main is still blocked after the Store panic"
    );
    assert_eq!(blocked[0].resources.pending, 1);
    assert_eq!(fixture.main_returned.load(Ordering::SeqCst), 0);
    fixture.release_main.notify_one();
    let child = runtime
        .drain_session_invocations(&session, false, Duration::from_millis(20))
        .await;
    assert!(!child[0].local_joined);
    assert_eq!(fixture.child_returned.load(Ordering::SeqCst), 0);
    fixture.release_child.notify_one();
    let joined = runtime
        .drain_session_invocations(&session, false, Duration::from_secs(2))
        .await;
    assert!(joined[0].local_joined);
    assert_eq!(joined[0].status, crate::SessionInvocationStatus::Joined);
    assert_eq!(fixture.main_returned.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.child_returned.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.factories.load(Ordering::SeqCst), 1);
    let request = fixture.request.lock().unwrap().clone().unwrap();
    assert_eq!(
        memory
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
}
