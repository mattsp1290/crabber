use super::*;
struct ScopedTool {
    request: Arc<Mutex<Option<SessionInvocationRequest>>>,
    release: Arc<Notify>,
    cleaned: Arc<AtomicUsize>,
}
#[async_trait]
impl JsonToolExecutor for ScopedTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("context required")
    }
    async fn execute_with_context(
        &self,
        context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let request = SessionInvocationRequest::new(
            "runtime.detached.scoped".into(),
            crabber_core::InputFingerprint::new("d".repeat(64)).unwrap(),
        );
        *self.request.lock().unwrap() = Some(request.clone());
        let release = self.release.clone();
        let cleaned = self.cleaned.clone();
        assert_eq!(
            context
                .session_invocations()
                .spawn(request.clone(), move |scope| async move {
                    scope
                        .cleanup
                        .spawn_checked(async move {
                            release.notified().await;
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .unwrap();
                    Ok(())
                })
                .await,
            Ok(SessionInvocationAdmission::Started)
        );
        assert_eq!(
            context
                .session_invocations()
                .spawn(request.clone(), |_| async {
                    panic!("metadata replay invoked factory")
                })
                .await,
            Ok(SessionInvocationAdmission::Replayed)
        );
        let mut changed = request;
        changed.join_id = crabber_core::SessionOwnerOperationId::new();
        assert_eq!(
            context
                .session_invocations()
                .spawn(changed, |_| async {
                    panic!("changed retry invoked factory")
                })
                .await,
            Err(SessionInvocationError::Rejected)
        );
        Ok(json!({"detached":true}))
    }
}

#[tokio::test]
async fn detached_registered_join_waits_for_exact_sealed_cleanup_after_parent_finishes() {
    let store = Arc::new(MemoryStore::new());
    let request_id = Arc::new(Mutex::new(None));
    let release = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        store.clone(),
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"detach"}"#),
            text_script("done"),
        ]),
        Arc::new(ScopedTool {
            request: request_id.clone(),
            release: release.clone(),
            cleaned: cleaned.clone(),
        }),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let request = request_id.lock().unwrap().clone().unwrap();
    let reports = runtime
        .drain_session_invocations(&session, false, Duration::from_millis(10))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(!reports[0].local_joined);
    assert_eq!(reports[0].resources.pending, 1);
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    release.notify_one();
    let reports = runtime
        .drain_session_invocations(&session, false, Duration::from_secs(2))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(reports[0].local_joined);
    assert!(reports[0].resources.sealed);
    assert_eq!(reports[0].resources.pending, 0);
    assert_eq!(
        reports[0].resources.failures,
        [] as [crabber_extension::ResourceFailure; 0]
    );
    assert_eq!(reports[0].status, crate::SessionInvocationStatus::Joined);
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .get_session_owner(&session, &request.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
}
