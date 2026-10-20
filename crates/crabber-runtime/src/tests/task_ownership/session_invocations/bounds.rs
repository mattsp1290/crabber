//! Public drain bounds observe the same real native invocation and cleanup task.
use super::*;

#[derive(Default)]
struct Fixture {
    entered: Notify,
    cancelled: Notify,
    cleanup_release: Notify,
    cleaned: AtomicUsize,
}
struct Release(Arc<Fixture>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.cleanup_release.notify_one();
    }
}
struct OverflowTool {
    fixture: Arc<Fixture>,
    cleanup_fails: bool,
}
#[async_trait]
impl JsonToolExecutor for OverflowTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("context required")
    }
    async fn execute_with_context(
        &self,
        context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let fixture = self.fixture.clone();
        let cleanup_fails = self.cleanup_fails;
        let admission = context
            .session_invocations()
            .spawn(
                SessionInvocationRequest::new(
                    "runtime.detached.overflow".into(),
                    crabber_core::InputFingerprint::new("f".repeat(64)).unwrap(),
                ),
                move |scope| async move {
                    let child = fixture.clone();
                    scope
                        .cleanup
                        .spawn_checked(async move {
                            child.cleanup_release.notified().await;
                            child.cleaned.fetch_add(1, Ordering::SeqCst);
                            if cleanup_fails {
                                Err(crabber_extension::ResourceFailure::CleanupFailed)
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap();
                    fixture.entered.notify_one();
                    scope.cancel.cancelled().await;
                    fixture.cancelled.notify_one();
                    Ok(())
                },
            )
            .await;
        assert_eq!(admission, Ok(SessionInvocationAdmission::Started));
        Ok(json!({"detached":true}))
    }
}

#[tokio::test]
async fn invocation_drain_overflow_preserves_idle_pending_cancellation_and_failed_cleanup() {
    for cleanup_fails in [false, true] {
        let fixture = Arc::new(Fixture::default());
        let release = Release(fixture.clone());
        let runtime = orchestrator(
            Arc::new(MemoryStore::new()),
            FakeProvider::scripted(vec![
                call_script(ToolCallId::new(), r#"{"text":"detach"}"#),
                text_script("done"),
            ]),
            Arc::new(OverflowTool {
                fixture: fixture.clone(),
                cleanup_fails,
            }),
            Arc::new(RecordingObserver::default()),
        );
        assert_eq!(
            runtime
                .drain_session_invocations(&SessionId::new(), false, Duration::MAX)
                .await,
            []
        );
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        tokio::time::timeout(Duration::from_secs(2), fixture.entered.notified())
            .await
            .unwrap();
        // Dropping an in-flight observation leaves the original JoinHandle retained.
        let mut observation =
            Box::pin(runtime.drain_session_invocations(&session, false, Duration::from_secs(30)));
        tokio::select! {
            biased;
            _ = &mut observation => panic!("held original unexpectedly joined"),
            () = tokio::task::yield_now() => {}
        }
        drop(observation);
        let pending = runtime
            .drain_session_invocations(&session, false, Duration::MAX)
            .await;
        let cancelled = runtime
            .drain_session_invocations(&session, true, Duration::MAX)
            .await;
        tokio::time::timeout(Duration::from_secs(2), fixture.cancelled.notified())
            .await
            .unwrap();
        // Overflow signalled the original cancellation but observed no fictitious join.
        assert_eq!(pending.len(), 1);
        assert_eq!(cancelled.len(), 1);
        assert!(!pending[0].local_joined);
        assert!(!cancelled[0].local_joined);
        assert_eq!(pending[0].resources.pending, 1);
        assert_eq!(cancelled[0].resources.pending, 1);
        assert_eq!(fixture.cleaned.load(Ordering::SeqCst), 0);
        drop(release);
        let joined = runtime
            .drain_session_invocations(&session, false, Duration::from_secs(2))
            .await;
        assert_eq!(fixture.cleaned.load(Ordering::SeqCst), 1);
        assert!(joined[0].local_joined);
        assert_eq!(joined[0].resources.pending, 0);
        assert!(joined[0].resources.sealed);
        if cleanup_fails {
            assert_eq!(joined[0].status, crate::SessionInvocationStatus::Uncertain);
            assert_eq!(
                joined[0].resources.failures,
                [crabber_extension::ResourceFailure::CleanupFailed]
            );
        } else {
            assert_eq!(joined[0].status, crate::SessionInvocationStatus::Joined);
            assert_eq!(joined[0].resources.failures, []);
        }
        // A later overflow reports actual observed completion/failure, unchanged.
        assert_eq!(
            runtime
                .drain_session_invocations(&session, false, Duration::MAX)
                .await,
            joined
        );
    }
}
