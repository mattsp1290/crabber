use super::*;

struct FailedJoin(Arc<Interactive>);
#[async_trait]
impl ResumableTool for FailedJoin {
    fn identity(&self) -> ResumableToolIdentity {
        self.0.identity()
    }
    async fn start(
        &self,
        context: ToolContext,
        args: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        context
            .resources()
            .spawn(async { Err(crabber::extension::ResourceFailure::CleanupFailed) })
            .unwrap();
        self.0.start(context, args).await
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        _: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        panic!("failed cleanup must never grant a continuation")
    }
}

#[tokio::test]
async fn failed_resource_join_preserves_uncertainty_and_cannot_publish_ready_checkpoint() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call("failed-cleanup"), done()]);
    let tool = Arc::new(Interactive {
        starts: Arc::default(),
        resumes: Arc::default(),
        effects: Arc::default(),
    });
    let agent = make(
        store.clone(),
        provider.clone(),
        Arc::new(FailedJoin(tool.clone())),
    );
    let handle = agent.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), handle.done())
        .await
        .unwrap();
    assert!(
        agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .is_none()
    );
    let phases = store
        .list_tool_phases(crabber::session::PhasePageRequest {
            session_id: session.clone(),
            run_id: run.clone(),
            limit: 64,
            after: None,
            expected_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(phases.phases.len(), 1);
    assert_eq!(
        phases.phases[0].state,
        crabber::session::PhaseState::Uncertain
    );
    assert_eq!(
        phases.phases[0].quiescence,
        crabber::session::PhaseQuiescence::Incomplete
    );
    let calls = store.list_unfinished_tool_calls(&run).await.unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].result.is_none());
    assert!(agent.prompt(Some(session), "fresh work").await.is_err());
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(tool.starts.load(Ordering::SeqCst), 1);
    assert_eq!(tool.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
}
