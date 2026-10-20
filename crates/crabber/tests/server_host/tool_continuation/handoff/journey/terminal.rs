//! A terminal native phase without a suspended predecessor must retain its audit proof.
use super::*;
struct CompletesFirst(AtomicUsize);
#[async_trait]
impl ResumableTool for CompletesFirst {
    fn identity(&self) -> ResumableToolIdentity {
        OncePaused {
            starts: Arc::default(),
            resumes: Arc::default(),
        }
        .identity()
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        Ok(if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            ToolPhaseOutcome::Completed(
                ToolOutput::from_json(&json!("original terminal output")).unwrap(),
            )
        } else {
            pause(1)
        })
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        _: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        panic!("this fixture never resumes a suspended tool")
    }
}
pub(crate) async fn cancel_completed_with_probe<F: std::future::Future<Output = ()>>(
    store: Arc<dyn Store>,
    approval: bool,
    probe: impl FnOnce(crabber::session::RunControlRequest) -> F,
) {
    let provider = FakeProvider::scripted(vec![
        call(&format!("terminal-first-{}", ControlId::new())),
        call(&format!("terminal-second-{}", ControlId::new())),
        done(),
    ]);
    let tool = Arc::new(CompletesFirst(AtomicUsize::new(0)));
    let agent = worker_with_tool(
        store.clone(),
        provider.clone(),
        tool.clone(),
        if approval {
            PermissionDecision::Ask
        } else {
            PermissionDecision::Allow
        },
    );
    let handle = agent
        .prompt(None, "verify historical terminal output")
        .await
        .unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    if approval {
        approve(&agent, &session, &run, 1).await;
    }
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(tool.0.load(Ordering::SeqCst), if approval { 1 } else { 2 });
    let cancel = waiting_cancel(&agent.run_control_binding(&session, &run).await.unwrap());
    agent.close_extensions().await.unwrap();
    probe(cancel.clone()).await;
    assert!(matches!(
        agent.control_run(cancel.clone()).await.unwrap(),
        crabber::session::RunControlOutcome::Applied(_)
    ));
    assert!(matches!(
        agent.control_run(cancel).await.unwrap(),
        crabber::session::RunControlOutcome::Replayed(_)
    ));
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
}
