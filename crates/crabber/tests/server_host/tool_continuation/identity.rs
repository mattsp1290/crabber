use super::*;

struct ChangedIdentity(Arc<Interactive>);
#[async_trait]
impl ResumableTool for ChangedIdentity {
    fn identity(&self) -> ResumableToolIdentity {
        let mut identity = self.0.identity();
        identity.component_version = "2".into();
        identity
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        panic!("changed implementation must not execute")
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        _: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        panic!("changed implementation must not resume")
    }
}

#[tokio::test]
async fn changed_component_is_rejected_before_consuming_answers_or_claiming() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call("identity-binding"), done()]);
    let tool = Arc::new(Interactive {
        starts: Arc::default(),
        resumes: Arc::default(),
        effects: Arc::default(),
    });
    let original = make(store.clone(), provider.clone(), tool.clone());
    let handle = original.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let snapshot = original
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let question = &snapshot.interactions[0];
    let accepted = original
        .answer_tool_interaction(ToolInteractionAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: question.id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "verified-host".into(),
                authority: question.description.authority().clone(),
                authority_claim: "authenticated operator".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("accepted"),
            },
        })
        .await
        .unwrap();
    original.close_extensions().await.unwrap();
    let before = store.get_run(&run).await.unwrap();
    let changed = make(
        store.clone(),
        provider.clone(),
        Arc::new(ChangedIdentity(tool.clone())),
    );
    let result = changed
        .continue_tool(ContinueTool {
            id: ControlId::new(),
            expected_revision: accepted.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation,
        })
        .await;
    assert!(result.is_err(), "frozen implementation identity must match");
    assert_eq!(store.get_run(&run).await.unwrap(), before);
    let after = changed
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.revision, accepted.revision);
    assert!(after.interactions.iter().all(|question| !question.consumed));
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(tool.starts.load(Ordering::SeqCst), 1);
    assert_eq!(tool.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
}
