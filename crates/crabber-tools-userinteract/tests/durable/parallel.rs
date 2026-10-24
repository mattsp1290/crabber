use super::*;
#[tokio::test]
async fn already_started_parallel_questions_resume_together() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let mut batch = turn("user_interact", json!({"question":"First?"}));
    batch.pop();
    batch.extend(turn("user_interact", json!({"question":"Second?"})));
    let fixture = Arc::new(parallel::Together::new(tool(UserInteractPolicy::durable(
        authority(),
    ))));
    let definition = Arc::new(
        ToolDefinition::resumable(crabber_tools_userinteract::info(), fixture.clone()).unwrap(),
    );
    let agent = Agent::builder()
        .behavior_version("coordinated-durable-fixture")
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![batch, done()])))
        .config(config(root.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .shared_tool(definition)
        .build()
        .unwrap();
    let snapshot = paused(&agent, &store).await;
    assert_eq!(snapshot.interactions.len(), 2);
    let first = agent
        .answer_tool_interaction(answer(
            &snapshot,
            0,
            ToolInteractionReply::Answer {
                value: json!("one"),
            },
        ))
        .await
        .unwrap();
    let premature = ContinueTool {
        id: ControlId::new(),
        session_id: snapshot.session_id.clone(),
        run_id: snapshot.run_id.clone(),
        continuation: snapshot.continuation.clone(),
        expected_revision: first.revision,
    };
    assert!(agent.continue_tool(premature).await.is_err());
    let fresh = agent
        .tool_continuation_snapshot(&snapshot.session_id, &snapshot.run_id)
        .await
        .unwrap()
        .unwrap();
    let second = agent
        .answer_tool_interaction(answer(
            &fresh,
            1,
            ToolInteractionReply::Answer {
                value: json!("two"),
            },
        ))
        .await
        .unwrap();
    resume(&agent, &fresh, second.revision).await;
    let records = calls(&store, &snapshot.session_id).await;
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .all(|r| r.status == ToolCallStatus::Completed)
    );
    assert_eq!(fixture.starts.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(fixture.resumes.load(std::sync::atomic::Ordering::SeqCst), 2);
    agent.close_extensions().await.unwrap();
}

// A test-only barrier coordinates both starts before either suspension returns.
// The unwrapped-tool test below separately qualifies arbitrary runtime scheduling.
pub(super) struct Together {
    inner: Arc<ToolDefinition>,
    barrier: tokio::sync::Barrier,
    pub starts: std::sync::atomic::AtomicUsize,
    pub resumes: std::sync::atomic::AtomicUsize,
}
impl Together {
    pub fn new(inner: Arc<ToolDefinition>) -> Self {
        Self {
            inner,
            barrier: tokio::sync::Barrier::new(2),
            starts: 0.into(),
            resumes: 0.into(),
        }
    }
}
#[async_trait::async_trait]
impl crabber::ResumableTool for Together {
    fn identity(&self) -> crabber::core::ResumableToolIdentity {
        self.inner.executor.resumable_identity().unwrap()
    }
    async fn start(
        &self,
        ctx: crabber::extension::ToolContext,
        args: serde_json::Value,
    ) -> Result<crabber::core::ToolPhaseOutcome, crabber::ExtensionError> {
        self.starts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.barrier.wait().await;
        self.inner
            .executor
            .resumable()
            .unwrap()
            .start(ctx, args)
            .await
    }
    async fn resume(
        &self,
        ctx: crabber::extension::ToolContext,
        args: serde_json::Value,
        checkpoint: crabber::core::ToolCheckpoint,
        responses: Vec<crabber::core::AcceptedToolResponse>,
    ) -> Result<crabber::core::ToolPhaseOutcome, crabber::ExtensionError> {
        self.resumes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner
            .executor
            .resumable()
            .unwrap()
            .resume(ctx, args, checkpoint, responses)
            .await
    }
}

#[tokio::test]
async fn uncoordinated_parallel_questions_each_settle_once() {
    for max in [1, 2] {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::new());
        let mut batch = turn("user_interact", json!({"question":"First?"}));
        batch.pop();
        batch.extend(turn("user_interact", json!({"question":"Second?"})));
        let agent = Agent::builder()
            .behavior_version("uncoordinated-durable-fixture")
            .store(store.clone())
            .provider(Arc::new(FakeProvider::scripted(vec![batch, done()])))
            .config(config(root.path()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .execution_mode(ExecutionMode::Parallel { max })
            .shared_tool(tool(UserInteractPolicy::durable(authority())))
            .build()
            .unwrap();
        let mut snapshot = paused(&agent, &store).await;
        let session = snapshot.session_id.clone();
        let mut answered = std::collections::BTreeSet::new();
        let mut pauses = 0;
        loop {
            pauses += 1;
            assert!(!snapshot.interactions.is_empty());
            let mut revision = snapshot.revision;
            for (index, question) in snapshot.interactions.iter().enumerate() {
                assert!(
                    answered.insert(question.call_id.to_string()),
                    "question answered twice"
                );
                let mut request = answer(
                    &snapshot,
                    index,
                    ToolInteractionReply::Answer {
                        value: json!(question.description.prompt()),
                    },
                );
                request.expected_revision = revision;
                revision = agent
                    .answer_tool_interaction(request)
                    .await
                    .unwrap()
                    .revision;
            }
            let request = ContinueTool {
                id: ControlId::new(),
                session_id: snapshot.session_id.clone(),
                run_id: snapshot.run_id.clone(),
                continuation: snapshot.continuation.clone(),
                expected_revision: revision,
            };
            let ToolContinuation::Started { handle, .. } =
                agent.continue_tool(request.clone()).await.unwrap()
            else {
                panic!("fresh continuation")
            };
            let status = handle.done().await.unwrap().status;
            assert!(matches!(
                agent.continue_tool(request).await.unwrap(),
                ToolContinuation::Replayed(_)
            ));
            if status == RunStatus::Completed {
                break;
            }
            assert_eq!(status, RunStatus::Paused);
            assert!(answered.len() < 2);
            snapshot = agent
                .tool_continuation_snapshot(&snapshot.session_id, &snapshot.run_id)
                .await
                .unwrap()
                .unwrap();
        }
        if max == 1 {
            assert_eq!(pauses, 2);
        }
        assert_eq!(answered.len(), 2);
        let records = calls(&store, &session).await;
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .all(|r| r.status == ToolCallStatus::Completed)
        );
        agent.close_extensions().await.unwrap();
    }
}
