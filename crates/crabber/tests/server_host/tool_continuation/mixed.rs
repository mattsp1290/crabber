use super::*;
struct MixedInteractive {
    started: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
    together: Arc<tokio::sync::Barrier>,
}
#[async_trait]
impl ResumableTool for MixedInteractive {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "mixed-fixture".into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new("d".repeat(64)).unwrap(),
            checkpoint_schema_id: "interactive-fixture".into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.together.wait().await;
        Ok(pause(1))
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.resumed.fetch_add(1, Ordering::SeqCst);
        assert_eq!(responses.len(), 1);
        Ok(ToolPhaseOutcome::Completed(
            ToolOutput::from_json(&json!("interactive done")).unwrap(),
        ))
    }
}
struct Ordinary {
    effects: Arc<AtomicUsize>,
    together: Option<Arc<tokio::sync::Barrier>>,
}
#[async_trait]
impl JsonToolExecutor for Ordinary {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        if let Some(together) = &self.together {
            together.wait().await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok(json!("ordinary done"))
    }
}
fn named_call(id: &str, name: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::ToolCallStart {
            call_id: ToolCallId::from(id),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: ToolCallId::from(id),
            text: "{}".into(),
        },
        StreamDelta::ToolCallDone {
            call_id: ToolCallId::from(id),
        },
    ]
}
fn info(name: &str) -> ToolInfo {
    ToolInfo {
        name: name.into(),
        description: "mixed batch fixture".into(),
        parameters: json!({"type":"object"}),
        retry_safe: false,
        required_permissions: vec![],
    }
}
struct PoisonedSibling(MixedInteractive);
#[async_trait]
impl ResumableTool for PoisonedSibling {
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
        panic!("unjoined sibling must not resume")
    }
}
#[tokio::test]
async fn unjoined_sibling_prevents_checkpoint_handover_and_preserves_both_uncertain_phases() {
    let store = Arc::new(MemoryStore::new());
    let together = Arc::new(tokio::sync::Barrier::new(2));
    let starts = Arc::new(AtomicUsize::new(0));
    let resumes = Arc::new(AtomicUsize::new(0));
    let mut script = named_call("checkpoint-returned", "interactive");
    script.extend(named_call("checkpoint-unjoined", "unjoined"));
    script.push(StreamDelta::Completed);
    let provider = FakeProvider::scripted(vec![script]);
    let make_tool = || MixedInteractive {
        started: starts.clone(),
        resumed: resumes.clone(),
        together: together.clone(),
    };
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .behavior_version("mixed-uncertain.v1")
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
        .shared_tool(Arc::new(
            ToolDefinition::resumable(info("interactive"), Arc::new(make_tool())).unwrap(),
        ))
        .shared_tool(Arc::new(
            ToolDefinition::resumable(info("unjoined"), Arc::new(PoisonedSibling(make_tool())))
                .unwrap(),
        ))
        .build()
        .unwrap();
    let handle = agent.prompt(None, "mixed").await.unwrap();
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
        .unwrap()
        .phases;
    assert_eq!(phases.len(), 2);
    assert!(
        phases
            .iter()
            .all(|phase| phase.state == crabber::session::PhaseState::Uncertain)
    );
    assert!(
        phases
            .iter()
            .any(|phase| phase.quiescence == crabber::session::PhaseQuiescence::Incomplete)
    );
    let unfinished = store.list_unfinished_tool_calls(&run).await.unwrap();
    assert_eq!(unfinished.len(), 2);
    assert!(unfinished.iter().all(|call| call.result.is_none()));
    assert!(agent.prompt(Some(session), "fresh work").await.is_err());
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(resumes.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // A complete public lifecycle journey.
async fn parallel_pause_drains_started_sibling_and_preserves_unstarted_call() {
    let store = Arc::new(MemoryStore::new());
    let together = Arc::new(tokio::sync::Barrier::new(2));
    let started = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));
    let mut script = named_call("mixed-interactive", "interactive");
    script.extend(named_call("mixed-finished", "finished"));
    script.extend(named_call("mixed-pending", "pending"));
    script.push(StreamDelta::Completed);
    let provider = FakeProvider::scripted(vec![script, done()]);
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .behavior_version("mixed-continuation.v1")
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
        .shared_tool(Arc::new(
            ToolDefinition::resumable(
                info("interactive"),
                Arc::new(MixedInteractive {
                    started: started.clone(),
                    resumed: resumed.clone(),
                    together: together.clone(),
                }),
            )
            .unwrap(),
        ))
        .shared_tool(Arc::new(ToolDefinition {
            info: info("finished"),
            executor: Arc::new(Ordinary {
                effects: finished.clone(),
                together: Some(together),
            }),
        }))
        .shared_tool(Arc::new(ToolDefinition {
            info: info("pending"),
            executor: Arc::new(Ordinary {
                effects: pending.clone(),
                together: None,
            }),
        }))
        .build()
        .unwrap();
    let handle = agent.prompt(None, "mixed").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Paused
    );
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    assert_eq!(pending.load(Ordering::SeqCst), 0);
    let calls = store.list_unfinished_tool_calls(&run).await.unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().any(|call| call.id.0 == "mixed-pending"
        && call.status == crabber::core::ToolCallStatus::Pending));
    assert!(calls.iter().any(|call| call.id.0 == "mixed-interactive"
        && call.status == crabber::core::ToolCallStatus::Waiting));
    let snapshot = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let question = &snapshot.interactions[0];
    let accepted = agent
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
                authority_claim: "authenticated".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("accepted"),
            },
        })
        .await
        .unwrap();
    let ToolContinuation::Started { handle, .. } = agent
        .continue_tool(ContinueTool {
            id: ControlId::new(),
            expected_revision: accepted.revision,
            session_id: session.clone(),
            run_id: run,
            continuation: snapshot.continuation,
        })
        .await
        .unwrap()
    else {
        panic!("fresh")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    assert_eq!(pending.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(
        store
            .list_all_messages(&session)
            .await
            .unwrap()
            .iter()
            .filter(|message| message.role == crabber::core::Role::Tool)
            .count(),
        3
    );
}
