use super::*;
use crabber_core::{
    AuthorityRequirement, FrontendResultStatus, FrontendToolResult, InputEnvelope,
    InteractionDescription, InteractionKind, ResumableToolIdentity, SessionRevision,
    ToolCheckpoint, ToolOutput, ToolPhaseOutcome, ToolSuspension,
};
use crabber_extension::{ResumableTool, ToolContext};
use crabber_session::{
    ApprovalResolverClaim, FrontendResolverClaim, ToolInteractionAnswer, ToolInteractionReply,
};
pub(super) struct Effects {
    pub(super) starts: AtomicUsize,
    pub(super) resumes: AtomicUsize,
}
struct Pausing(Arc<Effects>);
#[async_trait]
impl ResumableTool for Pausing {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "pending-tool".into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
            checkpoint_schema_id: "pending-tool".into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.0.starts.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Suspended(
            ToolSuspension::new(
                ToolCheckpoint::new("pending-tool", "1", json!({"effect":"started"})).unwrap(),
                vec![
                    InteractionDescription::new(
                        "choice",
                        InteractionKind::UserInput,
                        "Choose",
                        json!({"type":"string"}),
                        AuthorityRequirement::new("operator", "1").unwrap(),
                        None,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        ))
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        _: Vec<crabber_core::AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.0.resumes.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Completed(
            ToolOutput::from_json(&json!("resumed")).unwrap(),
        ))
    }
}
struct ProtectionCounter(Arc<SettlementGate>);
#[async_trait]
impl ToolPipeline for ProtectionCounter {
    async fn prepare(&self, _: &crabber_core::ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        _: &crabber_extension::ToolResultContext,
        _: &crabber_core::ToolInfo,
        result: ToolOutput,
    ) -> Result<crabber_extension::TransformOutput, String> {
        self.0
            .grant_payload_protections
            .fetch_add(1, Ordering::SeqCst);
        Ok(crabber_extension::TransformOutput::new(result))
    }
}
fn configured_runtime(
    store: Arc<dyn Store>,
    gate: Arc<SettlementGate>,
    provider: Arc<ScopedProvider>,
    fake: FakeProvider,
    clock: Arc<ManualClock>,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .clock(clock)
        .tool_pipeline(Arc::new(ProtectionCounter(gate.clone())))
        .store(Arc::new(DelayedTerminalStore { inner: store, gate }))
        .resolver(Arc::new(fake))
        .plan_provider(provider)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap()
}
pub(super) fn runtime(h: &Harness, clock: Arc<ManualClock>) -> Orchestrator {
    configured_runtime(
        h.memory.clone(),
        h.gate.clone(),
        h.provider.clone(),
        h.fake.clone(),
        clock,
    )
}
fn provider(frontend: bool, effects: Arc<Effects>) -> Arc<ScopedProvider> {
    let mut provider = ScopedProvider::new();
    if !frontend {
        let ordinary = tool(Arc::new(EchoTool(Arc::default())));
        Arc::get_mut(&mut provider).unwrap().base = StaticPlanProvider::new(
            vec![Arc::new(
                ToolDefinition::resumable(ordinary.info.clone(), Arc::new(Pausing(effects)))
                    .unwrap(),
            )],
            vec![],
        );
    }
    provider
}
fn input(frontend: bool) -> Request {
    let mut input = request();
    input.session_id = Some(SessionId::new());
    if frontend {
        input.text.clear();
        let mut envelope = serde_json::to_value(
            InputEnvelope::text("external", SessionRevision::INITIAL).unwrap(),
        )
        .unwrap();
        envelope["frontend_tools"] = json!([{"name":"echo", "description":"external", "input_parameters":{"type":"object"}, "result_contract":null}]);
        input.input_envelope = Some(serde_json::from_value(envelope).unwrap());
    }
    input
}
pub(super) async fn paused(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    unkeyed: bool,
) -> (Harness, Choice, Arc<Effects>) {
    paused_mode(store, clock, frontend, unkeyed, ResourceMode::Owned).await
}
pub(super) async fn paused_none(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
) -> (Harness, Choice, Arc<Effects>) {
    paused_mode(store, clock, frontend, false, ResourceMode::Legacy).await
}
#[derive(Clone, Copy)]
enum ResourceMode {
    Owned,
    Legacy,
}
async fn paused_mode(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    unkeyed: bool,
    mode: ResourceMode,
) -> (Harness, Choice, Arc<Effects>) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let effects = Arc::new(Effects {
        starts: AtomicUsize::new(0),
        resumes: AtomicUsize::new(0),
    });
    let provider = provider(frontend, effects.clone());
    provider
        .legacy_none_contract
        .store(matches!(mode, ResourceMode::Legacy), Ordering::SeqCst);
    let fake = FakeProvider::scripted(vec![
        call_script(
            ToolCallId::new(),
            if frontend { "{}" } else { r#"{"text":"ask"}"# },
        ),
        text_script("done"),
    ]);
    let runtime = configured_runtime(
        store.clone(),
        gate.clone(),
        provider.clone(),
        fake.clone(),
        clock,
    );
    let mut h = Harness {
        runtime,
        memory: store,
        provider,
        gate,
        fake,
        input: input(frontend),
    };
    let handle = if unkeyed {
        let mut input = h.input.clone();
        input.session_id = None;
        h.runtime.start(input).await.unwrap()
    } else {
        let crate::Admission::Started { handle, .. } = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .unwrap()
        else {
            panic!("fresh keyed admission")
        };
        handle
    };
    h.input.session_id = Some(handle.session_id().clone());
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let initial = h.runtime.retained_preparation_status(&session);
    match mode {
        ResourceMode::Owned => {
            assert_eq!(initial.len(), 1);
            h.finish_cleanup(&initial[0].preparation).await;
        }
        ResourceMode::Legacy => assert_eq!(initial, vec![]),
    }
    let snapshot = h
        .runtime
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let mut scope = crate::ContinueTool {
        id: crabber_core::ControlId::new(),
        session_id: session,
        run_id: run,
        continuation: snapshot.continuation.clone(),
        expected_revision: snapshot.revision,
    };
    let choice = selected_choice(&h, snapshot, &mut scope, frontend).await;
    assert_eq!(
        effects.starts.load(Ordering::SeqCst),
        usize::from(!frontend)
    );
    (h, choice, effects)
}

async fn selected_choice(
    h: &Harness,
    snapshot: crabber_session::ToolContinuationSnapshot,
    scope: &mut crate::ContinueTool,
    frontend: bool,
) -> Choice {
    if frontend {
        Choice::Frontend(crate::ContinueFrontend {
            continuation: scope.clone(),
            resolver: FrontendResolverClaim {
                resolver_id: "host".into(),
                authority_claim: "verified".into(),
            },
            results: vec![
                FrontendToolResult::new(
                    snapshot.frontend_calls[0].call_id.clone(),
                    FrontendResultStatus::Completed,
                    ToolOutput::from_json(&json!("external result")).unwrap(),
                )
                .unwrap(),
            ],
        })
    } else {
        let question = &snapshot.interactions[0];
        let answered = h
            .runtime
            .answer_tool_interaction(ToolInteractionAnswer {
                id: crabber_core::ControlId::new(),
                expected_revision: snapshot.revision,
                session_id: scope.session_id.clone(),
                run_id: scope.run_id.clone(),
                continuation: snapshot.continuation,
                interaction_id: question.id.clone(),
                resolver: ApprovalResolverClaim {
                    resolver_id: "host".into(),
                    authority: question.description.authority().clone(),
                    authority_claim: "verified".into(),
                },
                reply: ToolInteractionReply::Answer {
                    value: json!("yes"),
                },
            })
            .await
            .unwrap();
        scope.expected_revision = answered.revision;
        Choice::Tool(scope.clone())
    }
}
