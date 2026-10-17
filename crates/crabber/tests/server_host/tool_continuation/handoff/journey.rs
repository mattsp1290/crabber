//! Shared actual-worker fixture for public adapters and the source-compiled Memory facade.
use async_trait::async_trait;
use crabber::core::{
    AcceptedToolResponse, AuthorityRequirement, ControlId, InteractionDescription, InteractionKind,
    ResumableToolIdentity, RunStatus, SessionId, ToolCallId, ToolCheckpoint, ToolInfo, ToolOutput,
    ToolPhaseOutcome, ToolSuspension,
};
use crabber::extension::ToolContext;
use crabber::session::{
    ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval, Store,
    ToolInteractionAnswer, ToolInteractionReply,
};
use crabber::{
    Agent, AgentConfig, Continuation, ContinueApproval, ContinueTool, ExtensionError, FakeProvider,
    InputFingerprint, PermissionDecision, ResumableTool, Selection, StaticPolicy, StreamDelta,
    ToolContinuation, ToolDefinition,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct OncePaused {
    starts: Arc<AtomicUsize>,
    resumes: Arc<AtomicUsize>,
}
#[async_trait]
impl ResumableTool for OncePaused {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "interactive-fixture".into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
            checkpoint_schema_id: "interactive-fixture".into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(pause(1))
    }
    async fn resume(
        &self,
        _: ToolContext,
        _: Value,
        _: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        assert_eq!(responses.len(), 1);
        self.resumes.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Completed(
            ToolOutput::from_json(&json!("done")).unwrap(),
        ))
    }
}
pub(crate) async fn header(store: &dyn Store, session: &SessionId, run: &crabber::core::RunId) {
    let revision = store.session_revision(session).await.unwrap();
    let mut request = crabber::session::PublicHeaderRequest {
        session_id: session.clone(),
        run_id: Some(run.clone()),
        expected_revision: revision,
        max_bytes: 2 * 1024 * 1024,
        max_proof_bytes: 32 * 1024 * 1024,
        max_interactions: 64,
    };
    assert!(matches!(
        store.public_control_header(request.clone()).await.unwrap(),
        crabber::session::PublicHeaderOutcome::Header(_)
    ));
    request.max_proof_bytes = 1;
    assert!(matches!(
        store.public_control_header(request).await.unwrap(),
        crabber::session::PublicHeaderOutcome::Limited(
            crabber::session::PublicSessionLimit::ProofBytes
        )
    ));
}
pub(crate) async fn journey(store: Arc<dyn Store>) -> (SessionId, crabber::core::RunId) {
    let starts = Arc::new(AtomicUsize::new(0));
    let resumes = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![
        call(&format!("mixed-first-{}", ControlId::new())),
        call(&format!("mixed-second-{}", ControlId::new())),
        done(),
    ]);
    let make = || {
        worker(
            store.clone(),
            provider.clone(),
            starts.clone(),
            resumes.clone(),
        )
    };
    let mut agent = make();
    let handle = agent.prompt(None, "mixed work").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    for generation in 1..=2 {
        let request = approve(&agent, &session, &run, generation).await;
        header(store.as_ref(), &session, &run).await;
        assert_eq!(
            starts.load(Ordering::SeqCst),
            usize::try_from(generation).unwrap()
        );
        agent.close_extensions().await.unwrap();
        assert!(matches!(
            agent.continue_approval(request).await.unwrap(),
            Continuation::Replayed { .. }
        ));
        drop(agent);
        agent = make();
        let snapshot = agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.continuation.generation.get(), generation);
        assert_eq!(
            agent
                .approval_snapshot(&session, &run)
                .await
                .unwrap()
                .unwrap()
                .state,
            crabber::session::ApprovalContinuationState::Released
        );
        let request = resume(&agent, &session, &run, snapshot, generation).await;
        header(store.as_ref(), &session, &run).await;
        agent.close_extensions().await.unwrap();
        assert!(matches!(
            agent.continue_tool(request).await.unwrap(),
            ToolContinuation::Replayed { .. }
        ));
        drop(agent);
        agent = make();
    }
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(resumes.load(Ordering::SeqCst), 2);
    assert_eq!(provider.requests().len(), 3);
    assert_eq!(
        agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .state,
        crabber::session::ToolContinuationState::Released
    );
    (session, run)
}

async fn approve(
    agent: &Agent,
    session: &SessionId,
    run: &crabber::core::RunId,
    generation: u64,
) -> ContinueApproval {
    let snapshot = agent
        .approval_snapshot(session, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.continuation.generation.get(), generation);
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("approval")
    };
    let accepted = agent
        .answer_approval(ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "mixed-host".into(),
                authority: description.authority().clone(),
                authority_claim: "authenticated".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let request = ContinueApproval {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: accepted.revision,
    };
    let Continuation::Started { handle, .. } =
        agent.continue_approval(request.clone()).await.unwrap()
    else {
        panic!("fresh approval")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);

    request
}

async fn resume(
    agent: &Agent,
    session: &SessionId,
    run: &crabber::core::RunId,
    snapshot: crabber::session::ToolContinuationSnapshot,
    generation: u64,
) -> ContinueTool {
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
                resolver_id: "mixed-host".into(),
                authority: question.description.authority().clone(),
                authority_claim: "authenticated".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("accepted"),
            },
        })
        .await
        .unwrap();
    let request = ContinueTool {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: accepted.revision,
    };
    let ToolContinuation::Started { handle, .. } =
        agent.continue_tool(request.clone()).await.unwrap()
    else {
        panic!("fresh tool")
    };
    assert_eq!(
        handle.done().await.unwrap().status,
        if generation == 1 {
            RunStatus::Paused
        } else {
            RunStatus::Completed
        }
    );

    request
}

fn pause(step: u64) -> ToolPhaseOutcome {
    ToolPhaseOutcome::Suspended(
        ToolSuspension::new(
            ToolCheckpoint::new(
                "interactive-fixture",
                "1",
                json!({"step":step,"private":"never result"}),
            )
            .unwrap(),
            vec![
                InteractionDescription::new(
                    format!("question-{step}"),
                    InteractionKind::UserInput,
                    "Choose",
                    json!({"type":"string","enum":["accepted"]}),
                    AuthorityRequirement::new("operator", "1").unwrap(),
                    None,
                )
                .unwrap(),
            ],
        )
        .unwrap(),
    )
}

fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
fn call(id: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::ToolCallStart {
            call_id: ToolCallId::from(id),
            name: "identity".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: ToolCallId::from(id),
            text: "{\"directory\":\"forged\"}".into(),
        },
        StreamDelta::ToolCallDone {
            call_id: ToolCallId::from(id),
        },
        StreamDelta::Completed,
    ]
}

fn worker(
    store: Arc<dyn Store>,
    provider: FakeProvider,
    starts: Arc<AtomicUsize>,
    resumes: Arc<AtomicUsize>,
) -> Agent {
    worker_with_tool(
        store,
        provider,
        Arc::new(OncePaused { starts, resumes }),
        PermissionDecision::Ask,
    )
}
fn worker_with_tool(
    store: Arc<dyn Store>,
    provider: FakeProvider,
    tool: Arc<dyn ResumableTool>,
    permission: PermissionDecision,
) -> Agent {
    Agent::builder()
        .behavior_version("mixed-handoff.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .shared_tool(Arc::new(
            ToolDefinition::resumable(
                ToolInfo {
                    name: "identity".into(),
                    description: "mixed handoff".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                tool,
            )
            .unwrap(),
        ))
        .policy(Arc::new(StaticPolicy::new(permission)))
        .build()
        .unwrap()
}

pub(crate) async fn cancel_handoffs(store: Arc<dyn Store>) {
    cancel_handoff(store.clone(), false, |_| async {}).await;
    cancel_with_probe(store.clone(), |_| async {}).await;
    // Keep the public Memory baseline paired with the private terminal probe.
    cancel_completed_with_probe(store, true, |_| async {}).await;
}
pub(crate) async fn cancel_with_probe<F: std::future::Future<Output = ()>>(
    store: Arc<dyn Store>,
    probe: impl FnOnce(crabber::session::RunControlRequest) -> F,
) {
    cancel_handoff(store, true, probe).await;
}
async fn cancel_handoff<F: std::future::Future<Output = ()>>(
    store: Arc<dyn Store>,
    after_resume: bool,
    probe: impl FnOnce(crabber::session::RunControlRequest) -> F,
) {
    use crabber::session::{AttemptCleanup, AttemptState, RunControlOutcome};
    let starts = Arc::new(AtomicUsize::new(0));
    let resumes = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![
        call(&format!("cancel-first-{}", ControlId::new())),
        call(&format!("cancel-second-{}", ControlId::new())),
        done(),
    ]);
    let agent = worker(
        store.clone(),
        provider.clone(),
        starts.clone(),
        resumes.clone(),
    );
    let handle = agent.prompt(None, "cancel mixed waiting").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    approve(&agent, &session, &run, 1).await;
    if after_resume {
        let snapshot = agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        resume(&agent, &session, &run, snapshot, 1).await;
    }
    let parent = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(
        (parent.state, parent.cleanup),
        (AttemptState::Suspended, AttemptCleanup::Complete)
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
    let prior_events = store.list_events(&session, None, 1000).await.unwrap();
    let source_turn = prior_events
        .iter()
        .rev()
        .find(|e| e.kind == crabber::core::EventKind::RunPaused)
        .unwrap()
        .turn_id
        .clone();
    let completed_before = prior_events
        .iter()
        .filter(|e| e.kind == crabber::core::EventKind::TurnCompleted)
        .count();
    assert!(
        !prior_events
            .iter()
            .any(|e| e.kind == crabber::core::EventKind::TurnCompleted && e.turn_id == source_turn)
    );
    let binding = agent.run_control_binding(&session, &run).await.unwrap();
    let cancel = waiting_cancel(&binding);
    agent.close_extensions().await.unwrap();
    probe(cancel.clone()).await;
    assert!(matches!(
        agent
            .control_run(cancel.clone())
            .await
            .unwrap_or_else(|error| panic!("after_resume={after_resume}: {error:?}")),
        RunControlOutcome::Applied(_)
    ));
    assert!(matches!(
        agent.control_run(cancel.clone()).await.unwrap(),
        RunControlOutcome::Replayed(_)
    ));
    assert_cancelled(
        store.as_ref(),
        &agent,
        &binding.target,
        &CancelEvidence {
            parent,
            phases,
            source_turn,
            completed_before,
        },
    )
    .await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(resumes.load(Ordering::SeqCst), usize::from(after_resume));
    assert_eq!(provider.requests().len(), if after_resume { 2 } else { 1 });
}

struct CancelEvidence {
    parent: crabber::session::AttemptSnapshot,
    phases: Vec<crabber::session::PhaseSnapshot>,
    source_turn: Option<crabber::core::TurnId>,
    completed_before: usize,
}
async fn assert_cancelled(
    store: &dyn Store,
    agent: &Agent,
    target: &crabber::session::RunControlTarget,
    evidence: &CancelEvidence,
) {
    use crabber::session::RunControlStatus;
    let (session, run) = (&target.session_id, &target.run_id);
    assert_eq!(
        store.get_run(run).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    assert_eq!(
        store
            .latest_execution_attempt(run)
            .await
            .unwrap()
            .unwrap()
            .attempt,
        evidence.parent
    );
    assert_eq!(
        store
            .list_tool_phases(crabber::session::PhasePageRequest {
                session_id: session.clone(),
                run_id: run.clone(),
                limit: 64,
                after: None,
                expected_revision: None
            })
            .await
            .unwrap()
            .phases,
        evidence.phases
    );
    assert_eq!(
        agent.run_controls(target).await.unwrap().controls[0].status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    );
    assert_eq!(
        agent
            .approval_snapshot(session, run)
            .await
            .unwrap()
            .unwrap()
            .state,
        crabber::session::ApprovalContinuationState::Released
    );
    assert_eq!(
        agent
            .tool_continuation_snapshot(session, run)
            .await
            .unwrap()
            .unwrap()
            .state,
        crabber::session::ToolContinuationState::Released
    );
    header(store, session, run).await;
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == crabber::core::EventKind::RunSettled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == crabber::core::EventKind::TurnCompleted)
            .count(),
        evidence.completed_before + 1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == crabber::core::EventKind::TurnCompleted
                && e.turn_id == evidence.source_turn)
            .count(),
        1
    );
}

fn waiting_cancel(
    binding: &crabber::session::RunControlBinding,
) -> crabber::session::RunControlRequest {
    use crabber::session::{RunControlAction, RunControlRequest};
    RunControlRequest {
        id: ControlId::new(),
        target: binding.target.clone(),
        expected_revision: binding.revision,
        action: RunControlAction::Cancel {
            resolver: ApprovalResolverClaim {
                resolver_id: "mixed-controller".into(),
                authority: AuthorityRequirement::new("host-cancel", "1").unwrap(),
                authority_claim: "authorized".into(),
            },
        },
    }
}

#[path = "journey/terminal.rs"]
mod terminal;
pub(crate) use terminal::cancel_completed_with_probe;
