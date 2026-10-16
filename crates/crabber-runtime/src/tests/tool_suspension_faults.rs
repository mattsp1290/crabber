use super::*;
use crabber_core::{
    AuthorityRequirement, InputFingerprint, InteractionDescription, InteractionKind,
    ResumableToolIdentity, ToolCheckpoint, ToolPhaseOutcome, ToolSuspension,
};
use crabber_extension::{ResumableTool, ToolContext};
use crabber_session::{
    PhaseQuiescence, ToolContinuationError as Error, ToolContinuationSuspension,
    ToolSuspensionAuthority,
};
pub(super) async fn suspend(
    execution: &DelayedTerminalExecution,
    request: ToolContinuationSuspension,
    authority: ToolSuspensionAuthority,
) -> Result<crabber_session::ToolContinuationReceipt, Error> {
    let nth = execution
        .gate
        .tool_suspension_calls
        .fetch_add(1, Ordering::SeqCst);
    execution
        .gate
        .tool_suspension_keys
        .lock()
        .unwrap()
        .push(request.id.clone());
    match execution.gate.tool_suspension_fault.load(Ordering::SeqCst) {
        1 => return Err(Error::InvalidTransition),
        2 if nth == 0 => {
            execution
                .inner
                .mutate_public_state(crabber_session::PublicStateMutation {
                    id: crabber_core::ControlId::new(),
                    expected_revision: request.expected_revision,
                    contents: crabber_core::PublicStateContents::default(),
                })
                .await
                .unwrap();
            return Err(Error::RevisionConflict);
        }
        3 => return Err(Error::UnknownStoreOutcome),
        _ => (),
    }
    let mut receipt = execution
        .inner
        .suspend_tool_continuation(request, authority)
        .await?;
    if let Some(gate) = &execution.gate.observation {
        gate.acknowledge().await;
        if gate.lose_ack {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    match execution.gate.tool_suspension_fault.load(Ordering::SeqCst) {
        4 => return Err(Error::UnknownStoreOutcome),
        5 => receipt.semantic_digest = InputFingerprint::new("0".repeat(64)).unwrap(),
        _ => (),
    }
    Ok(receipt)
}
pub(super) struct Pausing(pub(super) Arc<AtomicUsize>);
#[async_trait]
impl ResumableTool for Pausing {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "fault-tool".into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
            checkpoint_schema_id: "fault".into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    async fn start(&self, _: ToolContext, _: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Suspended(
            ToolSuspension::new(
                ToolCheckpoint::new("fault", "1", json!({"effect":"done"})).unwrap(),
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
        panic!("metadata reconciliation cannot dispatch resume")
    }
}
#[tokio::test]
async fn suspension_distinguishes_definite_refusal_revision_retry_and_unknown_ack() {
    for fault in 1..=5 {
        let inner = Arc::new(MemoryStore::new());
        let gate = Arc::new(SettlementGate::default());
        gate.tool_suspension_fault.store(fault, Ordering::SeqCst);
        let effects = Arc::new(AtomicUsize::new(0));
        let ordinary = tool(Arc::new(EchoTool(Arc::default())));
        let definition =
            ToolDefinition::resumable(ordinary.info.clone(), Arc::new(Pausing(effects.clone())))
                .unwrap();
        let provider = FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
            text_script("must not run"),
        ]);
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
            .store(Arc::new(DelayedTerminalStore {
                inner: inner.clone(),
                gate: gate.clone(),
            }))
            .resolver(Arc::new(provider.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![Arc::new(definition)],
                Vec::new(),
            )))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        let result = handle.done().await;
        assert_eq!(effects.load(Ordering::SeqCst), 1, "fault {fault}");
        assert_eq!(provider.requests().len(), 1, "fault {fault}");
        let phase = inner
            .list_tool_phases(crabber_session::PhasePageRequest {
                session_id: session.clone(),
                run_id: run.clone(),
                expected_revision: None,
                after: None,
                limit: 16,
            })
            .await
            .unwrap()
            .phases
            .remove(0);
        match fault {
            1 => {
                assert!(result.is_err());
                assert_eq!(phase.state, crabber_session::PhaseState::Uncertain);
                assert_eq!(phase.quiescence, PhaseQuiescence::Complete);
            }
            2 | 4 => {
                assert_eq!(result.unwrap().status, RunStatus::Paused);
                assert_eq!(phase.state, crabber_session::PhaseState::Suspended);
            }
            3 | 5 => {
                assert!(matches!(
                    result,
                    Err(RuntimeError::ToolContinuationBoundaryUnknown { .. })
                ));
                assert_eq!(
                    phase.state,
                    if fault == 3 {
                        crabber_session::PhaseState::Started
                    } else {
                        crabber_session::PhaseState::Suspended
                    }
                );
            }
            _ => unreachable!(),
        }
        let keys = gate.tool_suspension_keys.lock().unwrap().clone();
        assert_eq!(keys.len(), if fault == 2 { 2 } else { 1 });
        if fault == 2 {
            assert_ne!(keys[0], keys[1]);
        }
        if fault == 3 {
            assert!(
                inner
                    .lookup_tool_continuation(&session, &keys[0])
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}
