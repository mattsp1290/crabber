//! Test-only crash/reconciliation fixtures. Worker authorities never cross a
//! process boundary; the writer acquires its own fresh native ownership.
#[path = "process/child.rs"]
pub(crate) mod child;
#[path = "process/controls.rs"]
mod controls;
#[path = "process/verify.rs"]
pub(crate) mod verify;
use super::{applied, mutation, output, resolver};
use crate::{
    AttemptCommand, AttemptMutation, AttemptMutationOutcome, PhaseCommand, PhaseError,
    PhaseMutation, PhaseMutationOutcome, PhaseState, PhaseTarget, PhaseWriteAuthority, Store,
    UncertainPhaseChoice,
};
use crabber_core::{
    AttemptGeneration, AttemptId, ControlId, EventKind, EventRecord, PhaseGeneration, PhaseId,
    RunFence, SessionId, SessionRevision, ToolCallId, ToolCallRecord, ToolCallStatus,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum Mode {
    Claim,
    Begin,
    Start,
    Success,
    Failure,
    Refuse,
    Settle,
    Uncertainty,
    Cleanup,
}
impl Mode {
    pub(crate) const ALL: [Self; 9] = [
        Self::Claim,
        Self::Begin,
        Self::Start,
        Self::Success,
        Self::Failure,
        Self::Refuse,
        Self::Settle,
        Self::Uncertainty,
        Self::Cleanup,
    ];
    pub(crate) fn operation(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Begin | Self::Start => "begin",
            Self::Success => "resolve_success",
            Self::Failure => "resolve_failure",
            Self::Refuse => "resolve_refuse",
            Self::Settle => "complete",
            Self::Uncertainty => "mark_stopped_uncertain",
            Self::Cleanup => "quiescence_complete",
        }
    }
    fn preoperations(self) -> usize {
        match self {
            Self::Claim => 2,
            Self::Begin | Self::Start => 3,
            Self::Settle | Self::Uncertainty => 4,
            Self::Cleanup | Self::Success | Self::Failure | Self::Refuse => 5,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Fixture {
    pub initial_run: crabber_core::Run,
    pub fence: RunFence,
    pub session: SessionId,
    pub base_revision: SessionRevision,
    pub request: PhaseMutation,
    pub mode: Mode,
}
pub(crate) async fn pending(store: &impl Store, mode: Mode) -> Fixture {
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::now_utc()).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let call = ToolCallId::new();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call.clone(),
                run_id: admitted.run.id.clone(),
                name: "process-owned".into(),
                arguments: serde_json::json!({}),
                status: ToolCallStatus::Pending,
                retry_safe: true,
                result: None,
            },
            EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: serde_json::json!({}),
                correlation: None,
                live_only: false,
                created_at: admitted.run.created_at,
            },
        )
        .await
        .unwrap();
    let target = PhaseTarget {
        id: PhaseId::new(),
        generation: PhaseGeneration::INITIAL,
        call_id: call,
        attempt: crate::AttemptTarget {
            id: AttemptId::new(),
            generation: AttemptGeneration::INITIAL,
        },
    };
    let base_revision = store.session_revision(&admitted.session.id).await.unwrap();
    let mut revision = base_revision;
    for _ in 0..mode.preoperations() {
        revision = revision.next().unwrap();
    }
    let command = match mode {
        Mode::Claim => PhaseCommand::Claim { target },
        Mode::Begin | Mode::Start => PhaseCommand::Begin { target },
        Mode::Uncertainty => PhaseCommand::MarkStoppedUncertain {
            target,
            claim: resolver(),
        },
        Mode::Cleanup => PhaseCommand::RecordQuiescence {
            target,
            result: crate::PhaseQuiescenceResult::Complete,
            host_claim: Some(resolver()),
        },
        Mode::Settle => PhaseCommand::Complete {
            target,
            output: output(),
        },
        _ => PhaseCommand::ResolveUncertain {
            target,
            claim: resolver(),
            choice: match mode {
                Mode::Success => UncertainPhaseChoice::Success { output: output() },
                Mode::Failure => UncertainPhaseChoice::Failure { output: None },
                _ => UncertainPhaseChoice::Refuse,
            },
        },
    };
    Fixture {
        initial_run: admitted.run,
        fence: admitted.fence,
        session: admitted.session.id,
        base_revision,
        request: mutation(revision, command),
        mode,
    }
}
pub(crate) async fn write(
    store: &impl Store,
    fixture: &Fixture,
    inject: impl Fn(u8),
    lose_ack: bool,
) -> Result<PhaseMutationOutcome, PhaseError> {
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    let authority = prepare(execution.as_ref(), fixture).await;
    if lose_ack {
        inject(2);
    }
    let result = if matches!(fixture.mode, Mode::Uncertainty | Mode::Cleanup) {
        controls::write(store, fixture).await.map(controls::outcome)
    } else if matches!(fixture.mode, Mode::Start) {
        let Some(PhaseWriteAuthority::Phase(worker)) = authority else {
            panic!("phase worker missing");
        };
        execution
            .start_tool_phase(start(fixture), Some(worker))
            .await
    } else if matches!(fixture.mode, Mode::Settle) {
        let Some(PhaseWriteAuthority::Phase(worker)) = authority else {
            panic!("phase worker missing");
        };
        execution
            .settle_tool_phase(settlement(fixture), Some(worker))
            .await
    } else if matches!(fixture.mode, Mode::Success | Mode::Failure | Mode::Refuse) {
        store
            .resolve_uncertain_phase(resolution(fixture))
            .await
            .map(|outcome| match outcome {
                crate::PhaseResolutionOutcome::Applied(receipt) => PhaseMutationOutcome::Applied {
                    receipt,
                    authority: None,
                },
                crate::PhaseResolutionOutcome::Replayed(receipt) => {
                    PhaseMutationOutcome::Replayed(receipt)
                }
            })
    } else {
        execution
            .mutate_phase(fixture.request.clone(), authority)
            .await
    };
    inject(0);
    result
}
async fn prepare(
    execution: &dyn crate::ExecutionStore,
    fixture: &Fixture,
) -> Option<PhaseWriteAuthority> {
    let target = fixture.request.command.target();
    let reserve = AttemptMutation {
        id: ControlId::new(),
        expected_revision: fixture.base_revision,
        command: AttemptCommand::Reserve {
            target: target.attempt.clone(),
        },
    };
    let AttemptMutationOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = execution.mutate_attempt(reserve, None).await.unwrap()
    else {
        panic!("fresh attempt authority missing");
    };
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: target.attempt.clone(),
        },
    };
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(begin, Some(authority.clone()))
        .await
        .unwrap()
    else {
        panic!("fresh attempt begin missing");
    };
    if matches!(fixture.mode, Mode::Claim) {
        return Some(PhaseWriteAuthority::Attempt(authority));
    }
    let (claimed, worker) = applied(
        execution
            .mutate_phase(
                mutation(
                    receipt.revision,
                    PhaseCommand::Claim {
                        target: target.clone(),
                    },
                ),
                Some(PhaseWriteAuthority::Attempt(authority)),
            )
            .await
            .unwrap(),
    );
    if matches!(fixture.mode, Mode::Begin | Mode::Start) {
        return Some(PhaseWriteAuthority::Phase(worker.unwrap()));
    }
    if matches!(fixture.mode, Mode::Settle) {
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: target.clone(),
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker.clone().unwrap())),
            )
            .await
            .unwrap();
        return Some(PhaseWriteAuthority::Phase(worker.unwrap()));
    }
    let (started, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: target.clone(),
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker.unwrap())),
            )
            .await
            .unwrap(),
    );
    if matches!(fixture.mode, Mode::Uncertainty) {
        return None;
    }
    execution
        .mutate_phase(
            mutation(
                started.revision,
                PhaseCommand::MarkStoppedUncertain {
                    target: target.clone(),
                    claim: resolver(),
                },
            ),
            None,
        )
        .await
        .unwrap();
    None
}
pub(crate) async fn uncommitted(store: &impl Store, fixture: &Fixture) {
    assert_eq!(
        store.get_run(&fixture.fence.run_id).await.unwrap(),
        Some(fixture.initial_run.clone())
    );
    let attempt = store
        .latest_execution_attempt(&fixture.fence.run_id)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(attempt.state, crate::AttemptState::Started);
    assert_eq!(attempt.cleanup, crate::AttemptCleanup::Pending);
    assert_eq!(
        store
            .lookup_phase_mutation(&fixture.session, &fixture.request.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store.session_revision(&fixture.session).await.unwrap(),
        fixture.request.expected_revision
    );
    let calls = store
        .list_unfinished_tool_calls(&fixture.fence.run_id)
        .await
        .unwrap();
    let call = calls
        .iter()
        .find(|c| c.id == fixture.request.command.target().call_id)
        .unwrap();
    assert_eq!(
        call.status,
        if matches!(
            fixture.mode,
            Mode::Settle
                | Mode::Success
                | Mode::Failure
                | Mode::Refuse
                | Mode::Uncertainty
                | Mode::Cleanup
        ) {
            ToolCallStatus::Running
        } else {
            ToolCallStatus::Pending
        }
    );
    assert!(call.result.is_none());
    assert_eq!(
        store
            .list_messages(&fixture.session, None)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap()
            .len(),
        fixture.mode.preoperations() + 1
    );
    let snapshot = store
        .tool_phase(&fixture.fence.run_id, &fixture.request.command.target().id)
        .await;
    match fixture.mode {
        Mode::Claim => assert_eq!(snapshot.unwrap_err(), PhaseError::NotFound),
        Mode::Begin | Mode::Start => assert_eq!(snapshot.unwrap().phase.state, PhaseState::Claimed),
        Mode::Settle | Mode::Uncertainty => {
            assert_eq!(snapshot.unwrap().phase.state, PhaseState::Started);
        }
        _ => assert_eq!(snapshot.unwrap().phase.state, PhaseState::Uncertain),
    }
}
pub(crate) async fn recover_killed_writer(store: &impl Store, fixture: &Fixture) {
    if matches!(fixture.mode, Mode::Uncertainty | Mode::Cleanup) {
        controls::recover(store, fixture).await;
        return;
    }
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    if matches!(
        fixture.mode,
        Mode::Claim | Mode::Begin | Mode::Start | Mode::Settle
    ) {
        // A reaped worker's opaque authority is gone. The same outbox must not
        // turn a retry into dispatch permission, even when BEGIN rolled back.
        let failure = if matches!(fixture.mode, Mode::Start) {
            execution.start_tool_phase(start(fixture), None).await
        } else if matches!(fixture.mode, Mode::Settle) {
            execution.settle_tool_phase(settlement(fixture), None).await
        } else {
            execution.mutate_phase(fixture.request.clone(), None).await
        };
        assert_eq!(failure.unwrap_err(), PhaseError::StaleOwner);
        if matches!(fixture.mode, Mode::Begin | Mode::Start | Mode::Settle) {
            let command = PhaseCommand::MarkStoppedUncertain {
                target: fixture.request.command.target().clone(),
                claim: crate::PhaseResolverClaim {
                    resolver_id: "process-controller".into(),
                    authority_claim: "controller reaped the phase-owner process".into(),
                },
            };
            let (uncertain, _) = applied(
                execution
                    .mutate_phase(mutation(fixture.request.expected_revision, command), None)
                    .await
                    .unwrap(),
            );
            assert_eq!(uncertain.phase.state, PhaseState::Uncertain);
            assert_eq!(uncertain.phase.quiescence, crate::PhaseQuiescence::Pending);
        }
    } else {
        // Only the original explicit Store control is retried; no worker
        // authority is recovered or needed to select this host outcome.
        let receipt = super::resolution::fresh(
            store
                .resolve_uncertain_phase(resolution(fixture))
                .await
                .unwrap(),
        );
        assert_eq!(
            receipt.revision,
            super::resolution::result_revision(&resolution(fixture))
        );
        assert_eq!(
            store
                .resolve_uncertain_phase(resolution(fixture))
                .await
                .unwrap(),
            crate::PhaseResolutionOutcome::Replayed(receipt)
        );
    }
}

fn settlement(fixture: &Fixture) -> crate::PhaseSettlement {
    crate::PhaseSettlement {
        result_status: crabber_core::ToolResultStatus::Completed,
        completion: fixture.request.clone(),
        message_id: crabber_core::MessageId(format!(
            "phase-result-{}",
            fixture.request.command.target().id
        )),
        part_id: crabber_core::PartId(format!(
            "phase-part-{}",
            fixture.request.command.target().id
        )),
        turn_id: None,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        duration_ms: 1,
    }
}
fn start(fixture: &Fixture) -> crate::PhaseStart {
    crate::PhaseStart {
        begin: fixture.request.clone(),
        turn_id: None,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn resolution(fixture: &Fixture) -> crate::PhaseResolution {
    super::resolution::descriptor(
        fixture.session.clone(),
        fixture.fence.run_id.clone(),
        fixture.request.clone(),
    )
}
