//! Test-only crash/reconciliation fixtures. Worker authorities never cross a
//! process boundary; the writer acquires its own fresh native ownership.
#[path = "process/child.rs"]
pub(crate) mod child;
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
    Success,
    Failure,
    Refuse,
}
impl Mode {
    pub(crate) const ALL: [Self; 5] = [
        Self::Claim,
        Self::Begin,
        Self::Success,
        Self::Failure,
        Self::Refuse,
    ];
    pub(crate) fn operation(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Begin => "begin",
            Self::Success => "resolve_success",
            Self::Failure => "resolve_failure",
            Self::Refuse => "resolve_refuse",
        }
    }
    fn preoperations(self) -> usize {
        match self {
            Self::Claim => 2,
            Self::Begin => 3,
            _ => 4,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Fixture {
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
        Mode::Begin => PhaseCommand::Begin { target },
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
    let result = execution
        .mutate_phase(fixture.request.clone(), authority)
        .await;
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
    if matches!(fixture.mode, Mode::Begin) {
        return Some(PhaseWriteAuthority::Phase(worker.unwrap()));
    }
    execution
        .mutate_phase(
            mutation(
                claimed.revision,
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
    assert_eq!(call.status, ToolCallStatus::Pending);
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
        Mode::Begin => assert_eq!(snapshot.unwrap().phase.state, PhaseState::Claimed),
        _ => assert_eq!(snapshot.unwrap().phase.state, PhaseState::Uncertain),
    }
}
pub(crate) async fn verify_loss(store: &impl Store, fixture: Fixture) {
    let receipt = store
        .lookup_phase_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.semantic_digest,
        fixture
            .request
            .fingerprint(&fixture.session, &fixture.fence.run_id)
            .unwrap()
    );
    let current = store
        .tool_phase(&fixture.fence.run_id, &receipt.phase.target.id)
        .await
        .unwrap();
    assert_eq!(current.phase, receipt.phase);
    assert_eq!(current.revision, receipt.revision);
    let calls = store
        .list_unfinished_tool_calls(&fixture.fence.run_id)
        .await
        .unwrap();
    let call = calls
        .iter()
        .find(|c| c.id == receipt.phase.target.call_id)
        .unwrap();
    let expected_call = if matches!(fixture.mode, Mode::Begin) {
        ToolCallStatus::Running
    } else {
        ToolCallStatus::Pending
    };
    assert_eq!(call.status, expected_call);
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap()
            .len(),
        fixture.mode.preoperations() + 2
    );
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    assert!(
        matches!(execution.mutate_phase(fixture.request, None).await.unwrap(), PhaseMutationOutcome::Replayed(r) if r == receipt)
    );
    assert_eq!(
        store
            .tool_phase(&fixture.fence.run_id, &receipt.phase.target.id)
            .await
            .unwrap(),
        current
    );
    let expected = match fixture.mode {
        Mode::Claim => PhaseState::Claimed,
        Mode::Begin => PhaseState::Started,
        Mode::Success => PhaseState::Completed,
        Mode::Failure => PhaseState::Failed,
        Mode::Refuse => PhaseState::Uncertain,
    };
    assert_eq!(receipt.phase.state, expected);
}

pub(crate) async fn recover_killed_writer(store: &impl Store, fixture: &Fixture) {
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    if matches!(fixture.mode, Mode::Claim | Mode::Begin) {
        // A reaped worker's opaque authority is gone. The same outbox must not
        // turn a retry into dispatch permission, even when BEGIN rolled back.
        assert_eq!(
            execution
                .mutate_phase(fixture.request.clone(), None)
                .await
                .unwrap_err(),
            PhaseError::StaleOwner
        );
        if matches!(fixture.mode, Mode::Begin) {
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
        // Only the original, explicit host resolution is retried here.
        let (receipt, authority) = applied(
            execution
                .mutate_phase(fixture.request.clone(), None)
                .await
                .unwrap(),
        );
        assert!(authority.is_none());
        assert_eq!(
            receipt.revision,
            fixture.request.expected_revision.next().unwrap()
        );
        assert!(
            matches!(execution.mutate_phase(fixture.request.clone(), None).await.unwrap(), PhaseMutationOutcome::Replayed(r) if r == receipt)
        );
    }
}
