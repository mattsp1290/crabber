//! Native phase conformance. Dispatch requires a fresh committed Begin;
//! metadata replay, resolver choices and quiescence never grant dispatch.
#[path = "phase_contract/controls.rs"]
pub(crate) mod controls;
#[path = "phase_contract/faults.rs"]
mod faults;
#[path = "phase_contract/guards.rs"]
mod guards;
#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "phase_contract/process.rs"]
pub(crate) mod process;
#[path = "phase_contract/recovery.rs"]
mod recovery;
#[path = "phase_contract/resolution.rs"]
mod resolution;
#[path = "phase_contract/settlement.rs"]
mod settlement;
#[path = "phase_contract/start.rs"]
mod start;
#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "phase_contract/storage.rs"]
pub(crate) mod storage;
#[cfg(test)]
#[path = "phase_contract/terminal_carrier.rs"]
pub(crate) mod terminal_carrier;
use crate::{
    AttemptAuthority, AttemptCommand, AttemptMutation, AttemptMutationOutcome, PhaseAuthority,
    PhaseCommand, PhaseError as Error, PhaseMutation, PhaseMutationOutcome as Outcome,
    PhaseQuiescence, PhaseReceipt, PhaseResolverClaim, PhaseState, PhaseTarget,
    PhaseWriteAuthority, Store,
};
use crabber_core::{
    ControlId, EventKind, EventRecord, ManualClock, PhaseGeneration, PhaseId, SessionRevision,
    ToolCallId, ToolCallRecord, ToolCallStatus, ToolOutput,
};
pub use faults::{FaultPoint, run_fault_contract};
use std::sync::Arc;
use time::OffsetDateTime;

/// # Panics
/// Panics when fixture setup or an adapter's native phase contract fails.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    controls::run(&store, &clock).await;
    lifecycle(&store).await;
    settlement::run(&store).await;
    start::run(&store).await;
    resolution::run(&store, &clock).await;
    guards::run(&store).await;
    recovery::run(&store, &clock).await;
}
pub(crate) fn output() -> ToolOutput {
    ToolOutput::from_json(&serde_json::json!({"literal":"private result"})).unwrap()
}
pub(crate) fn mutation(revision: SessionRevision, command: PhaseCommand) -> PhaseMutation {
    PhaseMutation {
        id: ControlId::new(),
        expected_revision: revision,
        command,
    }
}
pub(crate) fn applied(outcome: Outcome) -> (PhaseReceipt, Option<PhaseAuthority>) {
    let Outcome::Applied { receipt, authority } = outcome else {
        panic!("fresh operation must apply");
    };
    (receipt, authority)
}
pub(crate) async fn fixture(
    store: &impl Store,
) -> (
    crate::AdmitOutcome,
    Box<dyn crate::ExecutionStore>,
    AttemptAuthority,
    PhaseMutation,
) {
    let admitted = crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
    fixture_for_admission(store, admitted).await
}
pub(crate) async fn fixture_for_admission(
    store: &impl Store,
    admitted: crate::AdmitOutcome,
) -> (
    crate::AdmitOutcome,
    Box<dyn crate::ExecutionStore>,
    AttemptAuthority,
    PhaseMutation,
) {
    fixture_for_admission_named(store, admitted, "phase-owned").await
}
async fn fixture_for_admission_named(
    store: &impl Store,
    admitted: crate::AdmitOutcome,
    name: &str,
) -> (
    crate::AdmitOutcome,
    Box<dyn crate::ExecutionStore>,
    AttemptAuthority,
    PhaseMutation,
) {
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    if store
        .admission_execution_state(&admitted.run.id)
        .await
        .unwrap()
        == Some(crate::AdmissionExecutionState::Unstarted)
    {
        execution.begin_admission_execution().await.unwrap();
    }
    let reserve = crate::attempt_contract::reserve(
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let AttemptMutationOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = execution.mutate_attempt(reserve, None).await.unwrap()
    else {
        panic!("reserve authority missing");
    };
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: receipt.attempt.target.clone(),
                },
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let call = ToolCallId(format!("provider:原本/{}", uuid::Uuid::new_v4()));
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call.clone(),
                run_id: admitted.run.id.clone(),
                name: name.into(),
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
        attempt: receipt.attempt.target,
    };
    let request = mutation(
        store.session_revision(&admitted.session.id).await.unwrap(),
        PhaseCommand::Claim { target },
    );
    (admitted, execution, authority, request)
}
async fn lifecycle(store: &impl Store) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    assert_eq!(
        execution
            .mutate_phase(claim.clone(), None)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let (claimed, phase_owner) = applied(
        execution
            .mutate_phase(claim.clone(), Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let phase_owner = phase_owner.unwrap();
    assert!(
        matches!(execution.mutate_phase(claim, None).await.unwrap(), Outcome::Replayed(r) if r == claimed)
    );
    let begin = mutation(
        claimed.revision,
        PhaseCommand::Begin {
            target: claimed.phase.target.clone(),
        },
    );
    assert_eq!(
        execution
            .mutate_phase(begin.clone(), None)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    competing_begin(execution.as_ref(), &begin, &phase_owner).await;
    let started = store
        .tool_phase(&admitted.run.id, &claimed.phase.target.id)
        .await
        .unwrap();
    assert_eq!(started.phase.state, PhaseState::Started);
    let complete = mutation(
        started.revision,
        PhaseCommand::Complete {
            target: started.phase.target,
            output: output(),
        },
    );
    let (completed, no_grant) = applied(
        execution
            .mutate_phase(
                complete.clone(),
                Some(PhaseWriteAuthority::Phase(phase_owner)),
            )
            .await
            .unwrap(),
    );
    assert!(no_grant.is_none());
    assert_eq!(completed.phase.state, PhaseState::Completed);
    assert_eq!(completed.phase.quiescence, PhaseQuiescence::Pending);
    assert_eq!(
        store
            .lookup_phase_mutation(&admitted.session.id, &complete.id)
            .await
            .unwrap(),
        Some(completed.clone())
    );
    assert!(matches!(
        execution.mutate_phase(begin, None).await.unwrap(),
        Outcome::Replayed(_)
    ));
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        completed.revision
    );
    let mut changed = complete;
    changed.command = PhaseCommand::Complete {
        target: completed.phase.target,
        output: output().with_error(),
    };
    assert_eq!(
        execution.mutate_phase(changed, None).await.unwrap_err(),
        Error::SemanticConflict
    );
}
pub(crate) fn resolver() -> PhaseResolverClaim {
    PhaseResolverClaim {
        resolver_id: "operator-fixture".into(),
        authority_claim: "domain owner reconciled external effect".into(),
    }
}
async fn competing_begin(
    execution: &dyn crate::ExecutionStore,
    begin: &PhaseMutation,
    phase_owner: &PhaseAuthority,
) {
    let (left, right) = futures::join!(
        execution.mutate_phase(
            begin.clone(),
            Some(PhaseWriteAuthority::Phase(phase_owner.clone()))
        ),
        execution.mutate_phase(
            begin.clone(),
            Some(PhaseWriteAuthority::Phase(phase_owner.clone()))
        )
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(
                o,
                Outcome::Applied {
                    authority: None,
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, Outcome::Replayed(_)))
            .count(),
        1
    );
}
