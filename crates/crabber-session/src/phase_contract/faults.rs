use super::{applied, fixture, mutation, output, resolver};
use crate::{
    PhaseCommand, PhaseError, PhaseMutationOutcome, PhaseQuiescenceResult, PhaseWriteAuthority,
    Store,
};

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum FaultPoint {
    Disabled = 0,
    BeforeCommit = 1,
    AfterCommit = 2,
}
#[derive(Clone, Copy)]
enum Action {
    Claim,
    Begin,
    Complete,
    Uncertain,
    Quiescence,
}

/// Inject at the adapter's actual transaction commit boundary. After-commit
/// failure withholds the acknowledgement; it must not roll back durable state.
/// # Panics
/// Panics when an adapter exposes partial state or grants authority on replay.
pub async fn run_fault_contract(store: &impl Store, set_fault: impl Fn(FaultPoint)) {
    for action in [
        Action::Claim,
        Action::Begin,
        Action::Complete,
        Action::Uncertain,
        Action::Quiescence,
    ] {
        for point in [FaultPoint::BeforeCommit, FaultPoint::AfterCommit] {
            run_case(store, &set_fault, action, point).await;
        }
    }
    super::settlement::faults(store, &set_fault).await;
    super::start::faults(store, &set_fault).await;
    super::resolution::fault_contract(store, &set_fault).await;
}
async fn run_case(
    store: &impl Store,
    set_fault: &impl Fn(FaultPoint),
    action: Action,
    point: FaultPoint,
) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let target = claim.command.target().clone();
    let (request, authority) = prepare(execution.as_ref(), owner, claim, action).await;
    let before = store.tool_phase(&admitted.run.id, &target.id).await.ok();
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap()
        .len();
    set_fault(point);
    let result = execution
        .mutate_phase(request.clone(), authority.clone())
        .await;
    set_fault(FaultPoint::Disabled);
    assert_eq!(result.unwrap_err(), PhaseError::UnknownStoreOutcome);
    let receipt = store
        .lookup_phase_mutation(&admitted.session.id, &request.id)
        .await
        .unwrap();
    match point {
        FaultPoint::BeforeCommit => {
            assert!(receipt.is_none());
            assert_eq!(
                store.tool_phase(&admitted.run.id, &target.id).await.ok(),
                before
            );
            assert_eq!(
                store.session_revision(&admitted.session.id).await.unwrap(),
                request.expected_revision
            );
            assert_eq!(
                store
                    .list_events(&admitted.session.id, None, 100)
                    .await
                    .unwrap()
                    .len(),
                events
            );
            assert!(matches!(
                execution.mutate_phase(request, authority).await.unwrap(),
                PhaseMutationOutcome::Applied { .. }
            ));
        }
        FaultPoint::AfterCommit => {
            let receipt = receipt.unwrap();
            assert!(
                matches!(execution.mutate_phase(request, None).await.unwrap(), PhaseMutationOutcome::Replayed(r) if r == receipt)
            );
            assert_eq!(
                store.session_revision(&admitted.session.id).await.unwrap(),
                receipt.revision
            );
            assert_eq!(
                store
                    .list_events(&admitted.session.id, None, 100)
                    .await
                    .unwrap()
                    .len(),
                events + 1
            );
        }
        FaultPoint::Disabled => unreachable!(),
    }
}

async fn prepare(
    execution: &dyn crate::ExecutionStore,
    owner: crate::AttemptAuthority,
    claim: crate::PhaseMutation,
    action: Action,
) -> (crate::PhaseMutation, Option<PhaseWriteAuthority>) {
    let target = claim.command.target().clone();
    if matches!(action, Action::Claim) {
        (claim, Some(PhaseWriteAuthority::Attempt(owner)))
    } else {
        let (claimed, worker) = applied(
            execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        let worker = worker.unwrap();
        let mut revision = claimed.revision;
        let command = match action {
            Action::Begin => PhaseCommand::Begin {
                target: target.clone(),
            },
            Action::Complete => {
                let (started, _) = applied(
                    execution
                        .mutate_phase(
                            mutation(
                                revision,
                                PhaseCommand::Begin {
                                    target: target.clone(),
                                },
                            ),
                            Some(PhaseWriteAuthority::Phase(worker.clone())),
                        )
                        .await
                        .unwrap(),
                );
                revision = started.revision;
                PhaseCommand::Complete {
                    target: target.clone(),
                    output: output(),
                }
            }
            Action::Uncertain => PhaseCommand::MarkStoppedUncertain {
                target: target.clone(),
                claim: resolver(),
            },
            Action::Quiescence => {
                let (uncertain, _) = applied(
                    execution
                        .mutate_phase(
                            mutation(
                                revision,
                                PhaseCommand::MarkStoppedUncertain {
                                    target: target.clone(),
                                    claim: resolver(),
                                },
                            ),
                            None,
                        )
                        .await
                        .unwrap(),
                );
                revision = uncertain.revision;
                PhaseCommand::RecordQuiescence {
                    target: target.clone(),
                    result: PhaseQuiescenceResult::Complete,
                    host_claim: Some(resolver()),
                }
            }
            Action::Claim => unreachable!(),
        };
        (
            mutation(revision, command),
            Some(PhaseWriteAuthority::Phase(worker)),
        )
    }
}
