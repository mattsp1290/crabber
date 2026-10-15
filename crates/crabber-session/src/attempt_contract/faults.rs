use super::{applied, reserve};
pub use crate::public_state_contract::FaultPoint;
use crate::{
    AdmitOutcome, AttemptAuthority, AttemptCleanupResult, AttemptCommand, AttemptError as Error,
    AttemptFinish, AttemptMutation, AttemptMutationOutcome as Outcome, ExecutionStore, Store,
};
use crabber_core::{Clock, ControlId, ManualClock};

#[derive(Clone, Copy)]
enum Case {
    Reserve,
    Reclaim,
    Begin,
    Finish,
    Cleanup,
}
/// Fault each attempt transition before COMMIT and after COMMIT acknowledgement.
/// # Panics
/// Panics if rollback or metadata-only reconciliation violates the contract.
pub async fn run_fault_contract(
    store: &impl Store,
    clock: &ManualClock,
    inject: impl Fn(FaultPoint),
) {
    for case in [
        Case::Reserve,
        Case::Reclaim,
        Case::Begin,
        Case::Finish,
        Case::Cleanup,
    ] {
        for point in [FaultPoint::BeforeCommit, FaultPoint::AfterCommit] {
            let (admitted, execution, request, authority) = fixture(store, clock, case).await;
            let before = store
                .execution_attempt(&admitted.run.id, &request.command.target().id)
                .await;
            let events = store
                .list_events(&admitted.session.id, None, 100)
                .await
                .unwrap()
                .len();
            inject(point);
            assert_eq!(
                execution
                    .mutate_attempt(request.clone(), authority.clone())
                    .await
                    .unwrap_err(),
                Error::UnknownStoreOutcome
            );
            inject(FaultPoint::Off);
            let found = store
                .lookup_attempt_mutation(&admitted.session.id, &request.id)
                .await
                .unwrap();
            let receipt = if matches!(point, FaultPoint::BeforeCommit) {
                assert_eq!(found, None);
                assert_eq!(
                    store
                        .execution_attempt(&admitted.run.id, &request.command.target().id)
                        .await,
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
                applied(
                    execution
                        .mutate_attempt(request.clone(), authority)
                        .await
                        .unwrap(),
                )
                .0
            } else {
                found.expect("committed operation must be discoverable")
            };
            assert_eq!(
                receipt.semantic_digest,
                request
                    .fingerprint(&admitted.session.id, &admitted.run.id)
                    .unwrap()
            );
            assert_eq!(
                store
                    .execution_attempt(&admitted.run.id, &receipt.attempt.target.id)
                    .await
                    .unwrap()
                    .attempt,
                receipt.attempt
            );
            assert!(
                matches!(execution.mutate_attempt(request.clone(),None).await.unwrap(), Outcome::Replayed(r) if r == receipt)
            );
            assert_eq!(
                store
                    .list_events(&admitted.session.id, None, 100)
                    .await
                    .unwrap()
                    .len(),
                events + 1
            );
            assert_eq!(
                store.session_revision(&admitted.session.id).await.unwrap(),
                receipt.revision
            );
        }
    }
    super::conclusion::faults(store, clock, &inject).await;
}
async fn fixture(
    store: &impl Store,
    clock: &ManualClock,
    case: Case,
) -> (
    AdmitOutcome,
    Box<dyn ExecutionStore>,
    AttemptMutation,
    Option<AttemptAuthority>,
) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let mut execution = store.execution(admitted.fence.clone()).await.unwrap();
    let mut request = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    if matches!(case, Case::Reserve) {
        return (admitted, execution, request, None);
    }
    let (mut previous, authority) = applied(
        execution
            .mutate_attempt(request.clone(), None)
            .await
            .unwrap(),
    );
    let target = previous.attempt.target.clone();
    if matches!(case, Case::Reclaim) {
        clock.set(admitted.run.lease_until);
        let fence = store
            .claim_expired_run(&admitted.run.id, "reclaimer")
            .await
            .unwrap();
        execution = store.execution(fence).await.unwrap();
        request.id = ControlId::new();
        request.expected_revision = previous.revision;
        request.command = AttemptCommand::ReclaimUnstarted { target };
        return (admitted, execution, request, None);
    }
    request.id = ControlId::new();
    request.expected_revision = previous.revision;
    request.command = AttemptCommand::Begin {
        target: target.clone(),
    };
    if matches!(case, Case::Begin) {
        return (admitted, execution, request, authority);
    }
    previous = applied(
        execution
            .mutate_attempt(request.clone(), authority.clone())
            .await
            .unwrap(),
    )
    .0;
    request.id = ControlId::new();
    request.expected_revision = previous.revision;
    request.command = AttemptCommand::Finish {
        target: target.clone(),
        outcome: AttemptFinish::Suspended,
    };
    if matches!(case, Case::Finish) {
        return (admitted, execution, request, authority);
    }
    previous = applied(
        execution
            .mutate_attempt(request.clone(), authority.clone())
            .await
            .unwrap(),
    )
    .0;
    request.id = ControlId::new();
    request.expected_revision = previous.revision;
    request.command = AttemptCommand::RecordCleanup {
        target,
        result: AttemptCleanupResult::Complete,
    };
    (admitted, execution, request, authority)
}
