//! Genuine Claim/Begin/Start at the native transaction boundary.
use super::Harness;
use crate::public_session::v2::proof::Proof;
use crate::{PhaseCommand, PhaseError, PhaseMutation, PhaseMutationOutcome, PhaseWriteAuthority};
#[derive(Clone, Copy)]
enum Case {
    Claim,
    Begin,
    Start,
}
pub(crate) async fn run(store: &impl Harness, _: &crabber_core::ManualClock) {
    for case in [Case::Claim, Case::Begin, Case::Start] {
        for lost in [false, true] {
            cell(store, case, lost).await;
        }
    }
}
async fn invoke(
    execution: &dyn crate::ExecutionStore,
    request: PhaseMutation,
    authority: Option<PhaseWriteAuthority>,
    start: Option<crate::PhaseStart>,
) -> Result<PhaseMutationOutcome, PhaseError> {
    match start {
        Some(start) => {
            execution
                .start_tool_phase(
                    start,
                    authority.map(|a| match a {
                        PhaseWriteAuthority::Phase(p) => p,
                        PhaseWriteAuthority::Attempt(_) => panic!("actual phase authority"),
                    }),
                )
                .await
        }
        None => execution.mutate_phase(request, authority).await,
    }
}
async fn cell(store: &impl Harness, case: Case, lost: bool) {
    let Fixture {
        admitted,
        execution,
        request,
        authority,
        start,
    } = fixture(store, case).await;
    let session = &admitted.session.id;
    let run = &admitted.run.id;
    let before = store.retained(session, run, false).await;
    let original = store.retained(session, run, true).await;
    store.probe().arm(session, &request.id, true);
    assert_eq!(
        invoke(
            execution.as_ref(),
            request.clone(),
            Some(authority.clone()),
            start.clone()
        )
        .await
        .unwrap_err(),
        PhaseError::UnknownStoreOutcome
    );
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.retained(session, run, false).await, before);
    assert_eq!(store.retained(session, run, true).await, original);
    store.probe().arm(session, &request.id, false);
    let mut empty = Proof::bounded(0);
    assert_eq!(
        store
            .phase_with_proof(
                admitted.fence.clone(),
                request.clone(),
                Some(authority.clone()),
                start.clone(),
                &mut empty
            )
            .await
            .unwrap_err(),
        PhaseError::UnknownStoreOutcome
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, before);
    store.phase_fault(if lost { 2 } else { 0 });
    let outcome = invoke(
        execution.as_ref(),
        request.clone(),
        Some(authority.clone()),
        start.clone(),
    )
    .await;
    store.phase_fault(0);
    let saved = store
        .lookup_phase_mutation(session, &request.id)
        .await
        .unwrap()
        .unwrap();
    if lost {
        assert_eq!(outcome.unwrap_err(), PhaseError::UnknownStoreOutcome);
    } else {
        assert!(
            matches!(outcome.unwrap(), PhaseMutationOutcome::Applied { receipt, .. } if receipt == saved)
        );
    }
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.retained(session, run, true).await, original);
    let committed = store.retained(session, run, false).await;
    store.probe().arm(session, &request.id, true);
    assert!(
        matches!(invoke(execution.as_ref(), request.clone(), None, start).await.unwrap(), PhaseMutationOutcome::Replayed(receipt) if receipt == saved)
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, committed);
    let mut stale = request;
    stale.id = crabber_core::ControlId::new();
    store.probe().arm(session, &stale.id, true);
    assert_eq!(
        execution
            .mutate_phase(stale, Some(authority))
            .await
            .unwrap_err(),
        PhaseError::RevisionConflict
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, committed);
}

struct Fixture {
    admitted: crate::AdmitOutcome,
    execution: Box<dyn crate::ExecutionStore>,
    request: PhaseMutation,
    authority: PhaseWriteAuthority,
    start: Option<crate::PhaseStart>,
}
async fn fixture(store: &impl Harness, case: Case) -> Fixture {
    let (admitted, execution, owner, claim) = crate::phase_contract::fixture(store).await;
    let (request, authority) = if matches!(case, Case::Claim) {
        (claim, PhaseWriteAuthority::Attempt(owner))
    } else {
        let (receipt, worker) = crate::phase_contract::applied(
            execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        (
            PhaseMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: receipt.revision,
                command: PhaseCommand::Begin {
                    target: receipt.phase.target,
                },
            },
            PhaseWriteAuthority::Phase(worker.unwrap()),
        )
    };
    let start = matches!(case, Case::Start).then(|| crate::PhaseStart {
        begin: request.clone(),
        turn_id: None,
        created_at: admitted.run.created_at,
    });
    Fixture {
        admitted,
        execution,
        request,
        authority,
        start,
    }
}
