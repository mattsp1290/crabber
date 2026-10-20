//! Outcome resolution and cleanup commute without retiring the parent.
use super::{cleanup, evidence, fresh, uncertainty};
use crate::phase_contract::{applied, fixture, mutation, resolver};
use crate::{
    PhaseCleanup, PhaseCommand, PhaseControlOutcome, PhaseError, PhaseQuiescence,
    PhaseQuiescenceResult, PhaseReceipt, PhaseState, PhaseUncertainty, PhaseUncertaintyCause,
    PhaseWriteAuthority, Store, UncertainPhaseChoice,
};
use crabber_core::{ControlId, ManualClock};
use time::OffsetDateTime;
pub(super) async fn run(
    store: &impl Store,
    clock: &ManualClock,
    started: bool,
    resolve_first: bool,
) {
    clock.set(OffsetDateTime::UNIX_EPOCH);
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (mut receipt, worker) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    if started {
        receipt = applied(
            execution
                .mutate_phase(
                    mutation(
                        receipt.revision,
                        PhaseCommand::Begin {
                            target: receipt.phase.target.clone(),
                        },
                    ),
                    Some(PhaseWriteAuthority::Phase(worker.clone().unwrap())),
                )
                .await
                .unwrap(),
        )
        .0;
    }
    let request = uncertainty(
        admitted.session.id.clone(),
        admitted.run.id.clone(),
        &receipt,
    );
    let before = evidence(store, &request).await;
    assert_eq!(
        store
            .mark_phase_uncertain(request.clone())
            .await
            .unwrap_err(),
        PhaseError::LiveOwner
    );
    assert_eq!(evidence(store, &request).await, before);
    assert!(
        store
            .lookup_phase_mutation(&request.session_id, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    let invalid_cleanup = cleanup(&request, receipt.revision, PhaseQuiescenceResult::Complete);
    assert_eq!(
        store
            .record_phase_cleanup(invalid_cleanup.clone())
            .await
            .unwrap_err(),
        PhaseError::InvalidTransition
    );
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::days(1));
    receipt = fresh(store.mark_phase_uncertain(request.clone()).await.unwrap());
    assert_eq!(receipt.phase.state, PhaseState::Uncertain);
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Pending);
    assert_eq!(evidence(store, &request).await, before);
    assert_eq!(
        store.mark_phase_uncertain(request.clone()).await.unwrap(),
        PhaseControlOutcome::Replayed(receipt.clone())
    );
    let revision = receipt.revision;
    assert_eq!(
        execution
            .mutate_phase(
                mutation(
                    revision,
                    PhaseCommand::Complete {
                        target: request.target.clone(),
                        output: super::super::output()
                    }
                ),
                Some(PhaseWriteAuthority::Phase(worker.unwrap()))
            )
            .await
            .unwrap_err(),
        PhaseError::StaleOwner
    );
    // The old metadata domain cannot replay a host-control operation key.
    assert_eq!(
        execution
            .mutate_phase(request.mutation(), None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    let mut changed = request.clone();
    changed.cause = PhaseUncertaintyCause::HostStopped { claim: resolver() };
    assert_eq!(
        store.mark_phase_uncertain(changed).await.unwrap_err(),
        PhaseError::SemanticConflict
    );
    reconcile(store, request, receipt, invalid_cleanup, resolve_first).await;
}
async fn reconcile(
    store: &impl Store,
    mut request: PhaseUncertainty,
    mut receipt: PhaseReceipt,
    mut invalid_cleanup: PhaseCleanup,
    resolve_first: bool,
) {
    let resolution = super::super::resolution::descriptor(
        request.session_id.clone(),
        request.run_id.clone(),
        mutation(
            receipt.revision,
            PhaseCommand::ResolveUncertain {
                target: request.target.clone(),
                claim: resolver(),
                choice: UncertainPhaseChoice::Success {
                    output: super::super::output(),
                },
            },
        ),
    );
    if resolve_first {
        receipt = super::super::resolution::fresh(
            store.resolve_uncertain_phase(resolution).await.unwrap(),
        );
    }
    let incomplete = cleanup(
        &request,
        receipt.revision,
        PhaseQuiescenceResult::Incomplete,
    );
    receipt = fresh(
        store
            .record_phase_cleanup(incomplete.clone())
            .await
            .unwrap(),
    );
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Incomplete);
    if !resolve_first {
        let resolution = super::super::resolution::descriptor(
            request.session_id.clone(),
            request.run_id.clone(),
            mutation(
                receipt.revision,
                PhaseCommand::ResolveUncertain {
                    target: request.target.clone(),
                    claim: resolver(),
                    choice: UncertainPhaseChoice::Success {
                        output: super::super::output(),
                    },
                },
            ),
        );
        receipt = super::super::resolution::fresh(
            store.resolve_uncertain_phase(resolution).await.unwrap(),
        );
        assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Incomplete);
    }
    let complete = cleanup(&request, receipt.revision, PhaseQuiescenceResult::Complete);
    receipt = fresh(store.record_phase_cleanup(complete.clone()).await.unwrap());
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Complete);
    assert_eq!(receipt.phase.state, PhaseState::Completed);
    assert_eq!(
        store
            .record_phase_cleanup(incomplete.clone())
            .await
            .unwrap(),
        PhaseControlOutcome::Replayed(
            store
                .lookup_phase_mutation(&request.session_id, &incomplete.id)
                .await
                .unwrap()
                .unwrap()
        )
    );
    assert_eq!(
        store.record_phase_cleanup(complete.clone()).await.unwrap(),
        PhaseControlOutcome::Replayed(receipt.clone())
    );
    invalid_cleanup.expected_revision = receipt.revision;
    assert_eq!(
        store
            .record_phase_cleanup(invalid_cleanup)
            .await
            .unwrap_err(),
        PhaseError::InvalidTransition
    );
    request.id = ControlId::new();
    request.expected_revision = receipt.revision;
    assert_eq!(
        store.mark_phase_uncertain(request).await.unwrap_err(),
        PhaseError::InvalidTransition
    );
}
