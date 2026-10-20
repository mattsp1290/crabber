//! Cold host-control replay and killed-transaction recovery.
use super::{Fixture, Mode};
use crate::{PhaseControlOutcome, PhaseError, PhaseMutationOutcome, Store};
fn uncertainty(f: &Fixture) -> crate::PhaseUncertainty {
    crate::PhaseUncertainty {
        session_id: f.session.clone(),
        run_id: f.fence.run_id.clone(),
        id: f.request.id.clone(),
        expected_revision: f.request.expected_revision,
        target: f.request.command.target().clone(),
        cause: crate::PhaseUncertaintyCause::HostStopped {
            claim: super::resolver(),
        },
    }
}
fn cleanup(f: &Fixture) -> crate::PhaseCleanup {
    crate::PhaseCleanup {
        session_id: f.session.clone(),
        run_id: f.fence.run_id.clone(),
        id: f.request.id.clone(),
        expected_revision: f.request.expected_revision,
        target: f.request.command.target().clone(),
        result: crate::PhaseQuiescenceResult::Complete,
        claim: super::resolver(),
    }
}
pub(super) async fn write(
    store: &impl Store,
    f: &Fixture,
) -> Result<PhaseControlOutcome, PhaseError> {
    if matches!(f.mode, Mode::Uncertainty) {
        store.mark_phase_uncertain(uncertainty(f)).await
    } else {
        store.record_phase_cleanup(cleanup(f)).await
    }
}
pub(super) fn outcome(value: PhaseControlOutcome) -> PhaseMutationOutcome {
    match value {
        PhaseControlOutcome::Applied(receipt) => PhaseMutationOutcome::Applied {
            receipt,
            authority: None,
        },
        PhaseControlOutcome::Replayed(receipt) => PhaseMutationOutcome::Replayed(receipt),
    }
}
pub(super) async fn verify(store: &impl Store, f: &Fixture) {
    let receipt = store
        .lookup_phase_mutation(&f.session, &f.request.id)
        .await
        .unwrap()
        .unwrap();
    let digest = if matches!(f.mode, Mode::Uncertainty) {
        uncertainty(f).fingerprint()
    } else {
        cleanup(f).fingerprint()
    }
    .unwrap();
    assert_eq!(receipt.semantic_digest, digest);
    assert_eq!(receipt.phase.state, crate::PhaseState::Uncertain);
    assert_eq!(
        receipt.phase.quiescence,
        if matches!(f.mode, Mode::Uncertainty) {
            crate::PhaseQuiescence::Pending
        } else {
            crate::PhaseQuiescence::Complete
        }
    );
    assert_eq!(
        store.get_run(&f.fence.run_id).await.unwrap(),
        Some(f.initial_run.clone())
    );
    assert_eq!(
        store
            .tool_phase(&f.fence.run_id, &receipt.phase.target.id)
            .await
            .unwrap()
            .phase,
        receipt.phase
    );
    let calls = store
        .list_unfinished_tool_calls(&f.fence.run_id)
        .await
        .unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, crabber_core::ToolCallStatus::Running);
    assert!(calls[0].result.is_none());
    assert_eq!(
        store.list_messages(&f.session, None).await.unwrap().len(),
        1
    );
    assert_eq!(
        store.session_revision(&f.session).await.unwrap(),
        receipt.revision
    );
    assert_eq!(
        store
            .list_events(&f.session, None, 100)
            .await
            .unwrap()
            .len(),
        f.mode.preoperations() + 2
    );
    assert_eq!(
        write(store, f).await.unwrap(),
        PhaseControlOutcome::Replayed(receipt)
    );
}
pub(super) async fn recover(store: &impl Store, f: &Fixture) {
    assert!(matches!(
        write(store, f).await.unwrap(),
        PhaseControlOutcome::Applied(_)
    ));
    verify(store, f).await;
}
