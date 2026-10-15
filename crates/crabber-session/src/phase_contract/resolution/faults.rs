use super::{evidence, fresh, mixed_output, prepare};
use crate::phase_contract::FaultPoint;
use crate::{PhaseError, PhaseQuiescence, PhaseResolutionOutcome, Store, UncertainPhaseChoice};

pub(super) async fn run(store: &impl Store, set_fault: &impl Fn(FaultPoint)) {
    for choice in [
        UncertainPhaseChoice::Success {
            output: mixed_output(),
        },
        UncertainPhaseChoice::Failure { output: None },
        UncertainPhaseChoice::Refuse,
    ] {
        for point in [FaultPoint::BeforeCommit, FaultPoint::AfterCommit] {
            let f = prepare(store, choice.clone(), PhaseQuiescence::Incomplete, true).await;
            let before = evidence(store, &f.request).await;
            set_fault(point);
            let result = store.resolve_uncertain_phase(f.request.clone()).await;
            set_fault(FaultPoint::Disabled);
            assert_eq!(result.unwrap_err(), PhaseError::UnknownStoreOutcome);
            let receipt = store
                .lookup_phase_mutation(&f.request.session_id, &f.request.resolution.id)
                .await
                .unwrap();
            match point {
                FaultPoint::BeforeCommit => {
                    assert!(receipt.is_none());
                    assert_eq!(evidence(store, &f.request).await, before);
                    fresh(
                        store
                            .resolve_uncertain_phase(f.request.clone())
                            .await
                            .unwrap(),
                    );
                }
                FaultPoint::AfterCommit => {
                    let receipt = receipt.unwrap();
                    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Incomplete);
                    let committed = evidence(store, &f.request).await;
                    assert_eq!(
                        store
                            .resolve_uncertain_phase(f.request.clone())
                            .await
                            .unwrap(),
                        PhaseResolutionOutcome::Replayed(receipt)
                    );
                    assert_eq!(evidence(store, &f.request).await, committed);
                    assert_eq!(committed["run"], before["run"]);
                    assert_eq!(committed["attempt"], before["attempt"]);
                    assert_eq!(committed["approval"], before["approval"]);
                }
                FaultPoint::Disabled => unreachable!(),
            }
        }
    }
}
