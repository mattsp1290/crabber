//! Test-only fixtures for private SQL payload/receipt integrity checks.
use super::{applied, fixture, mutation, resolver};
use crate::{
    AdmitOutcome, PhaseCommand, PhaseError, PhaseMutation, PhaseMutationOutcome, PhaseReceipt,
    PhaseWriteAuthority, Store, UncertainPhaseChoice,
};
use crabber_core::{MessageContent, ToolOutput};

pub(crate) struct Fixture {
    pub admitted: AdmitOutcome,
    pub request: PhaseMutation,
    pub receipt: PhaseReceipt,
    pub output: ToolOutput,
}
pub(crate) async fn completed(store: &impl Store) -> Fixture {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (claimed, _) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let (uncertain, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: claimed.phase.target,
                        claim: resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let output = ToolOutput::new(
        vec![MessageContent::Text {
            text: "literal\n\"final result\"".into(),
        }],
        false,
    )
    .unwrap();
    let request = mutation(
        uncertain.revision,
        PhaseCommand::ResolveUncertain {
            target: uncertain.phase.target,
            claim: resolver(),
            choice: UncertainPhaseChoice::Success {
                output: output.clone(),
            },
        },
    );
    let (receipt, _) = applied(execution.mutate_phase(request.clone(), None).await.unwrap());
    Fixture {
        admitted,
        request,
        receipt,
        output,
    }
}
pub(crate) async fn isolated_receipt(store: &impl Store, fixture: &Fixture) {
    let receipt = store
        .lookup_phase_mutation(&fixture.admitted.session.id, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt, fixture.receipt);
    let execution = store
        .execution(fixture.admitted.fence.clone())
        .await
        .unwrap();
    assert!(
        matches!(execution.mutate_phase(fixture.request.clone(), None).await.unwrap(), PhaseMutationOutcome::Replayed(r) if r == receipt)
    );
    assert_eq!(
        store
            .session_revision(&fixture.admitted.session.id)
            .await
            .unwrap(),
        fixture.receipt.revision
    );
}
pub(crate) async fn reject_corrupt_receipt(store: &impl Store, fixture: &Fixture) {
    assert_eq!(
        store
            .lookup_phase_mutation(&fixture.admitted.session.id, &fixture.request.id)
            .await
            .unwrap_err(),
        PhaseError::UnknownStoreOutcome
    );
    let execution = store
        .execution(fixture.admitted.fence.clone())
        .await
        .unwrap();
    assert_eq!(
        execution
            .mutate_phase(fixture.request.clone(), None)
            .await
            .unwrap_err(),
        PhaseError::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .session_revision(&fixture.admitted.session.id)
            .await
            .unwrap(),
        fixture.receipt.revision
    );
}
