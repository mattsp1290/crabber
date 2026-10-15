//! Preserve a known protected interruption without inventing an executor result.
use super::{read_calls, started};
use crate::phase_contract::applied;
use crate::{PhaseError, PhaseMutationOutcome, PhaseQuiescence, PhaseState, Store};
use crabber_core::ToolCallStatus;

pub(super) async fn run(store: &impl Store) {
    let (admitted, execution, owner, mut settlement) = started(store, true).await;
    settlement.result_status = crabber_core::ToolResultStatus::Interrupted;
    let (receipt, _) = applied(
        execution
            .settle_tool_phase(settlement.clone(), Some(owner))
            .await
            .unwrap(),
    );
    assert_eq!(receipt.phase.state, PhaseState::Failed);
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Complete);
    let calls = read_calls(store, &admitted.session.id).await.unwrap();
    assert_eq!(calls[0].status, ToolCallStatus::Interrupted);
    assert_eq!(
        calls[0].result.as_ref().unwrap().status,
        crabber_core::ToolResultStatus::Interrupted
    );
    assert_eq!(
        calls[0].result.as_ref().unwrap().content,
        settlement.output().unwrap().to_content_blocks()
    );
    assert!(
        matches!(execution.settle_tool_phase(settlement.clone(), None).await.unwrap(),
        PhaseMutationOutcome::Replayed(saved) if saved == receipt)
    );
    let mut changed = settlement.clone();
    changed.result_status = crabber_core::ToolResultStatus::Failed;
    assert_eq!(
        execution
            .settle_tool_phase(changed, None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    settlement.completion.id = crabber_core::ControlId::new();
    settlement.result_status = crabber_core::ToolResultStatus::Completed;
    assert_eq!(
        execution
            .settle_tool_phase(settlement, None)
            .await
            .unwrap_err(),
        PhaseError::InvalidRequest
    );
}
