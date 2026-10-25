use super::{applied, terminal};
use crate::{
    AttemptCommand, AttemptConclusion, AttemptError as Error, AttemptFinish, AttemptMutation,
    AttemptState, RunBoundary, Store,
};
use crabber_core::{ControlId, RunStatus, Usage};
use serde_json::json;
pub(super) async fn run(store: &dyn Store) {
    pending_call_can_only_suspend(store).await;
    uncertain_phase_blocks_clean_conclusion(store).await;
}

async fn pending_call_can_only_suspend(store: &dyn Store) {
    let (admitted, execution, owner, claim) = crate::phase_contract::fixture(store).await;
    let mut conclusion = AttemptConclusion {
        finish: AttemptMutation {
            id: ControlId::new(),
            expected_revision: claim.expected_revision,
            command: AttemptCommand::Finish {
                target: claim.command.target().attempt.clone(),
                outcome: AttemptFinish::Completed,
            },
        },
        boundary: terminal(RunStatus::Completed),
        created_at: admitted.run.created_at,
        turn_id: None,
        elapsed: std::time::Duration::ZERO,
    };
    assert_eq!(
        execution
            .conclude_attempt(conclusion.clone(), Some(owner.clone()))
            .await
            .unwrap_err(),
        Error::CleanupBlocked
    );
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
    conclusion.finish.id = ControlId::new();
    if let AttemptCommand::Finish { outcome, .. } = &mut conclusion.finish.command {
        *outcome = AttemptFinish::Suspended;
    }
    conclusion.boundary = RunBoundary::Suspended {
        checkpoint: json!({"pending_calls":[claim.command.target().call_id]}),
        usage: Usage::default(),
    };
    let (receipt, _) = applied(
        execution
            .conclude_attempt(conclusion, Some(owner))
            .await
            .unwrap(),
    );
    assert_eq!(receipt.attempt.state, AttemptState::Suspended);
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Paused
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()
            .len(),
        1
    );
}
async fn uncertain_phase_blocks_clean_conclusion(store: &dyn Store) {
    let (admitted, execution, owner, claim) = crate::phase_contract::fixture(store).await;
    let (claimed, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                claim,
                Some(crate::PhaseWriteAuthority::Attempt(owner.clone())),
            )
            .await
            .unwrap(),
    );
    let (uncertain, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    claimed.revision,
                    crate::PhaseCommand::MarkStoppedUncertain {
                        target: claimed.phase.target,
                        claim: crate::phase_contract::resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let (quiet, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    uncertain.revision,
                    crate::PhaseCommand::RecordQuiescence {
                        target: uncertain.phase.target,
                        result: crate::PhaseQuiescenceResult::Complete,
                        host_claim: Some(crate::phase_contract::resolver()),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let conclusion = AttemptConclusion {
        finish: AttemptMutation {
            id: ControlId::new(),
            expected_revision: quiet.revision,
            command: AttemptCommand::Finish {
                target: quiet.phase.target.attempt,
                outcome: AttemptFinish::Suspended,
            },
        },
        boundary: RunBoundary::Suspended {
            checkpoint: json!({}),
            usage: Usage::default(),
        },
        created_at: admitted.run.created_at,
        turn_id: None,
        elapsed: std::time::Duration::ZERO,
    };
    assert_eq!(
        execution
            .conclude_attempt(conclusion.clone(), Some(owner))
            .await
            .unwrap_err(),
        Error::CleanupBlocked
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        quiet.revision
    );
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
    assert_eq!(
        store
            .execution_attempt(&admitted.run.id, &conclusion.finish.command.target().id)
            .await
            .unwrap()
            .attempt
            .state,
        AttemptState::Started
    );
}
