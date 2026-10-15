use super::{applied, fixture, mutation, output, resolver};
use crate::{
    AttemptCommand, AttemptError, AttemptFinish, AttemptMutation, AttemptMutationOutcome,
    PhaseCommand, PhaseError, PhaseQuiescence, PhaseQuiescenceResult, PhaseWriteAuthority, Store,
};
use crabber_core::{ControlId, EventKind};

pub(super) async fn run(store: &impl Store) {
    legacy_and_namespace(store).await;
    unresolved_cleanup(store).await;
    ended_worker_quiescence(store).await;
}
async fn legacy_and_namespace(store: &impl Store) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (claimed, _) = applied(
        execution
            .mutate_phase(
                claim.clone(),
                Some(PhaseWriteAuthority::Attempt(owner.clone())),
            )
            .await
            .unwrap(),
    );
    let mut event = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap()
        .pop()
        .unwrap();
    event.kind = EventKind::ToolCallRunning;
    assert_eq!(
        execution
            .claim_tool_call(&claimed.phase.target.call_id, event)
            .await
            .unwrap_err(),
        crate::StoreError::Conflict
    );
    let contents = crabber_core::PublicStateContents::default();
    assert_eq!(
        execution
            .mutate_public_state(crate::PublicStateMutation {
                id: claim.id.clone(),
                expected_revision: claimed.revision,
                contents: contents.clone()
            })
            .await
            .unwrap_err(),
        crate::PublicStateError::SemanticConflict
    );
    let foreign = execution
        .mutate_public_state(crate::PublicStateMutation {
            id: ControlId::new(),
            expected_revision: claimed.revision,
            contents,
        })
        .await
        .unwrap();
    let mut begin = mutation(
        foreign.revision,
        PhaseCommand::Begin {
            target: claimed.phase.target.clone(),
        },
    );
    begin.id = foreign.operation_id;
    assert_eq!(
        execution
            .mutate_phase(begin.clone(), None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_phase_mutation(&admitted.session.id, &begin.id)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    let mut finish = AttemptMutation {
        id: claim.id,
        expected_revision: foreign.revision,
        command: AttemptCommand::Finish {
            target: claimed.phase.target.attempt.clone(),
            outcome: AttemptFinish::OutcomeUnknown,
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(finish.clone(), Some(owner.clone()))
            .await
            .unwrap_err(),
        AttemptError::SemanticConflict
    );
    finish.id = ControlId::new();
    let finish_id = finish.id.clone();
    execution.mutate_attempt(finish, Some(owner)).await.unwrap();
    begin.id = finish_id;
    assert_eq!(
        execution.mutate_phase(begin, None).await.unwrap_err(),
        PhaseError::SemanticConflict
    );
}
async fn unresolved_cleanup(store: &impl Store) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (claimed, _) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner.clone())))
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
    let (quiet, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    uncertain.revision,
                    PhaseCommand::RecordQuiescence {
                        target: uncertain.phase.target,
                        result: PhaseQuiescenceResult::Complete,
                        host_claim: Some(resolver()),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let finish = AttemptMutation {
        id: ControlId::new(),
        expected_revision: quiet.revision,
        command: AttemptCommand::Finish {
            target: quiet.phase.target.attempt,
            outcome: AttemptFinish::OutcomeUnknown,
        },
    };
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(finish, Some(owner.clone()))
        .await
        .unwrap()
    else {
        panic!("fresh finish");
    };
    let cleanup = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::RecordCleanup {
            target: receipt.attempt.target,
            result: crate::AttemptCleanupResult::Complete,
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(cleanup, Some(owner))
            .await
            .unwrap_err(),
        AttemptError::CleanupBlocked
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        receipt.revision
    );
}
async fn ended_worker_quiescence(store: &impl Store) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (claimed, worker) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner.clone())))
            .await
            .unwrap(),
    );
    let worker = worker.unwrap();
    let (started, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: claimed.phase.target,
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker.clone())),
            )
            .await
            .unwrap(),
    );
    let (completed, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    started.revision,
                    PhaseCommand::Complete {
                        target: started.phase.target,
                        output: output(),
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker.clone())),
            )
            .await
            .unwrap(),
    );
    let finish = AttemptMutation {
        id: ControlId::new(),
        expected_revision: completed.revision,
        command: AttemptCommand::Finish {
            target: completed.phase.target.attempt.clone(),
            outcome: AttemptFinish::Completed,
        },
    };
    let AttemptMutationOutcome::Applied { receipt, .. } =
        execution.mutate_attempt(finish, Some(owner)).await.unwrap()
    else {
        panic!("fresh finish");
    };
    let (quiet, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    receipt.revision,
                    PhaseCommand::RecordQuiescence {
                        target: completed.phase.target.clone(),
                        result: PhaseQuiescenceResult::Complete,
                        host_claim: None,
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker)),
            )
            .await
            .unwrap(),
    );
    assert_eq!(quiet.phase.quiescence, PhaseQuiescence::Complete);
    assert_eq!(quiet.phase.state, completed.phase.state);
    assert_eq!(
        store
            .tool_phase(&admitted.run.id, &completed.phase.target.id)
            .await
            .unwrap()
            .phase,
        quiet.phase
    );
}
