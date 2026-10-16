use super::{
    AdmitOutcome, AttemptAuthority, AttemptCommand, AttemptConclusion, AttemptError, AttemptFinish,
    AttemptMutation, AttemptMutationOutcome, AttemptTarget, Clock, ControlId, ExecutionStore,
    ManualClock, RunBoundary, RunControlError, RunControlOutcome, RunControlStatus,
    RunControlTarget, RunStatus, Store, accept, cancel_action, request, steer,
};
use crabber_core::{AttemptGeneration, AttemptId, Usage};
pub(super) async fn start_attempt(
    store: &impl Store,
    admitted: &AdmitOutcome,
) -> (Box<dyn ExecutionStore>, AttemptAuthority, AttemptTarget) {
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let target = AttemptTarget {
        id: AttemptId::new(),
        generation: AttemptGeneration::INITIAL,
    };
    let reserved = execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                command: AttemptCommand::Reserve {
                    target: target.clone(),
                },
            },
            None,
        )
        .await
        .unwrap();
    let AttemptMutationOutcome::Applied {
        authority: Some(owner),
        ..
    } = reserved
    else {
        panic!("fresh reserve");
    };
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                command: AttemptCommand::Begin {
                    target: target.clone(),
                },
            },
            Some(owner.clone()),
        )
        .await
        .unwrap();
    (execution, owner, target)
}
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let target = RunControlTarget::of(&admitted.run);
    let (execution, owner, attempt) = start_attempt(store, &admitted).await;
    let steer = steer(store, &target, "TERMINAL_UNAPPLIED_INPUT").await;
    accept(store, steer).await;
    let cancel = request(store, &target, cancel_action()).await;
    let saved = accept(store, cancel.clone()).await;
    let mut conclusion = AttemptConclusion {
        finish: AttemptMutation {
            id: ControlId::new(),
            expected_revision: store.session_revision(&target.session_id).await.unwrap(),
            command: AttemptCommand::Finish {
                target: attempt.clone(),
                outcome: AttemptFinish::Completed,
            },
        },
        boundary: RunBoundary::Terminal {
            status: RunStatus::Completed,
            error: None,
            usage: Usage::default(),
        },
        created_at: clock.now(),
        turn_id: None,
        elapsed: std::time::Duration::ZERO,
    };
    assert_eq!(
        execution
            .conclude_attempt(conclusion.clone(), Some(owner.clone()))
            .await
            .unwrap_err(),
        AttemptError::PendingInput
    );
    assert_eq!(
        store.run_controls(&target).await.unwrap().controls[1].status,
        RunControlStatus::Accepted
    );
    conclusion.finish.id = ControlId::new();
    conclusion.finish.command = AttemptCommand::Finish {
        target: attempt,
        outcome: AttemptFinish::Interrupted,
    };
    conclusion.boundary = RunBoundary::Terminal {
        status: RunStatus::Interrupted,
        error: Some("cancelled".into()),
        usage: Usage::default(),
    };
    execution
        .conclude_attempt(conclusion.clone(), Some(owner.clone()))
        .await
        .unwrap();
    let page = store.run_controls(&target).await.unwrap();
    assert_eq!(
        page.controls[0].status,
        RunControlStatus::UnappliedTerminal {
            status: RunStatus::Interrupted
        }
    );
    assert_eq!(
        page.controls[1].status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    );
    let before = store.session_revision(&target.session_id).await.unwrap();
    assert!(matches!(
        execution
            .conclude_attempt(conclusion, Some(owner))
            .await
            .unwrap(),
        AttemptMutationOutcome::Replayed(_)
    ));
    assert_eq!(
        store.control_run(cancel).await.unwrap(),
        RunControlOutcome::Replayed(saved)
    );
    assert_eq!(
        store.session_revision(&target.session_id).await.unwrap(),
        before
    );
    let fresh = request(store, &target, cancel_action()).await;
    assert_eq!(
        store.control_run(fresh).await.unwrap_err(),
        RunControlError::Terminal
    );
}
