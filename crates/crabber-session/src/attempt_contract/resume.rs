//! A paused run is activated only by a fresh reservation after proved cleanup.
use super::{applied, reserve};
use crate::{
    AttemptAuthority, AttemptCleanup, AttemptCommand, AttemptConclusion, AttemptError as Error,
    AttemptFinish, AttemptMutation, AttemptMutationOutcome as Outcome, AttemptState,
    ExecutionStore, RunBoundary, Store,
};
use crabber_core::{Clock, ControlId, ManualClock, RunStatus, Usage};
use serde_json::json;

async fn suspended(
    store: &impl Store,
    clock: &ManualClock,
) -> (
    crate::AdmitOutcome,
    Box<dyn ExecutionStore>,
    AttemptAuthority,
    crate::AttemptReceipt,
) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    assert!(
        store
            .latest_execution_attempt(&admitted.run.id)
            .await
            .unwrap()
            .is_none()
    );
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let (reserved, owner) = applied(
        execution
            .mutate_attempt(
                reserve(store.session_revision(&admitted.session.id).await.unwrap()),
                None,
            )
            .await
            .unwrap(),
    );
    let owner = owner.unwrap();
    let (begun, _) = applied(
        execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: reserved.revision,
                    command: AttemptCommand::Begin {
                        target: reserved.attempt.target,
                    },
                },
                Some(owner.clone()),
            )
            .await
            .unwrap(),
    );
    let (finished, _) = applied(
        execution
            .conclude_attempt(
                AttemptConclusion {
                    finish: AttemptMutation {
                        id: ControlId::new(),
                        expected_revision: begun.revision,
                        command: AttemptCommand::Finish {
                            target: begun.attempt.target,
                            outcome: AttemptFinish::Suspended,
                        },
                    },
                    boundary: RunBoundary::Suspended {
                        checkpoint: json!({"private":"carry"}),
                        usage: Usage::default(),
                    },
                    created_at: clock.now(),
                    turn_id: None,
                    elapsed: std::time::Duration::ZERO,
                },
                Some(owner.clone()),
            )
            .await
            .unwrap(),
    );
    (admitted, execution, owner, finished)
}
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    assert_eq!(
        store
            .latest_execution_attempt(&crabber_core::RunId::new())
            .await
            .unwrap_err(),
        Error::NotFound
    );
    lifecycle(store, clock).await;
    legacy_pause_cannot_gain_attempt_authority(store, clock).await;
}
async fn lifecycle(store: &impl Store, clock: &ManualClock) {
    let (admitted, old_execution, old_owner, finished) = suspended(store, clock).await;
    let current = store
        .latest_execution_attempt(&admitted.run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.attempt, finished.attempt);
    assert_eq!(current.revision, finished.revision);
    assert_eq!(current.attempt.cleanup, AttemptCleanup::Complete);
    let mut next = reserve(finished.revision);
    if let AttemptCommand::Reserve { target } = &mut next.command {
        target.generation = finished.attempt.target.generation.next().unwrap();
    }
    assert_eq!(
        old_execution
            .mutate_attempt(next.clone(), None)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let fence = store
        .claim_expired_run(&admitted.run.id, "resumed-host")
        .await
        .unwrap();
    let execution = store.execution(fence).await.unwrap();
    next.expected_revision = store.session_revision(&admitted.session.id).await.unwrap();
    let mut wrong = next.clone();
    if let AttemptCommand::Reserve { target } = &mut wrong.command {
        target.generation = crabber_core::AttemptGeneration::INITIAL;
    }
    assert_eq!(
        execution.mutate_attempt(wrong, None).await.unwrap_err(),
        Error::GenerationConflict
    );
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Paused
    );
    let mut competing = next.clone();
    competing.id = ControlId::new();
    if let AttemptCommand::Reserve { target } = &mut competing.command {
        target.id = crabber_core::AttemptId::new();
    }
    let (left, right) = futures::join!(
        execution.mutate_attempt(next.clone(), None),
        execution.mutate_attempt(competing.clone(), None)
    );
    let (receipt, owner, winning) = match (left, right) {
        (Ok(outcome), Err(Error::RevisionConflict)) => {
            let (receipt, owner) = applied(outcome);
            (receipt, owner.unwrap(), next)
        }
        (Err(Error::RevisionConflict), Ok(outcome)) => {
            let (receipt, owner) = applied(outcome);
            (receipt, owner.unwrap(), competing)
        }
        other => panic!("exactly one activation must win: {other:?}"),
    };
    assert_eq!(receipt.attempt.state, AttemptState::Unstarted);
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
    assert!(
        matches!(execution.mutate_attempt(winning,None).await.unwrap(),Outcome::Replayed(saved) if saved==receipt)
    );
    let latest = store
        .latest_execution_attempt(&admitted.run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.attempt, receipt.attempt);
    assert_eq!(latest.revision, receipt.revision);
    begin_winner(execution.as_ref(), &receipt, owner, old_owner).await;
}
async fn begin_winner(
    execution: &dyn ExecutionStore,
    receipt: &crate::AttemptReceipt,
    owner: AttemptAuthority,
    old_owner: AttemptAuthority,
) {
    let begin_mutation = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: receipt.attempt.target.clone(),
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(begin_mutation.clone(), Some(old_owner))
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let (started, _) = applied(
        execution
            .mutate_attempt(begin_mutation.clone(), Some(owner))
            .await
            .unwrap(),
    );
    assert_eq!(started.attempt.state, AttemptState::Started);
    assert!(matches!(
        execution
            .mutate_attempt(begin_mutation, None)
            .await
            .unwrap(),
        Outcome::Replayed(_)
    ));
}

pub(super) async fn faults(
    store: &impl Store,
    clock: &ManualClock,
    inject: &impl Fn(super::FaultPoint),
) {
    for point in [
        super::FaultPoint::BeforeCommit,
        super::FaultPoint::AfterCommit,
    ] {
        let (admitted, _, _, finished) = suspended(store, clock).await;
        let fence = store
            .claim_expired_run(&admitted.run.id, "activation-fault")
            .await
            .unwrap();
        let execution = store.execution(fence).await.unwrap();
        let mut next = reserve(store.session_revision(&admitted.session.id).await.unwrap());
        if let AttemptCommand::Reserve { target } = &mut next.command {
            target.generation = finished.attempt.target.generation.next().unwrap();
        }
        let run = store.get_run(&admitted.run.id).await.unwrap();
        let latest = store
            .latest_execution_attempt(&admitted.run.id)
            .await
            .unwrap();
        let events = store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap();
        inject(point);
        let result = execution.mutate_attempt(next.clone(), None).await;
        inject(super::FaultPoint::Off);
        assert_eq!(result.unwrap_err(), Error::UnknownStoreOutcome);
        let saved = store
            .lookup_attempt_mutation(&admitted.session.id, &next.id)
            .await
            .unwrap();
        if matches!(point, super::FaultPoint::BeforeCommit) {
            assert!(saved.is_none());
            assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), run);
            assert_eq!(
                store
                    .latest_execution_attempt(&admitted.run.id)
                    .await
                    .unwrap(),
                latest
            );
            assert_eq!(
                store
                    .list_events(&admitted.session.id, None, 100)
                    .await
                    .unwrap(),
                events
            );
            assert!(matches!(
                execution.mutate_attempt(next, None).await.unwrap(),
                Outcome::Applied {
                    authority: Some(_),
                    ..
                }
            ));
        } else {
            let saved = saved.unwrap();
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
                    .latest_execution_attempt(&admitted.run.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .attempt,
                saved.attempt
            );
            assert_eq!(
                store.session_revision(&admitted.session.id).await.unwrap(),
                saved.revision
            );
            assert!(
                matches!(execution.mutate_attempt(next,None).await.unwrap(),Outcome::Replayed(receipt) if receipt==saved)
            );
        }
    }
}

async fn legacy_pause_cannot_gain_attempt_authority(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution
        .pause_run(
            json!({}),
            crabber_core::EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: crabber_core::EventKind::RunPaused,
                payload: json!({}),
                correlation: None,
                live_only: false,
                created_at: clock.now(),
            },
        )
        .await
        .unwrap();
    let fence = store
        .claim_expired_run(&admitted.run.id, "legacy")
        .await
        .unwrap();
    let execution = store.execution(fence).await.unwrap();
    let request = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    assert_eq!(
        execution
            .mutate_attempt(request.clone(), None)
            .await
            .unwrap_err(),
        Error::InvalidTransition
    );
    assert!(
        store
            .latest_execution_attempt(&admitted.run.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        request.expected_revision
    );
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Paused
    );
}
