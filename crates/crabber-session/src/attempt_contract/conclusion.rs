//! Native conclusion guards and exact retry semantics, independent of dispatch.
#[path = "conclusion/guards.rs"]
mod guards;
use super::{applied, reserve};
use crate::{
    AttemptAuthority, AttemptCleanup, AttemptCommand, AttemptConclusion, AttemptError as Error,
    AttemptFinish, AttemptMutation, AttemptMutationOutcome as Outcome, AttemptState,
    ExecutionStore, RunBoundary, Store,
};
use crabber_core::{Clock, ControlId, EventKind, ManualClock, RunStatus, Usage};
use serde_json::json;

async fn started(
    store: &impl Store,
    clock: &ManualClock,
    boundary: RunBoundary,
) -> (
    crate::AdmitOutcome,
    Box<dyn ExecutionStore>,
    AttemptAuthority,
    AttemptConclusion,
) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
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
    let outcome = match &boundary {
        RunBoundary::Terminal {
            status: RunStatus::Completed,
            ..
        } => AttemptFinish::Completed,
        RunBoundary::Terminal {
            status: RunStatus::Failed,
            ..
        } => AttemptFinish::Failed,
        RunBoundary::Terminal {
            status: RunStatus::Interrupted,
            ..
        } => AttemptFinish::Interrupted,
        RunBoundary::Suspended { .. } => AttemptFinish::Suspended,
        RunBoundary::Terminal { .. } => panic!("invalid fixture status"),
    };
    let conclusion = AttemptConclusion {
        finish: AttemptMutation {
            id: ControlId::new(),
            expected_revision: begun.revision,
            command: AttemptCommand::Finish {
                target: begun.attempt.target,
                outcome,
            },
        },
        boundary,
        created_at: clock.now(),
        turn_id: None,
        elapsed: std::time::Duration::from_nanos(25_123_456),
    };
    (admitted, execution, owner, conclusion)
}
fn terminal(status: RunStatus) -> RunBoundary {
    RunBoundary::Terminal {
        status,
        error: if status == RunStatus::Failed {
            Some("PRIVATE_FAILURE".into())
        } else {
            None
        },
        usage: Usage {
            input_tokens: 13,
            output_tokens: 5,
        },
    }
}
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    for boundary in [
        terminal(RunStatus::Completed),
        terminal(RunStatus::Failed),
        terminal(RunStatus::Interrupted),
        RunBoundary::Suspended {
            checkpoint: json!({"private":"PRIVATE_CHECKPOINT"}),
            usage: Usage {
                input_tokens: 17,
                output_tokens: 7,
            },
        },
    ] {
        lifecycle(store, clock, boundary).await;
    }
    pending_input(store, clock).await;
    contradictory_boundary(store, clock).await;
    guards::run(store).await;
}
async fn lifecycle(store: &impl Store, clock: &ManualClock, boundary: RunBoundary) {
    let (admitted, execution, owner, conclusion) = started(store, clock, boundary).await;
    let before = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap()
        .len();
    let (receipt, authority) = applied(
        execution
            .conclude_attempt(conclusion.clone(), Some(owner))
            .await
            .unwrap(),
    );
    assert!(authority.is_none());
    assert_eq!(receipt.attempt.cleanup, AttemptCleanup::Complete);
    assert_eq!(
        store
            .execution_attempt(&admitted.run.id, &receipt.attempt.target.id)
            .await
            .unwrap()
            .attempt,
        receipt.attempt
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        receipt.revision
    );
    let run = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    match &conclusion.boundary {
        RunBoundary::Terminal {
            status,
            error,
            usage,
        } => {
            assert_eq!(&run.error, error);
            assert_eq!(run.status, *status);
            assert_eq!(&run.usage, usage);
        }
        RunBoundary::Suspended { checkpoint, usage } => {
            assert_eq!(run.status, RunStatus::Paused);
            assert_eq!(run.checkpoint.as_ref(), Some(checkpoint));
            assert_eq!(&run.usage, usage);
        }
    }
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), before + 2);
    let audit = events
        .iter()
        .find(|event| event.cursor == Some(receipt.event_cursor))
        .unwrap();
    assert_eq!(audit.kind, EventKind::ExecutionAttemptChanged);
    assert_eq!(audit.payload["version"], 2);
    if let Some(source) = events
        .iter()
        .find(|event| event.kind == EventKind::RunSettled)
    {
        assert_eq!(source.payload["duration_ns"], 25_123_456_u64);
    }

    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("PRIVATE_CHECKPOINT")
    );
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("PRIVATE_FAILURE")
    );
    let mut retry = conclusion.clone();
    retry.elapsed += std::time::Duration::from_nanos(1);
    assert!(
        matches!(execution.conclude_attempt(retry, None).await.unwrap(), Outcome::Replayed(saved) if saved==receipt)
    );
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap().unwrap(), run);
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap(),
        events
    );
    conflicting_choices(execution.as_ref(), conclusion).await;
}
async fn conflicting_choices(execution: &dyn ExecutionStore, conclusion: AttemptConclusion) {
    let mut changed = conclusion.clone();
    changed.boundary = terminal(RunStatus::Failed);
    if let AttemptCommand::Finish { outcome, .. } = &mut changed.finish.command {
        *outcome = AttemptFinish::Failed;
    }
    if changed.boundary == conclusion.boundary {
        changed.boundary = terminal(RunStatus::Completed);
        if let AttemptCommand::Finish { outcome, .. } = &mut changed.finish.command {
            *outcome = AttemptFinish::Completed;
        }
    }
    assert_eq!(
        execution.conclude_attempt(changed, None).await.unwrap_err(),
        Error::SemanticConflict
    );
    assert_eq!(
        execution
            .mutate_attempt(conclusion.finish, None)
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
}

async fn pending_input(store: &impl Store, clock: &ManualClock) {
    let (admitted, execution, owner, mut conclusion) =
        started(store, clock, terminal(RunStatus::Completed)).await;
    let mut message = store
        .list_messages(&admitted.session.id, None)
        .await
        .unwrap()[0]
        .clone();
    message.id = crabber_core::MessageId::new();
    message.run_id = None;
    for part in &mut message.parts {
        part.id = crabber_core::PartId::new();
        part.message_id = message.id.clone();
    }
    store
        .enqueue_inbox(&admitted.session.id, crate::InboxKind::FollowUp, message)
        .await
        .unwrap();
    conclusion.finish.expected_revision =
        store.session_revision(&admitted.session.id).await.unwrap();
    let original = store.get_run(&admitted.run.id).await.unwrap();
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    assert_eq!(
        execution
            .conclude_attempt(conclusion.clone(), Some(owner.clone()))
            .await
            .unwrap_err(),
        Error::PendingInput
    );
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), original);
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        conclusion.finish.expected_revision
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap(),
        events
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
    assert!(
        store
            .lookup_attempt_mutation(&admitted.session.id, &conclusion.finish.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        execution
            .claim_inbox_into_history(crate::InboxKind::FollowUp)
            .await
            .unwrap()
            .len(),
        1
    );
    conclusion.finish.id = ControlId::new();
    conclusion.finish.expected_revision =
        store.session_revision(&admitted.session.id).await.unwrap();
    execution
        .conclude_attempt(conclusion, Some(owner))
        .await
        .unwrap();
}
async fn contradictory_boundary(store: &impl Store, clock: &ManualClock) {
    let (admitted, execution, owner, mut conclusion) =
        started(store, clock, terminal(RunStatus::Completed)).await;
    conclusion.boundary = RunBoundary::Suspended {
        checkpoint: json!({}),
        usage: Usage::default(),
    };
    assert_eq!(
        execution
            .conclude_attempt(conclusion.clone(), Some(owner))
            .await
            .unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        conclusion.finish.expected_revision
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
}

pub(super) async fn faults(
    store: &impl Store,
    clock: &ManualClock,
    inject: &impl Fn(super::FaultPoint),
) {
    for boundary in [
        terminal(RunStatus::Completed),
        RunBoundary::Suspended {
            checkpoint: json!({"private":"PRIVATE_CHECKPOINT"}),
            usage: Usage::default(),
        },
    ] {
        for point in [
            super::FaultPoint::BeforeCommit,
            super::FaultPoint::AfterCommit,
        ] {
            let (admitted, execution, owner, conclusion) =
                started(store, clock, boundary.clone()).await;
            let before_run = store.get_run(&admitted.run.id).await.unwrap();
            let before_attempt = store
                .execution_attempt(&admitted.run.id, &conclusion.finish.command.target().id)
                .await
                .unwrap();
            let events = store
                .list_events(&admitted.session.id, None, 100)
                .await
                .unwrap();
            inject(point);
            let result = execution
                .conclude_attempt(conclusion.clone(), Some(owner.clone()))
                .await;
            inject(super::FaultPoint::Off);
            assert_eq!(result.unwrap_err(), Error::UnknownStoreOutcome);
            let receipt = store
                .lookup_attempt_mutation(&admitted.session.id, &conclusion.finish.id)
                .await
                .unwrap();
            if matches!(point, super::FaultPoint::BeforeCommit) {
                assert!(receipt.is_none());
                assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), before_run);
                assert_eq!(
                    store
                        .execution_attempt(&admitted.run.id, &conclusion.finish.command.target().id)
                        .await
                        .unwrap(),
                    before_attempt
                );
                assert_eq!(
                    store
                        .list_events(&admitted.session.id, None, 100)
                        .await
                        .unwrap(),
                    events
                );
                assert!(matches!(
                    execution
                        .conclude_attempt(conclusion, Some(owner))
                        .await
                        .unwrap(),
                    Outcome::Applied {
                        authority: None,
                        ..
                    }
                ));
            } else {
                let receipt = receipt.unwrap();
                assert_eq!(
                    store.session_revision(&admitted.session.id).await.unwrap(),
                    receipt.revision
                );
                assert_eq!(
                    store
                        .list_events(&admitted.session.id, None, 100)
                        .await
                        .unwrap()
                        .len(),
                    events.len() + 2
                );
                assert!(
                    matches!(execution.conclude_attempt(conclusion,None).await.unwrap(),Outcome::Replayed(saved) if saved==receipt)
                );
            }
        }
    }
}
