//! Reusable explicit native checkpoint conformance; no backend assumes dispatch
//! from metadata replay or from an unresolved Started phase.
#[cfg(test)]
#[path = "tool_continuation_contract/corruption.rs"]
pub(crate) mod corruption;
#[path = "tool_continuation_contract/faults.rs"]
mod faults;
#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "tool_continuation_contract/process.rs"]
pub(crate) mod process;
#[path = "tool_continuation_contract/race.rs"]
mod race;
#[path = "tool_continuation_contract/recovery.rs"]
mod recovery;
#[path = "tool_continuation_contract/rejections.rs"]
mod rejections;
pub use faults::run_fault_contract;
#[path = "tool_continuation_contract/fixture.rs"]
pub(crate) mod fixture;
use crate::{
    PhaseQuiescence, PhaseState, Store, ToolContinuationClaimOutcome, ToolContinuationError,
    ToolContinuationState,
};
use crabber_core::{ManualClock, RunStatus, ToolCallStatus};
use std::sync::Arc;
use time::OffsetDateTime;
/// # Panics
/// Panics when an adapter violates the explicit checkpoint contract.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    lifecycle(&store).await;
    race::run(&store).await;
    recovery::run(&store, &clock).await;
    rejections::run(&store, &clock).await;
}
#[allow(clippy::too_many_lines)] // One complete native lifecycle fixture/journey.
pub(crate) async fn lifecycle(store: &impl Store) -> fixture::Fixture {
    let f = fixture::fixture(store).await;
    let receipt = f
        .execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    assert_eq!(
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .original_tool_continuation_plan(
                &f.admitted.session.id,
                &f.admitted.run.id,
                &f.suspension.continuation
            )
            .await
            .unwrap(),
        f.suspension.expected_plan
    );
    let session = &f.admitted.session.id;
    let run = &f.admitted.run.id;
    let snapshot = store
        .tool_continuation_snapshot(session, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, ToolContinuationState::Waiting);
    assert_eq!(snapshot.interactions.len(), 2);
    assert_eq!(
        store.get_run(run).await.unwrap().unwrap().status,
        RunStatus::Paused
    );
    let old = &f.suspension.phases[0].previous;
    let phase = store.tool_phase(run, &old.id).await.unwrap().phase;
    assert_eq!(
        (phase.state, phase.quiescence),
        (PhaseState::Suspended, PhaseQuiescence::Complete)
    );
    assert_eq!(
        store.list_unfinished_tool_calls(run).await.unwrap()[0].status,
        ToolCallStatus::Waiting
    );
    assert_eq!(
        store
            .claim_tool_continuation(fixture::claim(store, &f).await)
            .await
            .unwrap_err(),
        ToolContinuationError::Pending
    );
    let first = fixture::answer(store, &f, 0).await;
    let answered = store.answer_tool_interaction(first.clone()).await.unwrap();
    assert_eq!(
        store.answer_tool_interaction(first.clone()).await.unwrap(),
        answered
    );
    let mut changed = first.clone();
    changed.reply = crate::ToolInteractionReply::Deny;
    assert_eq!(
        store.answer_tool_interaction(changed).await.unwrap_err(),
        ToolContinuationError::SemanticConflict
    );
    assert_eq!(
        store
            .claim_tool_continuation(fixture::claim(store, &f).await)
            .await
            .unwrap_err(),
        ToolContinuationError::Pending
    );
    store
        .answer_tool_interaction(fixture::answer(store, &f, 1).await)
        .await
        .unwrap();
    let claim = fixture::claim(store, &f).await;
    let ToolContinuationClaimOutcome::Applied {
        receipt: claimed,
        grant,
    } = store.claim_tool_continuation(claim.clone()).await.unwrap()
    else {
        panic!("fresh claim")
    };
    assert_eq!(
        grant.phases[0].checkpoint,
        f.suspension.phases[0].checkpoint
    );
    assert_eq!(grant.phases[0].responses.len(), 2);
    assert!(
        grant.phases[0]
            .responses
            .iter()
            .all(|a| a.state == crabber_core::InteractionState::Answered)
    );
    assert!(
        matches!(store.claim_tool_continuation(claim).await.unwrap(),ToolContinuationClaimOutcome::Replayed(actual) if actual==claimed)
    );
    assert_eq!(
        store.answer_tool_interaction(first).await.unwrap(),
        answered
    );
    assert!(
        store
            .tool_continuation_snapshot(session, run)
            .await
            .unwrap()
            .unwrap()
            .interactions
            .iter()
            .all(|q| q.consumed)
    );
    let execution = store.execution(grant.fence.clone()).await.unwrap();
    execution
        .mutate_attempt(
            crate::AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: claimed.revision,
                command: crate::AttemptCommand::Begin {
                    target: f.suspension.next_attempt.clone(),
                },
            },
            Some(grant.authority.clone()),
        )
        .await
        .unwrap();
    let resumed = &grant.phases[0];
    execution
        .start_tool_phase(
            crate::PhaseStart {
                begin: crate::PhaseMutation {
                    id: crabber_core::ControlId::new(),
                    expected_revision: store.session_revision(session).await.unwrap(),
                    command: crate::PhaseCommand::Begin {
                        target: resumed.target.clone(),
                    },
                },
                turn_id: f.suspension.source_turn_id.clone(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            Some(resumed.authority.clone()),
        )
        .await
        .unwrap();
    let output = crabber_core::ToolOutput::from_json(&serde_json::json!({"done":true})).unwrap();
    execution
        .settle_tool_phase(
            crate::PhaseSettlement {
                completion: crate::PhaseMutation {
                    id: crabber_core::ControlId::new(),
                    expected_revision: store.session_revision(session).await.unwrap(),
                    command: crate::PhaseCommand::Complete {
                        target: resumed.target.clone(),
                        output,
                    },
                },
                result_status: crabber_core::ToolResultStatus::Completed,
                message_id: crabber_core::MessageId::new(),
                part_id: crabber_core::PartId::new(),
                turn_id: f.suspension.source_turn_id.clone(),
                created_at: OffsetDateTime::UNIX_EPOCH,
                duration_ms: 0,
            },
            Some(resumed.authority.clone()),
        )
        .await
        .unwrap();
    execution
        .conclude_attempt(
            crate::AttemptConclusion {
                finish: crate::AttemptMutation {
                    id: crabber_core::ControlId::new(),
                    expected_revision: store.session_revision(session).await.unwrap(),
                    command: crate::AttemptCommand::Finish {
                        target: f.suspension.next_attempt.clone(),
                        outcome: crate::AttemptFinish::Completed,
                    },
                },
                boundary: crate::RunBoundary::Terminal {
                    status: RunStatus::Completed,
                    error: None,
                    usage: f.suspension.usage.clone(),
                },
                turn_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                elapsed: std::time::Duration::ZERO,
            },
            Some(grant.authority),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .tool_continuation_snapshot(session, run)
            .await
            .unwrap()
            .unwrap()
            .state,
        ToolContinuationState::Released
    );
    assert_eq!(
        store
            .lookup_tool_continuation(session, &f.suspension.id)
            .await
            .unwrap(),
        Some(receipt)
    );
    f
}
