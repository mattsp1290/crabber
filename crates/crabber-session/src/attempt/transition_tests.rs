use super::*;
use crate::{AttemptFinish, AttemptTarget};
use crate::{MemoryStore, public_state_contract};
use crabber_core::{AttemptId, ControlId, SessionRevision};

fn mutation(
    target: &AttemptTarget,
    command: impl FnOnce(AttemptTarget) -> AttemptCommand,
) -> AttemptMutation {
    AttemptMutation {
        id: ControlId::new(),
        expected_revision: SessionRevision::INITIAL,
        command: command(target.clone()),
    }
}
async fn fixture() -> (Run, AttemptMutation, AttemptRecord) {
    let run = public_state_contract::admit(&MemoryStore::new(), OffsetDateTime::UNIX_EPOCH)
        .await
        .run;
    let request = AttemptMutation {
        id: ControlId::new(),
        expected_revision: SessionRevision::INITIAL,
        command: AttemptCommand::Reserve {
            target: AttemptTarget {
                id: AttemptId::new(),
                generation: AttemptGeneration::INITIAL,
            },
        },
    };
    let record = AttemptRecord::reserve(&run, &request, None).unwrap();
    (run, request, record)
}
#[tokio::test]
async fn lifecycle_requires_explicit_begin_finish_and_cleanup() {
    let (run, request, mut record) = fixture().await;
    let authority = record.authority().unwrap();
    let target = request.command.target();
    let begin = mutation(target, |target| AttemptCommand::Begin { target });
    assert_eq!(
        record.change(&run, &begin, None, run.created_at, true),
        Err(AttemptError::StaleOwner)
    );
    assert!(
        !record
            .change(&run, &begin, Some(&authority), run.created_at, true)
            .unwrap()
    );
    assert_eq!(record.snapshot.state, AttemptState::Started);
    assert_eq!(
        record.change(&run, &begin, Some(&authority), run.created_at, true),
        Err(AttemptError::InvalidTransition)
    );
    let finish = mutation(target, |target| AttemptCommand::Finish {
        target,
        outcome: AttemptFinish::Suspended,
    });
    record
        .change(&run, &finish, Some(&authority), run.created_at, true)
        .unwrap();
    assert_eq!(
        AttemptRecord::reserve(&run, &request, Some(&record.snapshot)).err(),
        Some(AttemptError::Busy)
    );
    let complete = mutation(target, |target| AttemptCommand::RecordCleanup {
        target,
        result: AttemptCleanupResult::Complete,
    });
    assert_eq!(
        record.change(&run, &complete, Some(&authority), run.created_at, false),
        Err(AttemptError::CleanupBlocked)
    );
    let incomplete = mutation(target, |target| AttemptCommand::RecordCleanup {
        target,
        result: AttemptCleanupResult::Incomplete,
    });
    record
        .change(&run, &incomplete, Some(&authority), run.created_at, false)
        .unwrap();
    record
        .change(&run, &complete, Some(&authority), run.created_at, true)
        .unwrap();
    let mut next = request;
    if let AttemptCommand::Reserve { target } = &mut next.command {
        target.generation = target.generation.next().unwrap();
        target.id = AttemptId::new();
    }
    assert_eq!(
        AttemptRecord::reserve(&run, &next, Some(&record.snapshot))
            .unwrap()
            .snapshot
            .state,
        AttemptState::Unstarted
    );
}
#[tokio::test]
async fn only_expired_unstarted_attempt_can_rotate_authority() {
    let (mut run, request, mut record) = fixture().await;
    let old = record.authority().unwrap();
    let old_run_token = run.claim_token.clone();
    let reclaim = mutation(request.command.target(), |target| {
        AttemptCommand::ReclaimUnstarted { target }
    });
    assert_eq!(
        record.change(&run, &reclaim, None, run.created_at, true),
        Err(AttemptError::LiveOwner)
    );
    let now = run.lease_until;
    run.claim_token = uuid::Uuid::new_v4().to_string();
    run.lease_until += time::Duration::seconds(30);
    assert!(record.change(&run, &reclaim, None, now, true).unwrap());
    let current = record.authority().unwrap();
    let begin = mutation(request.command.target(), |target| AttemptCommand::Begin {
        target,
    });
    assert_eq!(
        record.change(&run, &begin, Some(&old), now, true),
        Err(AttemptError::StaleOwner)
    );
    record
        .change(&run, &begin, Some(&current), now, true)
        .unwrap();
    assert_eq!(
        record.change(&run, &reclaim, None, run.lease_until, true),
        Err(AttemptError::InvalidTransition)
    );
    assert_eq!(
        verify_run(
            &run,
            &RunFence {
                run_id: run.id.clone(),
                claim_token: old_run_token
            },
            now
        ),
        Err(AttemptError::StaleOwner)
    );
}
#[tokio::test]
async fn unknown_outcome_and_generation_exhaustion_never_open_next_attempt() {
    let (run, request, mut record) = fixture().await;
    let authority = record.authority().unwrap();
    let target = request.command.target();
    let begin = mutation(target, |target| AttemptCommand::Begin { target });
    record
        .change(&run, &begin, Some(&authority), run.created_at, true)
        .unwrap();
    let finish = mutation(target, |target| AttemptCommand::Finish {
        target,
        outcome: AttemptFinish::OutcomeUnknown,
    });
    record
        .change(&run, &finish, Some(&authority), run.created_at, true)
        .unwrap();
    let cleanup = mutation(target, |target| AttemptCommand::RecordCleanup {
        target,
        result: AttemptCleanupResult::Complete,
    });
    record
        .change(&run, &cleanup, Some(&authority), run.created_at, true)
        .unwrap();
    assert_eq!(
        AttemptRecord::reserve(&run, &request, Some(&record.snapshot)).err(),
        Some(AttemptError::Busy)
    );
    record.snapshot.state = AttemptState::Suspended;
    record.snapshot.target.generation = AttemptGeneration::new(i64::MAX as u64).unwrap();
    assert_eq!(
        AttemptRecord::reserve(&run, &request, Some(&record.snapshot)).err(),
        Some(AttemptError::GenerationExhausted)
    );
    assert!(!format!("{authority:?}").contains(&record.token));
}

#[tokio::test]
async fn only_suspended_clean_attempts_can_continue_and_pause_blocks_new_work() {
    let (mut run, request, mut record) = fixture().await;
    for state in [
        AttemptState::Completed,
        AttemptState::Failed,
        AttemptState::Interrupted,
    ] {
        record.snapshot.state = state;
        record.snapshot.cleanup = AttemptCleanup::Complete;
        assert_eq!(
            AttemptRecord::reserve(&run, &request, Some(&record.snapshot)).err(),
            Some(AttemptError::InvalidTransition)
        );
    }
    record.snapshot.state = AttemptState::Unstarted;
    record.snapshot.cleanup = AttemptCleanup::Pending;
    let authority = record.authority().unwrap();
    run.status = RunStatus::Paused;
    assert_eq!(
        AttemptRecord::reserve(&run, &request, None).err(),
        Some(AttemptError::InvalidTransition)
    );
    let begin = mutation(request.command.target(), |target| AttemptCommand::Begin {
        target,
    });
    assert_eq!(
        record.change(&run, &begin, Some(&authority), run.created_at, true),
        Err(AttemptError::InvalidTransition)
    );
    let reclaim = mutation(request.command.target(), |target| {
        AttemptCommand::ReclaimUnstarted { target }
    });
    assert_eq!(
        record.change(&run, &reclaim, None, run.lease_until, true),
        Err(AttemptError::InvalidTransition)
    );
    record.snapshot.state = AttemptState::Suspended;
    let cleanup = mutation(request.command.target(), |target| {
        AttemptCommand::RecordCleanup {
            target,
            result: AttemptCleanupResult::Complete,
        }
    });
    record
        .change(&run, &cleanup, Some(&authority), run.created_at, true)
        .unwrap();
}
