use super::{Fixture, Mode};
use crate::{ApprovalClaimOutcome, AttemptCleanup, AttemptState, Store};
use crabber_core::RunStatus;

pub(crate) async fn uncommitted(store: &impl Store, f: &Fixture) {
    assert_eq!(
        store.lookup_approval(&f.session, f.key()).await.unwrap(),
        None
    );
    assert_eq!(
        store.approval_snapshot(&f.session, &f.run).await.unwrap(),
        f.before
    );
    let reserve_and_begin = if matches!(f.mode, Mode::Suspend) {
        2
    } else {
        0
    };
    let mut revision = f.revision;
    for _ in 0..reserve_and_begin {
        revision = revision.next().unwrap();
    }
    assert_eq!(store.session_revision(&f.session).await.unwrap(), revision);
    assert_eq!(
        store
            .list_events(&f.session, None, 1000)
            .await
            .unwrap()
            .len(),
        f.events + reserve_and_begin
    );
    let run = store.get_run(&f.run).await.unwrap().unwrap();
    assert_eq!(run.claim_token, f.fence.claim_token);
    assert_eq!(
        run.status,
        if matches!(f.mode, Mode::Suspend) {
            RunStatus::Running
        } else {
            RunStatus::Paused
        }
    );
    let attempt = store
        .latest_execution_attempt(&f.run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(attempt.target, f.batch.attempt);
    assert_eq!(
        attempt.state,
        if matches!(f.mode, Mode::Suspend) {
            AttemptState::Started
        } else {
            AttemptState::Suspended
        }
    );
    assert_eq!(
        attempt.cleanup,
        if matches!(f.mode, Mode::Suspend) {
            AttemptCleanup::Pending
        } else {
            AttemptCleanup::Complete
        }
    );
}
pub(crate) async fn verify_loss(store: &impl Store, f: &Fixture) {
    let receipt = store
        .lookup_approval(&f.session, f.key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.session_revision(&f.session).await.unwrap(),
        receipt.revision
    );
    let snapshot = store
        .approval_snapshot(&f.session, &f.run)
        .await
        .unwrap()
        .unwrap();
    let run = store.get_run(&f.run).await.unwrap().unwrap();
    match f.mode {
        Mode::Suspend => {
            assert_eq!(run.status, RunStatus::Paused);
            assert!(snapshot.interactions.iter().all(|q| !q.consumed));
            let attempt = store
                .latest_execution_attempt(&f.run)
                .await
                .unwrap()
                .unwrap()
                .attempt;
            assert_eq!(attempt.state, AttemptState::Suspended);
            assert_eq!(attempt.cleanup, AttemptCleanup::Complete);
        }
        Mode::Answer => {
            assert_eq!(run.status, RunStatus::Paused);
            assert_eq!(
                snapshot.interactions[0].state,
                crabber_core::InteractionState::Answered
            );
            assert_eq!(
                store
                    .answer_approval(f.answer.clone().unwrap())
                    .await
                    .unwrap(),
                receipt
            );
        }
        Mode::Claim => {
            assert_eq!(run.status, RunStatus::Running);
            assert!(snapshot.interactions.iter().all(|q| q.consumed));
            let attempt = store
                .latest_execution_attempt(&f.run)
                .await
                .unwrap()
                .unwrap()
                .attempt;
            assert_eq!(attempt.target, f.batch.next_attempt);
            assert_eq!(attempt.state, AttemptState::Unstarted);
            assert!(
                matches!(store.claim_approval_continuation(f.claim.clone().unwrap()).await.unwrap(),
                ApprovalClaimOutcome::Replayed(saved) if saved == receipt)
            );
        }
    }
    assert_eq!(
        store.session_revision(&f.session).await.unwrap(),
        receipt.revision
    );
}
