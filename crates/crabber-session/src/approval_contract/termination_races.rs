//! Same-revision contenders cannot both answer/claim and terminate a barrier.
use super::fixture::{answer, claim, staged};
use crate::{ApprovalClaimOutcome, ApprovalError, ApprovalReply, Store};
use crabber_core::{Clock, ManualClock, RunStatus};

pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    answer_race(store, clock).await;
    claim_race(store, clock).await;
    mixed_deadlines(store, clock).await;
}
async fn answer_race(store: &impl Store, clock: &ManualClock) {
    let f = staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let cancellation = super::termination::request(store, &f, false).await;
    let answer = answer(store, &f, 0, ApprovalReply::Deny).await;
    let (cancelled, answered) = futures::join!(
        store.terminate_approval(cancellation.clone()),
        store.answer_approval(answer)
    );
    assert_ne!(cancelled.is_ok(), answered.is_ok());
    let snapshot = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap()
        .unwrap();
    if let Err(error) = cancelled {
        assert_eq!(error, ApprovalError::RevisionConflict);
        assert!(snapshot.termination.is_none());
        let mut retry = cancellation;
        retry.expected_revision = snapshot.revision;
        store.terminate_approval(retry).await.unwrap();
    } else {
        assert_eq!(answered.unwrap_err(), ApprovalError::RevisionConflict);
        assert!(snapshot.termination.is_some());
    }
}
async fn claim_race(store: &impl Store, clock: &ManualClock) {
    let f = staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_approval(answer(store, &f, index, ApprovalReply::Grant).await)
            .await
            .unwrap();
    }
    let cancellation = super::termination::request(store, &f, false).await;
    let claim = claim(store, &f).await;
    let (cancelled, claimed) = futures::join!(
        store.terminate_approval(cancellation),
        store.claim_approval_continuation(claim)
    );
    assert_ne!(cancelled.is_ok(), claimed.is_ok());
    let run = store.get_run(&f.run.id).await.unwrap().unwrap();
    let snapshot = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap()
        .unwrap();
    if let Err(error) = cancelled {
        assert!(matches!(
            claimed.unwrap(),
            ApprovalClaimOutcome::Applied { .. }
        ));
        assert_eq!(error, ApprovalError::RevisionConflict);
        assert_eq!(run.status, RunStatus::Running);
        assert!(snapshot.interactions.iter().all(|q| q.consumed));
    } else {
        assert_eq!(claimed.unwrap_err(), ApprovalError::RevisionConflict);
        assert_eq!(run.status, RunStatus::Interrupted);
        assert!(snapshot.interactions.iter().all(|q| !q.consumed));
    }
}
async fn mixed_deadlines(store: &impl Store, clock: &ManualClock) {
    let mut f = staged(store, clock).await;
    let crate::CallApproval::Ask { description, .. } = &mut f.batch.calls[0].decision else {
        panic!("question")
    };
    *description = crabber_core::InteractionDescription::new(
        "approve",
        crabber_core::InteractionKind::Approval,
        "PRIVATE_PROMPT",
        serde_json::json!({"type":"boolean"}),
        description.authority().clone(),
        None,
    )
    .unwrap();
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    clock.set(clock.now() + time::Duration::seconds(2));
    let request = super::termination::request(store, &f, true).await;
    store.terminate_approval(request).await.unwrap();
    let snapshot = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.interactions[0].state,
        crabber_core::InteractionState::Cancelled
    );
    assert_eq!(
        snapshot.interactions[1].state,
        crabber_core::InteractionState::Expired
    );
}
