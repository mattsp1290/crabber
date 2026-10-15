//! Every control boundary is reconciled by its original key after response loss.
use super::fixture::{Fixture, answer, claim, staged};
pub use crate::public_state_contract::FaultPoint;
use crate::{
    ApprovalAnswer, ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalError as Error,
    ApprovalReceipt, ApprovalReply, Store,
};
use crabber_core::{ControlId, ManualClock};

#[derive(Clone, Copy)]
enum Case {
    Suspend,
    Answer,
    Claim,
}
enum Operation {
    Suspend,
    Answer(ApprovalAnswer),
    Claim(ApprovalContinuationClaim),
}
impl Operation {
    fn id(&self, f: &Fixture) -> ControlId {
        match self {
            Self::Suspend => f.batch.id.clone(),
            Self::Answer(a) => a.id.clone(),
            Self::Claim(c) => c.id.clone(),
        }
    }
    async fn execute(&self, store: &impl Store, f: &Fixture) -> Result<ApprovalReceipt, Error> {
        match self {
            Self::Suspend => {
                f.execution
                    .suspend_for_approval(f.batch.clone(), f.owner.clone())
                    .await
            }
            Self::Answer(a) => store.answer_approval(a.clone()).await,
            Self::Claim(c) => match store.claim_approval_continuation(c.clone()).await? {
                ApprovalClaimOutcome::Applied { receipt, .. }
                | ApprovalClaimOutcome::Replayed(receipt) => Ok(receipt),
            },
        }
    }
}
/// Fault suspension, answer and complete-set claim before and after COMMIT.
/// # Panics
/// Panics if an adapter publishes only part of a control or replay grants ownership.
pub async fn run_fault_contract(
    store: &impl Store,
    clock: &ManualClock,
    inject: impl Fn(FaultPoint),
) {
    super::recovery::fault_contract(store, clock, &inject).await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    for case in [Case::Suspend, Case::Answer, Case::Claim] {
        for point in [FaultPoint::BeforeCommit, FaultPoint::AfterCommit] {
            let (f, operation) = pending(store, clock, case).await;
            let id = operation.id(&f);
            let before_run = store.get_run(&f.run.id).await.unwrap();
            let before_attempt = store.latest_execution_attempt(&f.run.id).await.unwrap();
            let before = store
                .approval_snapshot(&f.run.session_id, &f.run.id)
                .await
                .unwrap();
            let before_revision = store.session_revision(&f.run.session_id).await.unwrap();
            let before_events = store
                .list_events(&f.run.session_id, None, 1000)
                .await
                .unwrap();
            inject(point);
            assert_eq!(
                operation.execute(store, &f).await.unwrap_err(),
                Error::UnknownStoreOutcome
            );
            inject(FaultPoint::Off);
            let saved = store.lookup_approval(&f.run.session_id, &id).await.unwrap();
            if matches!(point, FaultPoint::BeforeCommit) {
                assert_eq!(saved, None);
                assert_eq!(store.get_run(&f.run.id).await.unwrap(), before_run);
                assert_eq!(
                    store.latest_execution_attempt(&f.run.id).await.unwrap(),
                    before_attempt
                );
                assert_eq!(
                    store
                        .approval_snapshot(&f.run.session_id, &f.run.id)
                        .await
                        .unwrap(),
                    before
                );
                assert_eq!(
                    store.session_revision(&f.run.session_id).await.unwrap(),
                    before_revision
                );
                assert_eq!(
                    store
                        .list_events(&f.run.session_id, None, 1000)
                        .await
                        .unwrap(),
                    before_events
                );
                operation.execute(store, &f).await.unwrap();
            } else {
                let saved =
                    saved.expect("the full operation committed before its acknowledgment was lost");
                let revision = store.session_revision(&f.run.session_id).await.unwrap();
                let events = store
                    .list_events(&f.run.session_id, None, 1000)
                    .await
                    .unwrap();
                assert_eq!(operation.execute(store, &f).await.unwrap(), saved);
                if let Operation::Claim(claim) = &operation {
                    assert!(
                        matches!(store.claim_approval_continuation(claim.clone()).await.unwrap(), ApprovalClaimOutcome::Replayed(receipt) if receipt == saved)
                    );
                }
                assert_eq!(
                    store.session_revision(&f.run.session_id).await.unwrap(),
                    revision
                );
                assert_eq!(
                    store
                        .list_events(&f.run.session_id, None, 1000)
                        .await
                        .unwrap(),
                    events
                );
            }
        }
    }
}
async fn pending(store: &impl Store, clock: &ManualClock, case: Case) -> (Fixture, Operation) {
    let f = staged(store, clock).await;
    if matches!(case, Case::Suspend) {
        return (f, Operation::Suspend);
    }
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    if matches!(case, Case::Answer) {
        let answer = answer(store, &f, 0, ApprovalReply::Grant).await;
        return (f, Operation::Answer(answer));
    }
    for index in 0..2 {
        store
            .answer_approval(answer(store, &f, index, ApprovalReply::Grant).await)
            .await
            .unwrap();
    }
    let claim = claim(store, &f).await;
    (f, Operation::Claim(claim))
}
