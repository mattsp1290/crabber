//! Store-adapter qualification for durable pre-effect approvals and single claims.
#[path = "approval_contract/changed_batch.rs"]
mod changed_batch;
#[path = "approval_contract/faults.rs"]
mod faults;
#[path = "approval_contract/finish.rs"]
mod finish;
#[path = "approval_contract/fixture.rs"]
pub(crate) mod fixture;
#[path = "approval_contract/integrity.rs"]
mod integrity;
#[path = "approval_contract/presentation.rs"]
mod presentation;
#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "approval_contract/process.rs"]
pub(crate) mod process;
use crate::{
    ApprovalClaimOutcome, ApprovalError as Error, ApprovalReply, AttemptCommand, AttemptError,
    AttemptMutation, AttemptMutationOutcome, Store,
};
use crabber_core::{Clock, ControlId, ManualClock, RunStatus};
pub use faults::run_fault_contract;
use fixture::{answer, claim, staged};
use std::sync::Arc;

/// Answers are distinct from a one-shot continuation grant. No tool is invoked
/// by this harness: counted business work belongs in runtime integration tests.
/// # Panics
/// Panics when an adapter violates atomic scope, replay or ownership semantics.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    lifecycle(&store, &clock).await;
    integrity::run(&store, &clock).await;
    changed_batch::run(&store, &clock).await;
    expiration(&store, &clock).await;
}
async fn lifecycle(store: &impl Store, clock: &ManualClock) {
    let mut f = staged(store, clock).await;
    let suspended = presentation::suspend(store, clock, &mut f).await;
    let run = store.get_run(&f.run.id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Paused);
    assert!(matches!(
        suspended.outcome,
        crate::ApprovalOutcome::Suspended { .. }
    ));
    assert_eq!(
        store
            .latest_execution_attempt(&run.id)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .cleanup,
        crate::AttemptCleanup::Complete
    );
    let snapshot = store
        .approval_snapshot(&run.session_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.calls.len(), 3);
    assert_eq!(snapshot.interactions.len(), 2);
    assert!(!format!("{snapshot:?}").contains("PRIVATE_PROMPT"));
    assert_eq!(
        f.execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap(),
        suspended
    );
    let (first, first_receipt) = resolved_set(store, &f).await;
    let (receipt, grant) = single_claim(store, &f).await;
    assert_eq!(
        store.answer_approval(first.clone()).await.unwrap(),
        first_receipt
    );
    assert_eq!(
        store
            .approval_snapshot(&run.session_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .interactions
            .iter()
            .filter(|q| q.consumed)
            .count(),
        2
    );
    assert_eq!(
        store.get_run(&run.id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    assert!(
        f.execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: receipt.revision,
                    command: AttemptCommand::Begin {
                        target: f.batch.next_attempt.clone()
                    }
                },
                Some(f.owner.clone())
            )
            .await
            .is_err()
    );
    let execution = store.execution(grant.fence.clone()).await.unwrap();
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: f.batch.next_attempt.clone(),
        },
    };
    assert!(matches!(
        execution
            .mutate_attempt(begin, Some(grant.authority.clone()))
            .await
            .unwrap(),
        AttemptMutationOutcome::Applied { .. }
    ));
    finish::complete(store, clock, &f, execution.as_ref(), &grant).await;
    assert_eq!(store.answer_approval(first).await.unwrap(), first_receipt);
}
async fn expiration(store: &impl Store, clock: &ManualClock) {
    let f = staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    clock.set(clock.now() + time::Duration::seconds(2));
    assert_eq!(
        store
            .answer_approval(answer(store, &f, 0, ApprovalReply::Grant).await)
            .await
            .unwrap_err(),
        Error::Expired
    );
    assert_eq!(
        store
            .claim_approval_continuation(claim(store, &f).await)
            .await
            .unwrap_err(),
        Error::Pending
    );
    // Raw reservation cannot bypass the durable barrier after a fresh run claim.
    let fence = store.claim_expired_run(&f.run.id, "bypass").await.unwrap();
    let execution = store.execution(fence).await.unwrap();
    let reserve = AttemptMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        command: AttemptCommand::Reserve {
            target: f.batch.next_attempt.clone(),
        },
    };
    assert_eq!(
        execution.mutate_attempt(reserve, None).await.unwrap_err(),
        AttemptError::Busy
    );
}

async fn resolved_set(
    store: &impl Store,
    f: &fixture::Fixture,
) -> (crate::ApprovalAnswer, crate::ApprovalReceipt) {
    let first = answer(store, f, 0, ApprovalReply::Grant).await;
    let mut forged = first.clone();
    forged.resolver.authority = crabber_core::AuthorityRequirement::new("forged", "v1").unwrap();
    assert_eq!(
        store.answer_approval(forged).await.unwrap_err(),
        Error::AuthorityMismatch
    );
    let first_receipt = store.answer_approval(first.clone()).await.unwrap();
    let partial = claim(store, f).await;
    assert_eq!(
        store
            .claim_approval_continuation(partial)
            .await
            .unwrap_err(),
        Error::Pending
    );
    assert_eq!(
        store.get_run(&f.run.id).await.unwrap().unwrap().status,
        RunStatus::Paused
    );
    let mut changed = first.clone();
    changed.reply = ApprovalReply::Deny;
    assert_eq!(
        store.answer_approval(changed).await.unwrap_err(),
        Error::SemanticConflict
    );
    let second = answer(store, f, 1, ApprovalReply::Deny).await;
    store.answer_approval(second).await.unwrap();

    (first, first_receipt)
}

async fn single_claim(
    store: &impl Store,
    f: &fixture::Fixture,
) -> (crate::ApprovalReceipt, Box<crate::ApprovalGrant>) {
    let request = claim(store, f).await;
    let mut competing = request.clone();
    competing.id = ControlId::new();
    competing.owner = "other".into();
    let (left, right) = futures::join!(
        store.claim_approval_continuation(request.clone()),
        store.claim_approval_continuation(competing.clone())
    );
    let (receipt, grant, winner) = match (left, right) {
        (Ok(ApprovalClaimOutcome::Applied { receipt, grant }), Err(Error::RevisionConflict)) => {
            (receipt, grant, request)
        }
        (Err(Error::RevisionConflict), Ok(ApprovalClaimOutcome::Applied { receipt, grant })) => {
            (receipt, grant, competing)
        }
        other => panic!("one complete response-set claim must win: {other:?}"),
    };
    assert_eq!(
        grant
            .calls
            .iter()
            .map(|call| call.decision)
            .collect::<Vec<_>>(),
        vec![
            crate::GrantedDecision::Allow,
            crate::GrantedDecision::Deny,
            crate::GrantedDecision::Allow
        ]
    );
    assert!(
        matches!(store.claim_approval_continuation(winner).await.unwrap(), ApprovalClaimOutcome::Replayed(saved) if saved == receipt)
    );

    (receipt, grant)
}
