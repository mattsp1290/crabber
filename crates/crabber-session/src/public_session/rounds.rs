//! A superseded Released batch belongs to the same nonterminal run.
use super::tests::{page, request};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationState, ApprovalReply, AttemptCommand,
    AttemptMutation, AttemptTarget, CallApproval, Store,
};
use crabber_core::{AttemptId, ContinuationId, ControlId, InteractionId, ManualClock, PhaseId};

pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    checked(store, clock, |_, _| std::future::ready(())).await;
}
pub(crate) async fn checked<F, Fut>(store: &impl Store, clock: &ManualClock, check: F)
where
    F: FnOnce(crate::ContinuationTarget, crabber_core::Run) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut f = crate::approval_contract::fixture::staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_approval(
                crate::approval_contract::fixture::answer(store, &f, index, ApprovalReply::Grant)
                    .await,
            )
            .await
            .unwrap();
    }
    let ApprovalClaimOutcome::Applied { receipt, grant } = store
        .claim_approval_continuation(crate::approval_contract::fixture::claim(store, &f).await)
        .await
        .unwrap()
    else {
        panic!("fresh claim")
    };
    let claimed = page(store, request(f.run.session_id.clone())).await;
    assert_eq!(
        claimed.header.approval.unwrap().state,
        ApprovalContinuationState::Claimed
    );
    let execution = store.execution(grant.fence.clone()).await.unwrap();
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: f.batch.next_attempt.clone(),
                },
            },
            Some(grant.authority.clone()),
        )
        .await
        .unwrap();
    let prior = f.batch.continuation.clone();
    let mut next = f.batch.clone();
    next.id = ControlId::new();
    next.expected_revision = store.session_revision(&f.run.session_id).await.unwrap();
    next.attempt = f.batch.next_attempt.clone();
    next.next_attempt = AttemptTarget {
        id: AttemptId::new(),
        generation: next.attempt.generation.next().unwrap(),
    };
    next.continuation.id = ContinuationId::new();
    next.continuation.generation = prior.generation.next().unwrap();
    for prepared in &mut next.calls {
        prepared.target.id = PhaseId::new();
        prepared.target.attempt = next.next_attempt.clone();
        if let CallApproval::Ask { id, .. } = &mut prepared.decision {
            *id = InteractionId::new();
        }
    }
    // No call has ever entered a native phase. A fresh policy suspension can
    // retain these pending bindings without claiming that their effects ran.
    execution
        .suspend_for_approval(next.clone(), grant.authority)
        .await
        .unwrap();
    let current = page(store, request(f.run.session_id.clone())).await;
    assert_eq!(
        current.run.as_ref().unwrap().status,
        crabber_core::RunStatus::Paused
    );
    let approval = current.header.approval.unwrap();
    assert_eq!(approval.state, ApprovalContinuationState::Waiting);
    assert_eq!(approval.continuation, next.continuation);
    assert_ne!(approval.continuation, prior);
    assert_eq!(
        current.header.barrier.unwrap().continuation,
        next.continuation
    );
    check(prior, store.get_run(&f.run.id).await.unwrap().unwrap()).await;
    f.batch = next;
    store
        .terminate_approval(crate::approval_contract::termination::request(store, &f, false).await)
        .await
        .unwrap();
    let terminal = page(store, request(f.run.session_id.clone())).await;
    assert!(terminal.is_terminal());
    assert_eq!(
        terminal.header.approval.unwrap().continuation,
        f.batch.continuation
    );
}
