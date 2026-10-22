//! Consumed answers authorize only one fresh expired-Unstarted owner at a time.
#[path = "recovery/faults.rs"]
mod faults;
#[path = "recovery/guards.rs"]
mod guards;
use super::fixture::{Fixture, answer, claim, staged};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationRecovery, ApprovalError,
    ApprovalGrant, ApprovalReceipt, ApprovalReply, AttemptCommand, AttemptMutation,
    AttemptMutationOutcome, Store,
};
use crabber_core::{ControlId, ManualClock};
use time::{Duration, OffsetDateTime};

pub(crate) struct Prepared {
    pub fixture: Fixture,
    pub original: ApprovalContinuationClaim,
    pub receipt: ApprovalReceipt,
    pub grant: Box<ApprovalGrant>,
    pub request: ApprovalContinuationRecovery,
}
pub(crate) async fn prepare(store: &dyn Store, clock: &ManualClock) -> Prepared {
    clock.set(OffsetDateTime::UNIX_EPOCH);
    let mut f = staged(store, clock).await;
    f.batch.source_turn_id = Some(crabber_core::TurnId("turn:/λ/frozen".into()));
    f.batch.usage = crabber_core::Usage {
        input_tokens: 37,
        output_tokens: 11,
    };
    f.batch.turns_used = Some(3);
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for (index, reply) in [ApprovalReply::Grant, ApprovalReply::Deny]
        .into_iter()
        .enumerate()
    {
        store
            .answer_approval(answer(store, &f, index, reply).await)
            .await
            .unwrap();
    }
    let mut original = claim(store, &f).await;
    original.lease_ms = 30;
    let (receipt, grant) = applied(
        store
            .claim_approval_continuation(original.clone())
            .await
            .unwrap(),
    );
    let request = ApprovalContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: original.clone(),
        attempt: f.batch.next_attempt.clone(),
        owner: "fresh-recovery-owner".into(),
        lease_ms: 30,
    };
    Prepared {
        fixture: f,
        original,
        receipt,
        grant,
        request,
    }
}
pub(super) fn applied(outcome: ApprovalClaimOutcome) -> (ApprovalReceipt, Box<ApprovalGrant>) {
    let ApprovalClaimOutcome::Applied { receipt, grant } = outcome else {
        panic!("fresh approval owner")
    };
    (receipt, grant)
}
pub(crate) async fn evidence(store: &dyn Store, p: &Prepared) -> serde_json::Value {
    let run = store.get_run(&p.original.run_id).await.unwrap().unwrap();
    serde_json::json!({"run":run, "attempt":store.latest_execution_attempt(&run.id).await.unwrap(),
        "approval":store.approval_snapshot(&run.session_id,&run.id).await.unwrap(),
        "calls":store.list_unfinished_tool_calls(&run.id).await.unwrap(),
        "messages":store.list_all_messages(&run.session_id).await.unwrap(),
        "events":store.list_events(&run.session_id,None,1000).await.unwrap(),
        "revision":store.session_revision(&run.session_id).await.unwrap()})
}
pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    let p = prepare(store, clock).await;
    clock.set(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(31));
    let before = evidence(store, &p).await;
    let (receipt, grant) = applied(
        store
            .reclaim_approval_continuation(p.request.clone())
            .await
            .unwrap(),
    );
    p.request.verify_receipt(&receipt).unwrap();
    assert_ne!(grant.fence, p.grant.fence);
    assert_eq!(grant.calls, p.grant.calls);
    assert_eq!(grant.source_turn_id, p.grant.source_turn_id);
    assert_eq!(grant.usage, p.grant.usage);
    assert_eq!(grant.turns_used, p.fixture.batch.turns_used.unwrap());
    let after = evidence(store, &p).await;
    for field in ["calls", "messages"] {
        assert_eq!(before[field], after[field]);
    }
    let mut snapshot = before["approval"].clone();
    snapshot["revision"] = after["approval"]["revision"].clone();
    assert_eq!(snapshot, after["approval"]);
    assert_eq!(before["attempt"]["attempt"], after["attempt"]["attempt"]);
    assert!(
        matches!(store.claim_approval_continuation(p.original.clone()).await.unwrap(), ApprovalClaimOutcome::Replayed(saved) if saved == p.receipt)
    );
    assert!(
        matches!(store.reclaim_approval_continuation(p.request.clone()).await.unwrap(), ApprovalClaimOutcome::Replayed(saved) if saved == receipt)
    );
    let begin = |revision| AttemptMutation {
        id: ControlId::new(),
        expected_revision: revision,
        command: AttemptCommand::Begin {
            target: p.request.attempt.clone(),
        },
    };
    assert!(store.execution(p.grant.fence.clone()).await.is_err());
    let execution = store.execution(grant.fence.clone()).await.unwrap();
    assert!(
        execution
            .mutate_attempt(begin(receipt.revision), Some(p.grant.authority.clone()))
            .await
            .is_err()
    );
    let AttemptMutationOutcome::Applied { .. } = execution
        .mutate_attempt(begin(receipt.revision), Some(grant.authority))
        .await
        .unwrap()
    else {
        panic!("fresh Begin")
    };
    assert!(
        matches!(store.reclaim_approval_continuation(p.request.clone()).await.unwrap(), ApprovalClaimOutcome::Replayed(saved) if saved == receipt)
    );
    clock.set(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1));
    let mut next = p.request.clone();
    next.id = ControlId::new();
    next.expected_revision = store
        .session_revision(&p.original.session_id)
        .await
        .unwrap();
    let before = evidence(store, &p).await;
    assert_eq!(
        store.reclaim_approval_continuation(next).await.unwrap_err(),
        ApprovalError::InvalidTransition
    );
    assert_eq!(evidence(store, &p).await, before);
    guards::run(store, clock).await;
}
pub(super) async fn fault_contract(
    store: &dyn Store,
    clock: &ManualClock,
    set_fault: &impl Fn(crate::public_state_contract::FaultPoint),
) {
    faults::run(store, clock, set_fault).await;
}
