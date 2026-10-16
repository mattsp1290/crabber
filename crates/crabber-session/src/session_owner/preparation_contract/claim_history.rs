//! Two actual prepared recoveries of the same unstarted tool continuation.
use super::super::preparation::{
    ExistingWorkTarget, PreparationCompletion, PreparationKey, PreparationSource, PreparedOperation,
};
use super::continuations::{complete, request};
use crate::{
    Store, ToolContinuationClaimOutcome, ToolContinuationReceipt, ToolContinuationRecovery,
};
use crabber_core::{Clock, ControlId, ManualClock, Run};
pub(crate) struct Fixture {
    pub(crate) before_tail: Run,
    pub(crate) first: ToolContinuationReceipt,
    pub(crate) tail: ToolContinuationReceipt,
    pub(crate) first_grant: Box<crate::ToolContinuationGrant>,
}
pub(crate) async fn fixture(store: &impl Store, clock: &ManualClock) -> Fixture {
    fixture_checkpoint(store, clock, |_, _, _| async {}).await.0
}
pub(crate) async fn fixture_checkpoint<F, Fut, T>(
    store: &impl Store,
    clock: &ManualClock,
    checkpoint: F,
) -> (Fixture, T)
where
    F: FnOnce(Run, crate::AttemptTarget, crabber_core::ContinuationId) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use crate::tool_continuation_contract::fixture::{answer, claim, fixture};
    let f = fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_tool_interaction(answer(store, &f, index).await)
            .await
            .unwrap();
    }
    let mut original = claim(store, &f).await;
    original.lease_ms = 1;
    let reserve = request(
        store,
        &f.admitted.run,
        PreparationSource::ToolClaim(original.clone()),
        PreparationKey::ToolContinuation(original.id.clone()),
        ExistingWorkTarget::ToolClaim(original.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(ToolContinuationClaimOutcome::Applied {
        receipt,
        ..
    }) = complete(
        store,
        &reserve,
        PreparedOperation::ToolClaim(original.clone()),
    )
    .await
    else {
        panic!("fresh original claim");
    };
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let first = ToolContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: original.clone(),
        attempt: f.suspension.next_attempt.clone(),
        owner: "first-recovery".into(),
        lease_ms: 1,
    };
    let (first, first_grant) = recover(store, &f.admitted.run, first).await;
    let before_tail = store.get_run(&f.admitted.run.id).await.unwrap().unwrap();
    let captured = checkpoint(
        before_tail.clone(),
        f.suspension.next_attempt.clone(),
        original.continuation.id.clone(),
    )
    .await;
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let tail = ToolContinuationRecovery {
        id: ControlId::new(),
        expected_revision: first.revision,
        original: original.clone(),
        attempt: f.suspension.next_attempt,
        owner: "second-recovery".into(),
        lease_ms: 30000,
    };
    let (tail, _) = recover(store, &f.admitted.run, tail).await;
    assert!(tail.event_cursor > first.event_cursor);
    (
        Fixture {
            before_tail,
            first,
            tail,
            first_grant,
        },
        captured,
    )
}

async fn recover(
    store: &impl Store,
    run: &Run,
    recovery: ToolContinuationRecovery,
) -> (ToolContinuationReceipt, Box<crate::ToolContinuationGrant>) {
    let reserve = request(
        store,
        run,
        PreparationSource::ToolRecovery(recovery.clone()),
        PreparationKey::ToolContinuation(recovery.id.clone()),
        ExistingWorkTarget::ToolRecovery {
            continuation: recovery.original.continuation.clone(),
            attempt: recovery.attempt.clone(),
        },
        recovery.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(ToolContinuationClaimOutcome::Applied {
        receipt,
        grant,
    }) = complete(store, &reserve, PreparedOperation::ToolRecovery(recovery)).await
    else {
        panic!("fresh genuine recovery")
    };
    (receipt, grant)
}
