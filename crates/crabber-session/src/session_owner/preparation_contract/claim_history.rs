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
}
pub(crate) async fn fixture(store: &impl Store, clock: &ManualClock) -> Fixture {
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
    let reserve = request(
        store,
        &f.admitted.run,
        PreparationSource::ToolRecovery(first.clone()),
        PreparationKey::ToolContinuation(first.id.clone()),
        ExistingWorkTarget::ToolRecovery {
            continuation: original.continuation.clone(),
            attempt: first.attempt.clone(),
        },
        first.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(ToolContinuationClaimOutcome::Applied {
        receipt: first,
        ..
    }) = complete(store, &reserve, PreparedOperation::ToolRecovery(first)).await
    else {
        panic!("first recovery");
    };
    let before_tail = store.get_run(&f.admitted.run.id).await.unwrap().unwrap();
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let tail = ToolContinuationRecovery {
        id: ControlId::new(),
        expected_revision: first.revision,
        original: original.clone(),
        attempt: f.suspension.next_attempt,
        owner: "second-recovery".into(),
        lease_ms: 30000,
    };
    let reserve = request(
        store,
        &f.admitted.run,
        PreparationSource::ToolRecovery(tail.clone()),
        PreparationKey::ToolContinuation(tail.id.clone()),
        ExistingWorkTarget::ToolRecovery {
            continuation: original.continuation,
            attempt: tail.attempt.clone(),
        },
        tail.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(ToolContinuationClaimOutcome::Applied {
        receipt: tail,
        ..
    }) = complete(store, &reserve, PreparedOperation::ToolRecovery(tail)).await
    else {
        panic!("second recovery");
    };
    assert!(tail.event_cursor > first.event_cursor);
    Fixture {
        before_tail,
        first,
        tail,
    }
}
