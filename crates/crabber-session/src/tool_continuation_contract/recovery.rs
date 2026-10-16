use super::{ManualClock, Store, ToolContinuationClaimOutcome, ToolContinuationError, fixture};
use crate::{AttemptCommand, AttemptError, AttemptMutation, ToolContinuationRecovery};
use crabber_core::ControlId;
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let f = fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for i in 0..2 {
        store
            .answer_tool_interaction(fixture::answer(store, &f, i).await)
            .await
            .unwrap();
    }
    let mut claim = fixture::claim(store, &f).await;
    claim.lease_ms = 1;
    let ToolContinuationClaimOutcome::Applied {
        receipt,
        grant: old,
    } = store.claim_tool_continuation(claim.clone()).await.unwrap()
    else {
        panic!("claim")
    };
    let request = ToolContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: claim.clone(),
        attempt: f.suspension.next_attempt.clone(),
        owner: "recovered-worker".into(),
        lease_ms: 30000,
    };
    assert_eq!(
        store
            .recover_tool_continuation(request.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::LiveOwner
    );
    let old_execution = store.execution(old.fence.clone()).await.unwrap();
    clock.set(clock_value(clock) + time::Duration::milliseconds(2));
    let ToolContinuationClaimOutcome::Applied {
        receipt: recovered,
        grant,
    } = store
        .recover_tool_continuation(request.clone())
        .await
        .unwrap()
    else {
        panic!("recover")
    };
    request.verify_receipt(&recovered).unwrap();
    assert_eq!(grant.phases[0].target, old.phases[0].target);
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: recovered.revision,
        command: AttemptCommand::Begin {
            target: request.attempt.clone(),
        },
    };
    assert_eq!(
        old_execution
            .mutate_attempt(begin.clone(), Some(old.authority))
            .await
            .unwrap_err(),
        AttemptError::StaleOwner
    );
    let execution = store.execution(grant.fence).await.unwrap();
    execution
        .mutate_attempt(begin, Some(grant.authority))
        .await
        .unwrap();
    assert!(
        matches!(store.recover_tool_continuation(request.clone()).await.unwrap(),ToolContinuationClaimOutcome::Replayed(actual) if actual==recovered)
    );
    let mut started = request;
    started.id = ControlId::new();
    started.expected_revision = store
        .session_revision(&f.admitted.session.id)
        .await
        .unwrap();
    assert_eq!(
        store.recover_tool_continuation(started).await.unwrap_err(),
        ToolContinuationError::CleanupBlocked
    );
    assert!(
        matches!(store.claim_tool_continuation(claim).await.unwrap(),ToolContinuationClaimOutcome::Replayed(actual) if actual==receipt)
    );
}
fn clock_value(clock: &ManualClock) -> time::OffsetDateTime {
    crabber_core::Clock::now(clock)
}
