//! Faults cover the complete compound write, including generated native authority.
use super::{Store, ToolContinuationClaimOutcome, ToolContinuationError, fixture};
use crabber_core::{EventRecord, Run, SessionRevision, ToolCallRecord};
#[derive(Debug, PartialEq)]
struct Evidence {
    run: Option<Run>,
    revision: SessionRevision,
    events: Vec<EventRecord>,
    calls: Vec<ToolCallRecord>,
    snapshot: serde_json::Value,
    attempt: Option<crate::VersionedAttempt>,
    phases: crate::PhasePage,
}
async fn evidence(store: &impl Store, f: &fixture::Fixture) -> Evidence {
    let session = &f.admitted.session.id;
    let run = &f.admitted.run.id;
    Evidence {
        run: store.get_run(run).await.unwrap(),
        attempt: store.latest_execution_attempt(run).await.unwrap(),
        phases: store
            .list_tool_phases(crate::PhasePageRequest {
                session_id: session.clone(),
                run_id: run.clone(),
                limit: 64,
                after: None,
                expected_revision: None,
            })
            .await
            .unwrap(),
        revision: store.session_revision(session).await.unwrap(),
        events: store.list_events(session, None, 100).await.unwrap(),
        calls: store.list_unfinished_tool_calls(run).await.unwrap(),
        snapshot: serde_json::to_value(
            store
                .tool_continuation_snapshot(session, run)
                .await
                .unwrap(),
        )
        .unwrap(),
    }
}
/// Shared before-commit rollback and committed-ACK-loss contract.
/// # Panics
/// Panics when a transaction leaks partial state or a replay grants authority.
pub async fn run_fault_contract(
    store: &impl Store,
    clock: &crabber_core::ManualClock,
    set: impl Fn(u8),
) {
    let f = fixture::fixture(store).await;
    let before = evidence(store, &f).await;
    set(1);
    assert_eq!(
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    set(0);
    assert_eq!(evidence(store, &f).await, before);
    assert!(
        store
            .lookup_tool_continuation(&f.admitted.session.id, &f.suspension.id)
            .await
            .unwrap()
            .is_none()
    );
    set(2);
    assert_eq!(
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let suspended = f
        .execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    set(0);
    let answer = fixture::answer(store, &f, 0).await;
    let before = evidence(store, &f).await;
    set(1);
    assert_eq!(
        store
            .answer_tool_interaction(answer.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    set(0);
    assert_eq!(evidence(store, &f).await, before);
    set(2);
    assert_eq!(
        store
            .answer_tool_interaction(answer.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let answered = store.answer_tool_interaction(answer.clone()).await.unwrap();
    set(0);
    assert_eq!(
        store.answer_tool_interaction(answer).await.unwrap(),
        answered
    );
    store
        .answer_tool_interaction(fixture::answer(store, &f, 1).await)
        .await
        .unwrap();
    let mut claim = fixture::claim(store, &f).await;
    claim.lease_ms = 1;
    let before = evidence(store, &f).await;
    set(1);
    assert_eq!(
        store
            .claim_tool_continuation(claim.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    set(0);
    assert_eq!(evidence(store, &f).await, before);
    set(2);
    assert_eq!(
        store
            .claim_tool_continuation(claim.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert!(matches!(
        store.claim_tool_continuation(claim.clone()).await.unwrap(),
        ToolContinuationClaimOutcome::Replayed(_)
    ));
    set(0);
    assert_eq!(
        store
            .lookup_tool_continuation(&f.admitted.session.id, &f.suspension.id)
            .await
            .unwrap(),
        Some(suspended)
    );
    recovery(store, clock, &set, &f, claim).await;
}
async fn recovery(
    store: &impl Store,
    clock: &crabber_core::ManualClock,
    set: &impl Fn(u8),
    f: &fixture::Fixture,
    original: crate::ToolContinuationClaim,
) {
    use crabber_core::Clock;
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let request = crate::ToolContinuationRecovery {
        id: crabber_core::ControlId::new(),
        expected_revision: store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
        original,
        attempt: f.suspension.next_attempt.clone(),
        owner: "fault-recovery".into(),
        lease_ms: 30000,
    };
    let before = evidence(store, f).await;
    set(1);
    assert_eq!(
        store
            .recover_tool_continuation(request.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    set(0);
    assert_eq!(evidence(store, f).await, before);
    set(2);
    assert_eq!(
        store
            .recover_tool_continuation(request.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let ToolContinuationClaimOutcome::Replayed(receipt) = store
        .recover_tool_continuation(request.clone())
        .await
        .unwrap()
    else {
        panic!("ACK-loss retry must not grant fresh execution authority")
    };
    request.verify_receipt(&receipt).unwrap();
    assert_eq!(
        store
            .lookup_tool_continuation(&f.admitted.session.id, &request.id)
            .await
            .unwrap(),
        Some(receipt)
    );
    set(0);
}
