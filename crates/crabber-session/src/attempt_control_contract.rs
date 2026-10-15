//! Reusable host parent reconciliation contract; no credentials or executor needed.
use crate::{
    AdmitOutcome, AttemptCleanup, AttemptControlOutcome, AttemptError, AttemptReceipt,
    AttemptResolverClaim, AttemptRetirement, AttemptRetirementOutcome, AttemptState, AttemptTarget,
    AttemptUncertainty, AttemptUncertaintyCause, PendingCallDisposition, PhaseCleanup,
    PhaseCommand, PhaseControlOutcome, PhaseError, PhaseQuiescenceResult, PhaseResolution,
    PhaseResolutionOutcome, PhaseStart, PhaseUncertainty, PhaseUncertaintyCause,
    PhaseWriteAuthority, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store,
    UncertainPhaseChoice,
};
use crabber_core::{
    ControlId, ManualClock, MessageId, PartId, RunStatus, SessionRevision, ToolCallStatus,
};
use std::sync::Arc;
use time::OffsetDateTime;
#[cfg(test)]
#[path = "attempt_control_contract/faults.rs"]
pub(crate) mod faults;
#[path = "attempt_control_contract/lifecycle.rs"]
mod lifecycle;
#[path = "attempt_control_contract/ordering.rs"]
mod ordering;
pub(crate) fn claim() -> AttemptResolverClaim {
    AttemptResolverClaim {
        resolver_id: "verified-host".into(),
        authority_claim: "exact worker and owned resources joined".into(),
    }
}
pub(crate) fn fresh(outcome: AttemptControlOutcome) -> AttemptReceipt {
    let AttemptControlOutcome::Applied(receipt) = outcome else {
        panic!("fresh parent control");
    };
    receipt
}
pub(crate) fn uncertainty(
    admitted: &AdmitOutcome,
    target: AttemptTarget,
    revision: SessionRevision,
) -> AttemptUncertainty {
    AttemptUncertainty {
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        target,
        cause: AttemptUncertaintyCause::HostStopped { claim: claim() },
    }
}
pub(crate) fn retirement(
    request: &AttemptUncertainty,
    revision: SessionRevision,
) -> AttemptRetirement {
    AttemptRetirement {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        target: request.target.clone(),
        outcome: AttemptRetirementOutcome::Interrupted,
        joined: claim(),
        pending: PendingCallDisposition::CancelNeverStarted,
    }
}
/// Prove parent revocation, truthful retirement, exact replay and competing hosts.
/// # Panics
/// Panics when an adapter violates the contract or fixture setup fails.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    pending_and_replay(&store).await;
    lifecycle::run(&store).await;
    ordering::run(&store, &clock).await;
    expiry(&store, &clock).await;
}
#[allow(clippy::too_many_lines)] // One end-to-end replay/racing controller contract.
async fn pending_and_replay(store: &impl Store) {
    let (admitted, execution, owner, planned) = crate::phase_contract::fixture(store).await;
    let source_turn = crabber_core::TurnId::new();
    execution
        .append_event(crabber_core::EventRecord {
            cursor: None,
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            turn_id: Some(source_turn.clone()),
            kind: crabber_core::EventKind::TurnStarted,
            payload: serde_json::Value::Null,
            correlation: None,
            live_only: false,
            created_at: admitted.run.created_at,
        })
        .await
        .unwrap();
    let before = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    let revision = store.session_revision(&admitted.session.id).await.unwrap();
    let request = uncertainty(
        &admitted,
        planned.command.target().attempt.clone(),
        revision,
    );
    let mut expired = request.clone();
    expired.cause = AttemptUncertaintyCause::ExpiredOrRevokedOwner;
    assert_eq!(
        store.mark_attempt_uncertain(expired).await.unwrap_err(),
        AttemptError::LiveOwner
    );
    let mut wrong = request.clone();
    wrong.target.generation = wrong.target.generation.next().unwrap();
    assert_eq!(
        store.mark_attempt_uncertain(wrong).await.unwrap_err(),
        AttemptError::GenerationConflict
    );
    let (a, b) = futures::join!(
        store.mark_attempt_uncertain(request.clone()),
        store.mark_attempt_uncertain(request.clone())
    );
    let receipt = match (a.unwrap(), b.unwrap()) {
        (AttemptControlOutcome::Applied(a), AttemptControlOutcome::Replayed(b))
        | (AttemptControlOutcome::Replayed(a), AttemptControlOutcome::Applied(b)) => {
            assert_eq!(a, b);
            a
        }
        other => panic!("one parent revocation: {other:?}"),
    };
    assert_eq!(receipt.attempt.state, AttemptState::OutcomeUnknown);
    assert_eq!(receipt.attempt.cleanup, AttemptCleanup::Pending);
    let run = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    assert_ne!(run.claim_token, before.claim_token);
    assert_eq!(run.usage, before.usage);
    assert_eq!(run.status, RunStatus::Running);
    assert!(
        execution
            .mutate_phase(planned, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .is_err()
    );
    let mut changed = request.clone();
    changed.cause = AttemptUncertaintyCause::HostStopped {
        claim: AttemptResolverClaim {
            authority_claim: "changed claim".into(),
            ..claim()
        },
    };
    assert_eq!(
        store.mark_attempt_uncertain(changed).await.unwrap_err(),
        AttemptError::SemanticConflict
    );
    let mut retire = retirement(&request, receipt.revision);
    retire.pending = PendingCallDisposition::RequireNone;
    assert_eq!(
        store.retire_attempt(retire.clone()).await.unwrap_err(),
        AttemptError::CleanupBlocked
    );
    retire.pending = PendingCallDisposition::CancelNeverStarted;
    let mut competing = retire.clone();
    competing.id = ControlId::new();
    competing.outcome = AttemptRetirementOutcome::Failed;
    let (a, b) = futures::join!(
        store.retire_attempt(retire.clone()),
        store.retire_attempt(competing)
    );
    let settled = match (a, b) {
        (Ok(outcome), Err(AttemptError::RevisionConflict))
        | (Err(AttemptError::RevisionConflict), Ok(outcome)) => fresh(outcome),
        other => panic!("one retirement: {other:?}"),
    };
    assert_eq!(settled.attempt.cleanup, AttemptCleanup::Complete);
    let run = store.get_run(&request.run_id).await.unwrap().unwrap();
    assert!(matches!(
        run.status,
        RunStatus::Interrupted | RunStatus::Failed
    ));
    assert_eq!(run.usage, before.usage);
    let calls = read_calls(store, &admitted.session.id).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, ToolCallStatus::Interrupted);
    assert_eq!(
        calls[0].result.as_ref().unwrap().status,
        crabber_core::ToolResultStatus::Interrupted
    );
    assert_eq!(
        store.mark_attempt_uncertain(request.clone()).await.unwrap(),
        AttemptControlOutcome::Replayed(receipt)
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&request.session_id, &settled.operation_id)
            .await
            .unwrap(),
        Some(settled)
    );
    assert_eq!(
        store
            .lookup_phase_mutation(&request.session_id, &request.id)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    assert_eq!(store.get_run(&request.run_id).await.unwrap().unwrap(), run);
    let events = store
        .list_events(&request.session_id, None, 100)
        .await
        .unwrap();
    let closed = events
        .iter()
        .filter(|event| event.kind == crabber_core::EventKind::TurnCompleted)
        .collect::<Vec<_>>();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].turn_id, Some(source_turn.clone()));
    assert!(
        events
            .iter()
            .filter(|event| matches!(
                event.kind,
                crabber_core::EventKind::ToolCallSettled | crabber_core::EventKind::RunSettled
            ))
            .all(|event| event.turn_id == Some(source_turn.clone()))
    );
    let terminal = events
        .iter()
        .find(|event| event.kind == crabber_core::EventKind::RunSettled)
        .unwrap();
    assert_eq!(terminal.payload["duration_ns"], 0);
    assert_eq!(terminal.payload["input_tokens"], before.usage.input_tokens);
    // A new ordinary admission cannot invalidate historical control replay.
    let mut input = crate::management_contract::admission(
        &store.session_summary(&admitted.session.id).await.unwrap(),
    );
    input.user_message.id = MessageId::new();
    store.admit_run(input).await.unwrap();
    assert!(matches!(
        store.mark_attempt_uncertain(request).await.unwrap(),
        AttemptControlOutcome::Replayed(_)
    ));
}
pub(crate) async fn read_calls(
    store: &impl Store,
    session: &crabber_core::SessionId,
) -> Vec<crabber_core::ToolCallRecord> {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 64,
                tool_calls: 64,
                parts: 128,
                text_bytes: 1024 * 1024,
                encoded_bytes: 4 * 1024 * 1024,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("bounded calls");
    };
    page.tool_calls
}
async fn expiry(store: &impl Store, clock: &ManualClock) {
    let (admitted, _, _, planned) = crate::phase_contract::fixture(store).await;
    let mut request = uncertainty(
        &admitted,
        planned.command.target().attempt.clone(),
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    request.cause = AttemptUncertaintyCause::ExpiredOrRevokedOwner;
    clock.set(admitted.run.lease_until);
    let marked = fresh(store.mark_attempt_uncertain(request.clone()).await.unwrap());
    store
        .retire_attempt(retirement(&request, marked.revision))
        .await
        .unwrap();
}

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "attempt_control_contract/process.rs"]
pub(crate) mod process;

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "attempt_control_contract/integrity.rs"]
pub(crate) mod integrity;
