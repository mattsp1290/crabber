#[path = "frontend_contract/large.rs"]
pub(crate) mod large;
// Shared frontend execution contracts. External results never authorize backend dispatch.
#[path = "frontend_contract/faults.rs"]
mod faults;
#[path = "frontend_contract/fixture.rs"]
pub(crate) mod fixture;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "frontend_contract/process.rs"]
pub(crate) mod process;
#[path = "frontend_contract/race.rs"]
mod race;
use crate::*;
use crabber_core::{ControlId, ManualClock, PhaseGeneration, PhaseId, ToolCallStatus};
pub(crate) use faults::run as run_fault_contract;
use std::sync::Arc;
use time::OffsetDateTime;
#[cfg(test)]
pub(crate) async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    Box::pin(journey(&store, &clock)).await;
}
#[cfg(test)]
#[allow(clippy::too_many_lines)] // One complete paused, claimed, and recovered authority journey.
async fn journey(store: &dyn Store, clock: &ManualClock) {
    race::run(store).await;
    let f = fixture::fixture(store).await;
    let staged = f
        .execution
        .stage_frontend_call(f.request.clone(), f.authority.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .lookup_frontend_staging(&f.request.session_id, &f.request.id)
            .await
            .unwrap(),
        Some(staged.clone())
    );
    let origin = store
        .frontend_call_origin(&f.request.session_id, &f.request.run_id, &f.request.call.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        origin.call_binding,
        crate::phase::call_binding(&f.request.call).unwrap()
    );
    assert_eq!(
        f.execution
            .stage_frontend_call(f.request.clone(), f.authority.clone())
            .await
            .unwrap(),
        staged
    );
    let raw = PhaseMutation {
        id: ControlId::new(),
        expected_revision: staged.revision,
        command: PhaseCommand::Claim {
            target: PhaseTarget {
                id: PhaseId::new(),
                generation: PhaseGeneration::INITIAL,
                call_id: f.request.call.id.clone(),
                attempt: f.request.parent.clone(),
            },
        },
    };
    assert_eq!(
        f.execution
            .mutate_phase(raw, Some(PhaseWriteAuthority::Attempt(f.authority.clone())))
            .await
            .unwrap_err(),
        PhaseError::InvalidTransition
    );
    let batch = fixture::suspension(store, &f.request).await;
    let mut collision = batch.clone();
    collision.id = ControlId::new();
    collision
        .calls
        .iter_mut()
        .find(|call| call.target.call_id != f.request.call.id)
        .unwrap()
        .target
        .id = batch.frontend[0].target.id.clone();
    assert_eq!(
        f.execution
            .suspend_tool_continuation(
                collision.clone(),
                ToolSuspensionAuthority {
                    attempt: f.authority.clone(),
                    phases: Vec::new()
                }
            )
            .await
            .unwrap_err(),
        ToolContinuationError::InvalidRequest
    );
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        staged.revision
    );
    assert!(
        store
            .lookup_tool_continuation(&f.request.session_id, &collision.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .tool_continuation_snapshot(&f.request.session_id, &f.request.run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_unfinished_tool_calls(&f.request.run_id)
            .await
            .unwrap()
            .iter()
            .all(|call| call.status == ToolCallStatus::Pending)
    );
    let paused = f
        .execution
        .suspend_tool_continuation(
            batch.clone(),
            ToolSuspensionAuthority {
                attempt: f.authority.clone(),
                phases: Vec::new(),
            },
        )
        .await
        .unwrap();
    let snapshot = store
        .tool_continuation_snapshot(&f.request.session_id, &f.request.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, ToolContinuationState::Waiting);
    assert_eq!(snapshot.frontend_calls.len(), 1);
    assert_eq!(
        store
            .list_unfinished_tool_calls(&f.request.run_id)
            .await
            .unwrap()
            .iter()
            .filter(|c| c.status == ToolCallStatus::Waiting)
            .count(),
        1
    );
    let claim = fixture::external_claim(&f.request, &batch, paused.revision);
    assert_eq!(
        store
            .claim_tool_continuation(claim.claim.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::InvalidTransition
    );
    let mut bad_output = claim.clone();
    bad_output.results = vec![
        crabber_core::FrontendToolResult::new(
            f.request.call.id.clone(),
            crabber_core::FrontendResultStatus::Completed,
            crabber_core::ToolOutput::new(Vec::new(), false).unwrap(),
        )
        .unwrap(),
    ];
    assert_eq!(
        store
            .claim_frontend_continuation(bad_output)
            .await
            .unwrap_err(),
        ToolContinuationError::InvalidRequest
    );
    let mut wrong = claim.clone();
    wrong.results.clear();
    assert!(store.claim_frontend_continuation(wrong).await.is_err());
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        paused.revision
    );
    let ToolContinuationClaimOutcome::Applied { receipt, grant } = store
        .claim_frontend_continuation(claim.clone())
        .await
        .unwrap()
    else {
        panic!("fresh grant")
    };
    assert!(grant.phases.is_empty());
    assert_eq!(grant.frontend.len(), 1);
    assert_eq!(grant.frontend[0].target, batch.frontend[0].target);
    assert!(
        matches!(store.claim_frontend_continuation(claim.clone()).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==receipt)
    );
    let recovery = FrontendContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: claim.clone(),
        attempt: batch.next_attempt.clone(),
        owner: "new-external-worker".into(),
        lease_ms: 30000,
    };
    assert!(
        store
            .recover_frontend_continuation(recovery.clone())
            .await
            .is_err()
    );
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(2));
    let ToolContinuationClaimOutcome::Applied {
        receipt: recovered,
        grant: new,
    } = store
        .recover_frontend_continuation(recovery.clone())
        .await
        .unwrap()
    else {
        panic!("fresh recovery")
    };
    assert_eq!(new.frontend[0].result, grant.frontend[0].result);
    assert!(
        matches!(store.recover_frontend_continuation(recovery).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==recovered)
    );
    assert!(store.execution(grant.fence).await.is_err());
    let execution = store.execution(new.fence).await.unwrap();
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: recovered.revision,
                command: AttemptCommand::Begin {
                    target: batch.next_attempt.clone(),
                },
            },
            Some(new.authority),
        )
        .await
        .unwrap();
    let late = FrontendContinuationRecovery {
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.request.session_id).await.unwrap(),
        original: claim,
        attempt: batch.next_attempt,
        owner: "late-worker".into(),
        lease_ms: 30000,
    };
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(31));
    assert_eq!(
        store
            .recover_frontend_continuation(late.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::CleanupBlocked
    );
    assert_eq!(
        store
            .lookup_tool_continuation(&f.request.session_id, &f.request.id)
            .await
            .unwrap_err(),
        ToolContinuationError::SemanticConflict
    );
    crate::session_owner::preparation_contract::frontend::run(store, clock).await;
}
