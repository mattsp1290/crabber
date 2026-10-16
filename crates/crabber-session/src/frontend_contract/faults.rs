use super::{Store, fixture};
use crate::*;
use crabber_core::{Clock, ControlId, ManualClock};
#[allow(clippy::too_many_lines)] // Four consecutive atomic boundaries, each with rollback and lost-ACK verification.
pub(crate) async fn run(store: &impl Store, clock: &ManualClock, set: impl Fn(u8)) {
    let f = fixture::fixture(store).await;
    set(1);
    assert_eq!(
        f.execution
            .stage_frontend_call(f.request.clone(), f.authority.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_frontend_staging(&f.request.session_id, &f.request.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        f.request.expected_revision
    );
    set(2);
    assert_eq!(
        f.execution
            .stage_frontend_call(f.request.clone(), f.authority.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let staged = store
        .lookup_frontend_staging(&f.request.session_id, &f.request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        f.execution
            .stage_frontend_call(f.request.clone(), f.authority.clone())
            .await
            .unwrap(),
        staged
    );
    let batch = fixture::suspension(store, &f.request).await;
    let authority = ToolSuspensionAuthority {
        attempt: f.authority.clone(),
        phases: Vec::new(),
    };
    set(1);
    assert_eq!(
        f.execution
            .suspend_tool_continuation(batch.clone(), authority.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_tool_continuation(&f.request.session_id, &batch.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        staged.revision
    );
    set(2);
    assert_eq!(
        f.execution
            .suspend_tool_continuation(batch.clone(), authority.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let paused = store
        .lookup_tool_continuation(&f.request.session_id, &batch.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        f.execution
            .suspend_tool_continuation(batch.clone(), authority)
            .await
            .unwrap(),
        paused
    );
    let claim = fixture::external_claim(&f.request, &batch, paused.revision);
    set(1);
    assert_eq!(
        store
            .claim_frontend_continuation(claim.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_tool_continuation(&f.request.session_id, &claim.claim.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        paused.revision
    );
    set(2);
    assert_eq!(
        store
            .claim_frontend_continuation(claim.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let claimed = store
        .lookup_tool_continuation(&f.request.session_id, &claim.claim.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.claim_frontend_continuation(claim.clone()).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==claimed)
    );
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let recovery = FrontendContinuationRecovery {
        id: ControlId::new(),
        expected_revision: claimed.revision,
        original: claim,
        attempt: batch.next_attempt,
        owner: "fault-recovery".into(),
        lease_ms: 30000,
    };
    set(1);
    assert_eq!(
        store
            .recover_frontend_continuation(recovery.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_tool_continuation(&f.request.session_id, &recovery.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        claimed.revision
    );
    set(2);
    assert_eq!(
        store
            .recover_frontend_continuation(recovery.clone())
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    let recovered = store
        .lookup_tool_continuation(&f.request.session_id, &recovery.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.recover_frontend_continuation(recovery).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==recovered)
    );
    set(0);
}
