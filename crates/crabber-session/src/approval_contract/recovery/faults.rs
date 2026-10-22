use super::{
    ApprovalClaimOutcome, ApprovalError, ControlId, Duration, ManualClock, OffsetDateTime, Store,
    applied, evidence, prepare,
};
use crate::public_state_contract::FaultPoint;
pub(super) async fn run(store: &dyn Store, clock: &ManualClock, set_fault: &impl Fn(FaultPoint)) {
    for point in [FaultPoint::BeforeCommit, FaultPoint::AfterCommit] {
        let p = prepare(store, clock).await;
        clock.set(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(31));
        let before = evidence(store, &p).await;
        set_fault(point);
        let outcome = store.reclaim_approval_continuation(p.request.clone()).await;
        set_fault(FaultPoint::Off);
        assert_eq!(outcome.unwrap_err(), ApprovalError::UnknownStoreOutcome);
        let receipt = store
            .lookup_approval(&p.original.session_id, &p.request.id)
            .await
            .unwrap();
        if matches!(point, FaultPoint::BeforeCommit) {
            assert!(receipt.is_none());
            assert_eq!(evidence(store, &p).await, before);
            applied(
                store
                    .reclaim_approval_continuation(p.request.clone())
                    .await
                    .unwrap(),
            );
        } else {
            let receipt = receipt.unwrap();
            assert!(
                matches!(store.reclaim_approval_continuation(p.request.clone()).await.unwrap(),ApprovalClaimOutcome::Replayed(saved) if saved == receipt)
            );
            // Recovery ACK loss never turns the original operation into a permit.
            assert!(
                matches!(store.claim_approval_continuation(p.original.clone()).await.unwrap(),ApprovalClaimOutcome::Replayed(saved) if saved == p.receipt)
            );
            clock.set(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(62));
            let mut next = p.request.clone();
            next.id = ControlId::new();
            next.expected_revision = receipt.revision;
            let (_, grant) = applied(store.reclaim_approval_continuation(next).await.unwrap());
            assert_eq!(grant.calls, p.grant.calls);
            assert_ne!(grant.fence, p.grant.fence);
            assert!(
                matches!(store.reclaim_approval_continuation(p.request).await.unwrap(),ApprovalClaimOutcome::Replayed(saved) if saved == receipt)
            );
        }
    }
}
