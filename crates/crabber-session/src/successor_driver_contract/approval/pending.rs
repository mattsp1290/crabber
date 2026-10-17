//! Actual owned preparation before its native Approval completion exists.
use super::*;
pub(crate) async fn prepared(
    store: &impl Store,
    clock: &ManualClock,
    recovery: bool,
    unkeyed: bool,
) -> (SessionContext, JoinedPreparation) {
    let (context, f) = Box::pin(staged(store, clock, unkeyed, false)).await;
    let mut original = crate::approval_contract::fixture::claim(store, &f).await;
    if recovery {
        original.lease_ms = 1;
    }
    let joined = prepare(
        store,
        &f.run,
        PreparationSource::ApprovalClaim(original.clone()),
        PreparedOperation::ApprovalClaim(original.clone()),
        PreparationKey::Approval(original.id.clone()),
        ExistingWorkTarget::ApprovalClaim(original.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    if !recovery {
        return (context, joined);
    }
    assert!(matches!(
        store.complete_preparation(joined).await.unwrap(),
        PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied { .. })
    ));
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let request = crate::ApprovalContinuationRecovery {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        original: original.clone(),
        attempt: f.batch.next_attempt.clone(),
        owner: "pending-approval-host".into(),
        lease_ms: 30000,
    };
    let joined = prepare(
        store,
        &f.run,
        PreparationSource::ApprovalRecovery(request.clone()),
        PreparedOperation::ApprovalRecovery(request.clone()),
        PreparationKey::Approval(request.id.clone()),
        ExistingWorkTarget::ApprovalRecovery {
            continuation: original.continuation,
            attempt: request.attempt.clone(),
        },
        request.fingerprint().unwrap(),
    )
    .await;
    (context, joined)
}

pub(crate) async fn inspected(
    store: &impl Store,
    clock: &ManualClock,
    recovery: bool,
) -> crate::session_owner::preparation::InspectPreparedApprovalDriver {
    use crate::session_owner::{driver::*, preparation::*};
    let (context, joined) = prepared(store, clock, recovery, false).await;
    let (operation_id, semantic_digest, origin) = match joined.observed_operation() {
        PreparedOperation::ApprovalClaim(v) => (
            v.id.clone(),
            v.fingerprint().unwrap(),
            SuccessorExecutionDriverOrigin::ApprovalClaim,
        ),
        PreparedOperation::ApprovalRecovery(v) => (
            v.id.clone(),
            v.fingerprint().unwrap(),
            SuccessorExecutionDriverOrigin::ApprovalRecovery,
        ),
        _ => panic!("actual Approval"),
    };
    let captured = store
        .complete_preparation_with_driver_source(joined)
        .await
        .unwrap();
    let PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied { grant, .. }) =
        captured.completion
    else {
        panic!("fresh native");
    };
    InspectPreparedApprovalDriver {
        request: RegisterSuccessorExecutionDriver {
            context,
            fence: grant.fence,
            claim: captured.claim.unwrap(),
        },
        operation_id,
        semantic_digest,
        origin,
    }
}

pub(crate) async fn commit_faults(store: &impl Store, clock: &ManualClock, fault: impl Fn(u8)) {
    for recovery in [false, true] {
        for point in [1, 2] {
            let (_, joined) = prepared(store, clock, recovery, false).await;
            let session = joined
                .observed_grant()
                .original_reservation()
                .owner
                .binding
                .session_id
                .clone();
            let id = match joined.observed_operation() {
                PreparedOperation::ApprovalClaim(v) => v.id.clone(),
                PreparedOperation::ApprovalRecovery(v) => v.id.clone(),
                _ => panic!("actual Approval"),
            };
            let before = store.session_revision(&session).await.unwrap();
            fault(point);
            assert!(
                store
                    .complete_preparation_with_driver_source(joined.clone())
                    .await
                    .is_err()
            );
            fault(0);
            assert_eq!(
                store
                    .lookup_approval(&session, &id)
                    .await
                    .unwrap()
                    .is_some(),
                point == 2
            );
            if point == 1 {
                assert_eq!(store.session_revision(&session).await.unwrap(), before);
            }
            let completion = store
                .complete_preparation_with_driver_source(joined.clone())
                .await
                .unwrap();
            assert_eq!(completion.completion.is_replayed(), point == 2);
            assert_eq!(completion.claim.is_some(), point == 1);
            let after = store.session_revision(&session).await.unwrap();
            let replay = store
                .complete_preparation_with_driver_source(joined)
                .await
                .unwrap();
            assert!(replay.completion.is_replayed());
            assert!(replay.claim.is_none());
            assert_eq!(store.session_revision(&session).await.unwrap(), after);
        }
    }
}

pub(crate) async fn unprepared(
    store: &impl Store,
    clock: &ManualClock,
    recovery: bool,
    unkeyed: bool,
) -> crate::session_owner::preparation::InspectPendingApprovalPreparation {
    use crate::session_owner::preparation::{
        InspectPendingApprovalPreparation, PendingApprovalSource,
    };
    let (context, f) = Box::pin(staged(store, clock, unkeyed, false)).await;
    let mut claim = crate::approval_contract::fixture::claim(store, &f).await;
    if !recovery {
        return InspectPendingApprovalPreparation {
            context,
            source: PendingApprovalSource::Claim(Box::new(claim)),
        };
    }
    claim.lease_ms = 1;
    let joined = prepare(
        store,
        &f.run,
        PreparationSource::ApprovalClaim(claim.clone()),
        PreparedOperation::ApprovalClaim(claim.clone()),
        PreparationKey::Approval(claim.id.clone()),
        ExistingWorkTarget::ApprovalClaim(claim.continuation.clone()),
        claim.fingerprint().unwrap(),
    )
    .await;
    store.complete_preparation(joined).await.unwrap();
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let recovery = crate::ApprovalContinuationRecovery {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        original: claim,
        attempt: f.batch.next_attempt.clone(),
        owner: "pending-preflight".into(),
        lease_ms: 30000,
    };
    InspectPendingApprovalPreparation {
        context,
        source: PendingApprovalSource::Recovery(Box::new(recovery)),
    }
}
