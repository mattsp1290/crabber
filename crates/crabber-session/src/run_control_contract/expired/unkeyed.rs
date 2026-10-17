//! Explicit unkeyed original source, including metadata-only lost-ACK recovery.
use super::{
    Clock, Error, ExpiredRunPreparation, InspectExpiredRunPreparation, ManualClock, Store, atomic,
};
#[cfg(test)]
use super::{ExpiredRunOutcome, PreparationCompletion, lookup, prepare};
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let (source, binding) = source(store, clock).await;
    assert!(matches!(
        source.target().origin,
        crate::session_owner::preparation::ExpiredRunOrigin::UnkeyedAdmission(_)
    ));
    atomic(store, source, binding).await;
    legacy(store, clock).await;
}
pub(crate) async fn source(
    store: &impl Store,
    clock: &ManualClock,
) -> (ExpiredRunPreparation, crate::ExecutionBindingCapsule) {
    let admitted = super::owned_admission::admit(store, clock).await;
    let session = admitted.session.id;
    let run = admitted.run.id;
    let binding = store
        .execution_binding(&session, &run)
        .await
        .unwrap()
        .unwrap()
        .capsule;
    let request = InspectExpiredRunPreparation {
        session_id: session,
        expected_fence: admitted.fence,
        owner: "unkeyed-expired".into(),
        lease: std::time::Duration::from_secs(30),
    };
    assert!(matches!(
        store.inspect_expired_run_preparation(request.clone()).await,
        Err(Error::LiveOwner)
    ));
    clock.set(admitted.run.lease_until);
    (
        store
            .inspect_expired_run_preparation(request)
            .await
            .unwrap(),
        binding,
    )
}
#[cfg(test)]
pub(crate) async fn faults(store: &impl Store, clock: &ManualClock, inject: impl Fn(u8)) {
    for point in [1, 2] {
        let (source, binding) = source(store, clock).await;
        let joined = prepare(store, &source, binding).await;
        inject(point);
        assert!(
            Box::pin(store.complete_preparation(joined.clone()))
                .await
                .is_err()
        );
        inject(0);
        assert_eq!(lookup(store, &source).await.is_some(), point == 2);
        let outcome = Box::pin(store.complete_preparation(joined.clone()))
            .await
            .unwrap();
        assert_eq!(outcome.is_replayed(), point == 2);
        let PreparationCompletion::ExpiredRun(
            ExpiredRunOutcome::Claimed { receipt, .. } | ExpiredRunOutcome::Replayed(receipt),
        ) = outcome
        else {
            panic!("exact expired domain");
        };
        assert_eq!(lookup(store, &source).await, Some(receipt));
    }
}

#[cfg(test)]
pub(crate) async fn lost_admission_ack(
    store: &impl Store,
    clock: &ManualClock,
    inject: impl Fn(u8),
) {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let joined = super::owned_admission::pending(store, clock, effects.clone()).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    inject(2);
    assert!(
        Box::pin(store.complete_preparation(joined.clone()))
            .await
            .is_err()
    );
    inject(0);
    let PreparationCompletion::AdmissionReplayed(receipt) =
        Box::pin(store.complete_preparation(joined)).await.unwrap()
    else {
        panic!("metadata only; no fence from lost ACK");
    };
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .admission_execution_state(&receipt.run_id)
            .await
            .unwrap(),
        None
    );
    assert!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .is_none()
    );
    // An explicit host recovery observes the actual current fence and expiry;
    // metadata replay itself never supplies a grant or dispatches a task.
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    clock.set(run.lease_until);
    let source = store
        .inspect_expired_run_preparation(InspectExpiredRunPreparation {
            session_id: receipt.session_id,
            expected_fence: crabber_core::RunFence {
                run_id: run.id,
                claim_token: run.claim_token,
            },
            owner: "explicit lost-ACK recovery".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    let binding = store
        .execution_binding(
            &source.request().session_id,
            &source.request().expected_fence.run_id,
        )
        .await
        .unwrap()
        .unwrap()
        .capsule;
    atomic(store, source, binding).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}
#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
pub(crate) async fn pending(store: &impl Store, clock: &ManualClock) -> super::Pending {
    let (source, binding) = source(store, clock).await;
    let joined = prepare(store, &source, binding).await;
    super::Pending { source, joined }
}

async fn legacy(store: &impl Store, clock: &ManualClock) {
    let session = crabber_core::SessionId::new();
    let mut bound = super::bound(&session, clock.now());
    bound.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(crate::BoundAdmitRequest {
            request: bound.keyed.request,
            binding: bound.binding,
        })
        .await
        .unwrap();
    clock.set(admitted.run.lease_until);
    assert!(matches!(
        store
            .inspect_expired_run_preparation(InspectExpiredRunPreparation {
                session_id: session,
                expected_fence: admitted.fence,
                owner: "legacy unkeyed".into(),
                lease: std::time::Duration::from_secs(30),
            })
            .await,
        Err(Error::Unsupported)
    ));
}
#[cfg(test)]
pub(crate) async fn assert_refuses(
    store: &impl Store,
    source: &ExpiredRunPreparation,
    binding: &crate::ExecutionBindingCapsule,
) {
    assert!(
        store
            .inspect_expired_run_preparation(source.request().clone())
            .await
            .is_err()
    );
    let request = super::reservation(source, binding);
    assert!(store.reserve_preparation(request.clone()).await.is_err());
    assert!(
        store
            .lookup_preparation(&source.request().session_id, &request.key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .session_revision(&source.request().session_id)
            .await
            .unwrap(),
        source.expected_revision()
    );
}
