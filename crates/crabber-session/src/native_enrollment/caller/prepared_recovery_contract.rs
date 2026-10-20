//! Original joined tasks, native source witnesses, and whole transaction rollback.
use super::Harness;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparedAdmissionRecovery, PreparedOperation,
};
use crabber_core::{ManualClock, RunId, SessionId};
use std::sync::Arc;

pub(crate) async fn run(store: &impl Harness, clock: &Arc<ManualClock>) {
    for expired in [false, true] {
        for origin in 0..if expired { 4 } else { 1 } {
            for lost in [false, true] {
                let (session, run, joined) = fixture(store, clock, expired, origin).await;
                cell(store, &session, &run, joined, expired, lost).await;
            }
        }
    }
}

async fn fixture(
    store: &impl Harness,
    clock: &Arc<ManualClock>,
    expired: bool,
    origin: usize,
) -> (SessionId, RunId, JoinedPreparation) {
    if expired {
        use crate::run_control_contract::expired;
        let pending = match origin {
            0 => Box::pin(expired::pending(store, clock)).await,
            1 => Box::pin(expired::unkeyed::pending(store, clock)).await,
            2 => Box::pin(expired::unstarted::pending(store, clock)).await,
            3 => Box::pin(expired::pending_keyed(store, clock)).await,
            _ => unreachable!(),
        };
        let request = pending.source.request();
        return (
            request.session_id.clone(),
            request.expected_fence.run_id.clone(),
            pending.joined,
        );
    }
    let (source, grant, bound) =
        crate::admission_execution_contract::inspection::reserved_fixture(store, clock).await;
    let request = source.request();
    let (session, run) = (
        request.session_id.clone(),
        request.expected_fence.run_id.clone(),
    );
    let ((), joined) = JoinedPreparation::observe(
        grant,
        tokio::spawn(async move {
            Ok::<_, ()>((
                (),
                PreparedOperation::AdmissionRecovery(Box::new(PreparedAdmissionRecovery {
                    source,
                    binding: Box::new(bound),
                })),
            ))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    (session, run, joined)
}

fn arm(store: &impl Harness, session: &SessionId, run: &RunId, expired: bool, refuse: bool) {
    if expired {
        store.probe().arm_expired_recovery(session, run, refuse);
    } else {
        store.probe().arm_admission_recovery(session, run, refuse);
    }
}

async fn cell(
    store: &impl Harness,
    session: &SessionId,
    run: &RunId,
    joined: JoinedPreparation,
    expired: bool,
    lost: bool,
) {
    let before = store.initial_retained(session).await;
    let original = store.retained(session, run, true).await;
    arm(store, session, run, expired, true);
    assert!(store.complete_preparation(joined.clone()).await.is_err());
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.initial_retained(session).await, before);
    let mut measured = Proof::bounded(32 << 20);
    assert!(
        store
            .complete_with_proof(joined.clone(), &mut measured)
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 2);
    arm(store, session, run, expired, false);
    assert!(
        store
            .complete_with_proof(joined.clone(), &mut Proof::bounded(measured.used() - 1))
            .await
            .is_err()
    );
    assert_eq!(
        store.probe().hits(),
        [] as [crabber_core::InputFingerprint; 0]
    );
    assert_eq!(store.initial_retained(session).await, before);
    store.initial_fault(if lost { 2 } else { 0 });
    let result = if lost {
        store.complete_preparation(joined.clone()).await
    } else {
        store
            .complete_with_proof(joined.clone(), &mut Proof::bounded(measured.used()))
            .await
    };
    store.initial_fault(0);
    assert_eq!(result.is_err(), lost, "expired={expired}");
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.retained(session, run, true).await, original);
    let committed = store.initial_retained(session).await;
    assert_ne!(committed, before);
    arm(store, session, run, expired, true);
    assert!(
        store
            .complete_preparation(joined)
            .await
            .unwrap()
            .is_replayed()
    );
    assert_eq!(
        store.probe().hits(),
        [] as [crabber_core::InputFingerprint; 0]
    );
    assert_eq!(store.initial_retained(session).await, committed);
}
