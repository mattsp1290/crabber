//! A protected initial reservation is preserved and actually reclaimed.
use super::{Clock, Error, ManualClock, Store};
use crate::{AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptTarget};
use crabber_core::{AttemptGeneration, AttemptId, ControlId};
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    for unkeyed in [false, true] {
        let (admitted, reserved, old) = reserved(store, clock, unkeyed).await;
        let stale = store.execution(admitted.fence.clone()).await.unwrap();
        let run = admitted.run.id;
        let session = admitted.session.id;
        let binding = store
            .execution_binding(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .capsule;
        clock.set(admitted.run.lease_until);
        let source = store
            .inspect_expired_run_preparation(super::InspectExpiredRunPreparation {
                session_id: session.clone(),
                expected_fence: admitted.fence.clone(),
                owner: "reclaimed initial parent".into(),
                lease: std::time::Duration::from_secs(30),
            })
            .await
            .unwrap();
        assert!(matches!(
            source.target().origin,
            crate::session_owner::preparation::ExpiredRunOrigin::InitialUnstarted { .. }
        ));
        assert_eq!(source.expected_revision(), reserved.revision);
        Box::pin(super::atomic(store, source.clone(), binding)).await;
        let actual = store.get_run(&run).await.unwrap().unwrap();
        assert!(
            stale
                .mutate_attempt(
                    AttemptMutation {
                        id: ControlId::new(),
                        expected_revision: store.session_revision(&session).await.unwrap(),
                        command: AttemptCommand::Begin {
                            target: reserved.attempt.target.clone()
                        },
                    },
                    Some(old)
                )
                .await
                .is_err()
        );
        let execution = store
            .execution(crabber_core::RunFence {
                run_id: run.clone(),
                claim_token: actual.claim_token,
            })
            .await
            .unwrap();
        let AttemptMutationOutcome::Applied {
            authority: Some(authority),
            ..
        } = execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: store.session_revision(&session).await.unwrap(),
                    command: AttemptCommand::ReclaimUnstarted {
                        target: reserved.attempt.target.clone(),
                    },
                },
                None,
            )
            .await
            .unwrap()
        else {
            panic!("fresh reclaim, never metadata authority");
        };
        execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: store.session_revision(&session).await.unwrap(),
                    command: AttemptCommand::Begin {
                        target: reserved.attempt.target,
                    },
                },
                Some(authority),
            )
            .await
            .unwrap();
        assert!(super::lookup(store, &source).await.is_some());
    }
    revision_changed(store, clock).await;
}
pub(crate) async fn reserved(
    store: &impl Store,
    clock: &ManualClock,
    unkeyed: bool,
) -> (
    crate::AdmitOutcome,
    crate::AttemptReceipt,
    crate::AttemptAuthority,
) {
    let admitted = if unkeyed {
        super::owned_admission::admit(store, clock).await
    } else {
        let bound = super::bound(&crabber_core::SessionId::new(), clock.now());
        let crate::KeyedAdmitOutcome::Started { admitted, .. } =
            store.admit_bound_keyed_run(bound).await.unwrap()
        else {
            panic!("new keyed admission");
        };
        store
            .execution(admitted.fence.clone())
            .await
            .unwrap()
            .begin_admission_execution()
            .await
            .unwrap();
        *admitted
    };
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let AttemptMutationOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                command: AttemptCommand::ReserveBound {
                    target: AttemptTarget {
                        id: AttemptId::new(),
                        generation: AttemptGeneration::INITIAL,
                    },
                },
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("genuine initial reservation");
    };
    (admitted, receipt, authority)
}
async fn revision_changed(store: &impl Store, clock: &ManualClock) {
    let (admitted, receipt, _) = reserved(store, clock, true).await;
    clock.set(admitted.run.lease_until);
    let request = super::InspectExpiredRunPreparation {
        session_id: admitted.session.id.clone(),
        expected_fence: admitted.fence,
        owner: "refused".into(),
        lease: std::time::Duration::from_secs(30),
    };
    let original = store
        .inspect_expired_run_preparation(request.clone())
        .await
        .unwrap();
    store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: admitted.session.id.clone(),
            expected_revision: receipt.revision,
            change: crate::SessionChange::Rename {
                title: "unrelated public change".into(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        store.inspect_expired_run_preparation(request.clone()).await,
        Err(Error::RevisionConflict)
    ));
    let revision = store.session_revision(&admitted.session.id).await.unwrap();
    let forged = super::ExpiredRunPreparation::from_verified_source(
        request,
        revision,
        original.target().clone(),
    )
    .unwrap();
    let binding = store
        .execution_binding(&admitted.session.id, &admitted.run.id)
        .await
        .unwrap()
        .unwrap()
        .capsule;
    let mut request = super::reservation(&original, &binding);
    request.expected_revision = revision;
    request.intent = forged.fingerprint().unwrap();
    request.source = Some(super::PreparationSource::ExpiredRun(forged));
    assert!(matches!(
        request.fingerprint(),
        Err(Error::RevisionConflict)
    ));
    assert!(matches!(
        store.reserve_preparation(request).await,
        Err(Error::RevisionConflict)
    ));
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
}
#[cfg(test)]
pub(crate) async fn pending(store: &impl Store, clock: &ManualClock) -> super::Pending {
    let (admitted, _, _) = reserved(store, clock, true).await;
    clock.set(admitted.run.lease_until);
    let source = store
        .inspect_expired_run_preparation(super::InspectExpiredRunPreparation {
            session_id: admitted.session.id,
            expected_fence: admitted.fence,
            owner: "unstarted cold claim".into(),
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
    let joined = super::prepare(store, &source, binding).await;
    super::Pending { source, joined }
}
#[cfg(test)]
pub(crate) async fn faults(store: &impl Store, clock: &ManualClock, inject: impl Fn(u8)) {
    for point in [1, 2] {
        let pending = Box::pin(self::pending(store, clock)).await;
        inject(point);
        assert!(
            Box::pin(store.complete_preparation(pending.joined.clone()))
                .await
                .is_err()
        );
        inject(0);
        assert_eq!(
            super::lookup(store, &pending.source).await.is_some(),
            point == 2
        );
        let outcome = Box::pin(store.complete_preparation(pending.joined))
            .await
            .unwrap();
        assert_eq!(outcome.is_replayed(), point == 2);
        assert!(super::lookup(store, &pending.source).await.is_some());
    }
}
