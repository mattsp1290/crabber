//! Real quiet history through the ordinary Store, then the guarded coordinator.
use crate::{DeleteSession, DeletionBlocker, DeletionError, DeletionOutcome, Store};
use crabber_core::{ControlId, SessionId, SessionRevision};

pub(crate) async fn run(
    store: &impl Store,
    delete: impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
) {
    let missing = DeleteSession {
        id: ControlId::new(),
        session_id: SessionId::new(),
        expected_revision: SessionRevision::INITIAL,
    };
    assert_eq!(
        delete(missing.clone()).await.unwrap_err(),
        DeletionError::NotFound
    );
    assert_eq!(
        store
            .session_disposition(&missing.session_id)
            .await
            .unwrap(),
        crate::SessionDisposition::Missing
    );
    let fixture = crate::deletion::contract::cancelled_queue(store).await;
    let revision = store.session_revision(&fixture.session).await.unwrap();
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: fixture.session.clone(),
        expected_revision: revision,
    };
    let mut stale = request.clone();
    stale.expected_revision = SessionRevision::INITIAL;
    assert_eq!(
        delete(stale.clone()).await.unwrap_err(),
        DeletionError::RevisionConflict
    );
    let mut collision = request.clone();
    collision.id = fixture.enqueue.id.clone();
    assert_eq!(
        delete(collision).await.unwrap_err(),
        DeletionError::SemanticConflict
    );
    let DeletionOutcome::Applied(receipt) = delete(request.clone()).await.unwrap() else {
        panic!("actual fresh deletion")
    };
    assert_eq!(receipt.final_revision, revision.next().unwrap());
    assert_eq!(receipt.semantic_digest, request.fingerprint().unwrap());
    assert_eq!(
        delete(request.clone()).await.unwrap(),
        DeletionOutcome::Replayed(receipt.clone())
    );
    assert_eq!(
        delete(stale).await.unwrap_err(),
        DeletionError::SemanticConflict
    );
    let mut changed_key = request.clone();
    changed_key.id = ControlId::new();
    assert_eq!(
        delete(changed_key).await.unwrap_err(),
        DeletionError::Deleted
    );
    let saved = crate::deletion::Tombstone {
        format_version: 1,
        expected_revision: revision,
        receipt,
    };
    crate::deletion::contract::metadata(store, &saved).await;
    crate::deletion::contract::creation_guards(store, &saved).await;
    crate::deletion::contract::queue_guards(store, &fixture.session, Some(&fixture)).await;

    blockers(store, &delete).await;
}
async fn blockers(
    store: &impl Store,
    delete: &impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
) {
    // A genuine pending queue is not quiescent, even with positive creation.
    let pending = crate::queue_contract::fixture(store).await;
    store.mutate_queue(pending.enqueue).await.unwrap();
    let pending_request = DeleteSession {
        id: ControlId::new(),
        session_id: pending.session.clone(),
        expected_revision: store.session_revision(&pending.session).await.unwrap(),
    };
    assert_eq!(
        delete(pending_request.clone()).await.unwrap(),
        DeletionOutcome::Blocked(DeletionBlocker::Queue)
    );
    assert!(store.get_session(&pending.session).await.unwrap().is_some());
    assert!(
        store
            .lookup_session_deletion(&pending.session, &pending_request.id)
            .await
            .unwrap()
            .is_none()
    );

    // Ordinary admission has no positive preparation proof and is never
    // certified by a locally empty owner collection.
    let active = crate::queue_contract::fixture(store).await;
    store.admit_bound_keyed_run(active.prepared).await.unwrap();
    let active_request = DeleteSession {
        id: ControlId::new(),
        session_id: active.session.clone(),
        expected_revision: store.session_revision(&active.session).await.unwrap(),
    };
    assert_eq!(
        delete(active_request).await.unwrap(),
        DeletionOutcome::Blocked(DeletionBlocker::Runtime)
    );
}

pub(crate) async fn faults(
    store: &impl Store,
    delete: impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
    fault: impl Fn(u8),
) {
    for point in [1, 2] {
        let fixture = crate::deletion::contract::cancelled_queue(store).await;
        let request = DeleteSession {
            id: ControlId::new(),
            session_id: fixture.session.clone(),
            expected_revision: store.session_revision(&fixture.session).await.unwrap(),
        };
        fault(point);
        assert_eq!(
            delete(request.clone()).await.unwrap_err(),
            DeletionError::UnknownStoreOutcome
        );
        if point == 1 {
            assert!(store.get_session(&fixture.session).await.unwrap().is_some());
            assert!(
                store
                    .lookup_session_deletion(&fixture.session, &request.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                store.session_revision(&fixture.session).await.unwrap(),
                request.expected_revision
            );
            fault(0);
            assert!(matches!(
                delete(request).await.unwrap(),
                DeletionOutcome::Applied(_)
            ));
        } else {
            assert!(store.get_session(&fixture.session).await.unwrap().is_none());
            let original = store
                .lookup_session_deletion(&fixture.session, &request.id)
                .await
                .unwrap()
                .unwrap();
            // Re-arm the consumed ACK fault: replay must not visit a write seam.
            fault(2);
            assert_eq!(
                delete(request).await.unwrap(),
                DeletionOutcome::Replayed(original)
            );
            fault(0);
        }
    }
}

pub(crate) async fn races(
    store: &impl Store,
    delete: impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
) {
    let fixture = crate::deletion::contract::cancelled_queue(store).await;
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: fixture.session.clone(),
        expected_revision: store.session_revision(&fixture.session).await.unwrap(),
    };
    let (left, right) = tokio::join!(delete(request.clone()), delete(request.clone()));
    let left = left.unwrap();
    let right = right.unwrap();
    assert!(
        matches!((&left, &right), (DeletionOutcome::Applied(a), DeletionOutcome::Replayed(b)) | (DeletionOutcome::Replayed(a), DeletionOutcome::Applied(b)) if a == b)
    );

    let fixture = crate::deletion::contract::cancelled_queue(store).await;
    let left = DeleteSession {
        id: ControlId::new(),
        session_id: fixture.session.clone(),
        expected_revision: store.session_revision(&fixture.session).await.unwrap(),
    };
    let mut right = left.clone();
    right.id = ControlId::new();
    let (left, right) = tokio::join!(delete(left), delete(right));
    assert!(matches!(
        (&left, &right),
        (Ok(DeletionOutcome::Applied(_)), Err(DeletionError::Deleted))
            | (Err(DeletionError::Deleted), Ok(DeletionOutcome::Applied(_)))
    ));
}

pub(crate) async fn mutation_races(
    store: &impl Store,
    delete: impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
) {
    for kind in 0..3 {
        let fixture = crate::deletion::contract::cancelled_queue(store).await;
        let revision = store.session_revision(&fixture.session).await.unwrap();
        let request = DeleteSession {
            id: ControlId::new(),
            session_id: fixture.session.clone(),
            expected_revision: revision,
        };
        match kind {
            0 => {
                let mutation = crate::ManageSession {
                    id: ControlId::new(),
                    session_id: fixture.session.clone(),
                    expected_revision: revision,
                    change: crate::SessionChange::Rename {
                        title: "race".into(),
                    },
                };
                let (deleted, renamed) =
                    tokio::join!(delete(request), store.manage_session(mutation));
                assert!(
                    matches!(
                        (&deleted, &renamed),
                        (
                            Ok(DeletionOutcome::Applied(_)),
                            Err(crate::ManagementError::Deleted)
                        ) | (Err(DeletionError::RevisionConflict), Ok(_))
                    ),
                    "rename race at revision {revision:?}: delete={deleted:?}, rename={renamed:?}"
                );
            }
            1 => {
                // The queue fixture's original proposal predates Enqueue and
                // Cancel. This ordinary admission needs a fresh canonical R.
                let prepared = crate::queue_contract::entry(store, fixture.session.clone())
                    .await
                    .prepared;
                let (deleted, admitted) =
                    tokio::join!(delete(request), store.admit_bound_keyed_run(prepared));
                assert!(
                    matches!(
                        (&deleted, &admitted),
                        (
                            Ok(DeletionOutcome::Applied(_)),
                            Err(crate::StoreError::SessionDeleted)
                        ) | (Err(DeletionError::RevisionConflict), Ok(_))
                    ),
                    "admission race at revision {revision:?}: delete={deleted:?}, admission={admitted:?}"
                );
            }
            _ => {
                let reservation = owner_reservation(store, &fixture.session).await;
                let (deleted, reserved) = tokio::join!(
                    delete(request),
                    store.reserve_session_owner(reservation, None)
                );
                assert!(
                    matches!(
                        (&deleted, &reserved),
                        (
                            Ok(DeletionOutcome::Applied(_)),
                            Err(crate::session_owner::SessionOwnerError::Deleted)
                        ) | (
                            Err(DeletionError::RevisionConflict),
                            Ok(crate::session_owner::SessionOwnerOutcome::Applied { .. })
                        )
                    ),
                    "owner race at revision {revision:?}: delete={deleted:?}, reserve={reserved:?}"
                );
            }
        }
    }
    owner_orderings(store, &delete).await;
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn forks(
    store: &impl Store,
    delete: impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
) {
    let mut fixture = crate::fork_pending_contract::fixture(store).await;
    let crate::PhaseControlOutcome::Applied(joined) = store
        .record_phase_cleanup(crate::PhaseCleanup {
            session_id: fixture.request.source_session_id.clone(),
            run_id: fixture.run,
            id: ControlId::new(),
            expected_revision: store
                .session_revision(&fixture.request.source_session_id)
                .await
                .unwrap(),
            target: fixture.target,
            result: crate::PhaseQuiescenceResult::Complete,
            claim: crate::PhaseResolverClaim {
                resolver_id: "deletion-test".into(),
                authority_claim: "actual test source worker stopped and resources joined".into(),
            },
        })
        .await
        .unwrap()
    else {
        panic!("joined source")
    };
    fixture.request.expected_source_revision = joined.revision;
    let crate::ForkOutcome::Applied(fork) = store.fork_session(fixture.request).await.unwrap()
    else {
        panic!("closed imported history")
    };
    assert_ne!(
        store
            .list_all_messages(&fork.target.session_id)
            .await
            .unwrap(),
        []
    );
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: fork.target.session_id.clone(),
        expected_revision: fork.target.revision,
    };
    let DeletionOutcome::Applied(receipt) = delete(request.clone()).await.unwrap() else {
        panic!("positive quiet fork")
    };
    assert!(
        store
            .get_session(&fork.source_session_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .list_all_messages(&fork.target.session_id)
            .await
            .unwrap(),
        []
    );
    assert_eq!(
        delete(request).await.unwrap(),
        DeletionOutcome::Replayed(receipt)
    );
}

async fn owner_reservation(
    store: &impl Store,
    session: &SessionId,
) -> crate::session_owner::ReserveSessionOwner {
    let summary = store.session_summary(session).await.unwrap();
    let mut reservation = crate::session_owner_contract::reservation(session.clone());
    reservation.binding.workspace_id = summary.workspace_id;
    reservation.binding.directory = summary.directory;
    reservation
}

// Establish both serialized winners deterministically in addition to the
// uncontrolled concurrent race above. Owner publication MUST advance CAS.
async fn owner_orderings(
    store: &impl Store,
    delete: &impl AsyncFn(DeleteSession) -> Result<DeletionOutcome, DeletionError>,
) {
    use crate::session_owner::{SessionOwnerError, SessionOwnerOutcome};
    for reserve_first in [true, false] {
        let fixture = crate::deletion::contract::cancelled_queue(store).await;
        let revision = store.session_revision(&fixture.session).await.unwrap();
        let request = DeleteSession {
            id: ControlId::new(),
            session_id: fixture.session.clone(),
            expected_revision: revision,
        };
        let reservation = owner_reservation(store, &fixture.session).await;
        if reserve_first {
            let reserved = store
                .reserve_session_owner(reservation, None)
                .await
                .unwrap();
            assert!(
                matches!(reserved, SessionOwnerOutcome::Applied { .. }),
                "fresh reserve winner: {reserved:?}"
            );
            let published = store.session_revision(&fixture.session).await.unwrap();
            assert_eq!(
                published,
                revision.next().unwrap(),
                "Reserve publishes exactly one public revision"
            );
            assert_eq!(
                delete(request.clone()).await.unwrap_err(),
                DeletionError::RevisionConflict
            );
            assert!(
                store
                    .lookup_session_deletion(&fixture.session, &request.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let current = DeleteSession {
                id: ControlId::new(),
                expected_revision: published,
                ..request
            };
            assert_eq!(
                delete(current).await.unwrap(),
                DeletionOutcome::Blocked(DeletionBlocker::SessionOwner)
            );
            assert_eq!(
                store.session_revision(&fixture.session).await.unwrap(),
                published
            );
        } else {
            let DeletionOutcome::Applied(receipt) = delete(request).await.unwrap() else {
                panic!("fresh delete winner")
            };
            assert_eq!(receipt.final_revision, revision.next().unwrap());
            assert_eq!(
                store
                    .reserve_session_owner(reservation, None)
                    .await
                    .unwrap_err(),
                SessionOwnerError::Deleted
            );
            assert_eq!(
                store.session_disposition(&fixture.session).await.unwrap(),
                crate::SessionDisposition::Deleted {
                    final_revision: receipt.final_revision,
                    deleted_at: receipt.deleted_at
                }
            );
        }
    }
}
