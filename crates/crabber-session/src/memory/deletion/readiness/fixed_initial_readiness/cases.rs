use super::*;
use std::sync::atomic::Ordering;
fn source() -> crabber_extension::LibraryScriptedSource {
    crabber_extension::LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "library".into(),
            model_id: "text".into(),
        },
        vec!["must not dispatch".into()],
    )
    .unwrap()
}
async fn deletion(
    host: &FixedMemoryInitialHost,
    session: &SessionId,
) -> Result<crate::DeletionOutcome, Error> {
    host.store()
        .delete_session(crate::DeleteSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: host.store().session_revision(session).await.unwrap(),
        })
        .await
}
#[tokio::test]
async fn actual_no_open_and_closed_no_publication_are_uncovered_not_absent() {
    for fault in [1, 2] {
        let (host, session) = completed(false, 1).await;
        let before: Vec<_> = host
            .store()
            .state
            .lock()
            .unwrap()
            .pending_initial
            .epoch_entries()
            .map(|(id, _)| id.clone())
            .collect();
        let revision = host.store().session_revision(&session).await.unwrap();
        host.store()
            .pending_initial_test_state
            .fault
            .store(fault, Ordering::SeqCst);
        let mut task = host
            .register_scripted(
                FixedMemoryInitialInput {
                    context: SessionContext::new(session.clone(), "workspace", "/workspace"),
                    title: "unpublished source".into(),
                    input: InputEnvelope::text("do not publish", revision).unwrap(),
                    key: None,
                },
                source(),
            )
            .unwrap();
        task.start().unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), task.join())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::Unresolved)
        );
        let mut retry = observed
            .take_retry()
            .expect("only same original Seal retry");
        host.store()
            .pending_initial_test_state
            .fault
            .store(0, Ordering::SeqCst);
        assert_eq!(
            retry.retry().await.unwrap(),
            PendingInitialProducerStatus::ClosedNoPublication
        );
        drop(task);
        host.wait_for_supervision().await;
        {
            let state = host.store().state.lock().unwrap();
            let (_, epoch) = state
                .pending_initial
                .epoch_entries()
                .find(|(id, _)| !before.contains(id))
                .unwrap();
            let birth = state
                .pending_initial
                .selected(
                    &epoch.correlation.key,
                    &epoch.correlation.identity,
                    &mut Proof::bounded(MAX_PROOF_BYTES),
                )
                .unwrap();
            assert!(birth.assignment.is_none() && birth.close.is_some());
            let view = crate::memory::initial_consumer::selected(
                &state,
                &epoch.correlation,
                &mut Proof::bounded(MAX_PROOF_BYTES),
            )
            .unwrap();
            if fault == 1 {
                assert!(matches!(
                    view.origin,
                    Some(crate::native_enrollment::initial_consumer::Origin::NoOpenedMember(_))
                ));
            } else {
                assert!(matches!(
                    view.origin,
                    Some(crate::native_enrollment::initial_consumer::Origin::Opened(
                        _
                    ))
                ));
            }
            assert!(
                pending_uncovered(&state, &session),
                "reverse Pending fold itself must reject extra source"
            );
            assert_eq!(
                ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
                Some(Blocker::OwnershipUnknown)
            );
        }
        assert_eq!(
            deletion(&host, &session).await.unwrap(),
            crate::DeletionOutcome::Blocked(Blocker::OwnershipUnknown)
        );
    }
}
#[tokio::test]
async fn genuine_terminal_raw_run_cannot_borrow_completed_fixed_coverage() {
    let (host, session) = completed(true, 1).await;
    let mut request = crate::deletion::contract::admission(&session);
    request.session_id = Some(session.clone());
    request.workspace_id = "workspace".into();
    request.directory = "/workspace".into();
    let admitted = host.store().admit_run(request).await.unwrap();
    let event = crabber_core::EventRecord {
        cursor: None,
        session_id: session.clone(),
        run_id: admitted.run.id.clone(),
        turn_id: None,
        kind: crabber_core::EventKind::RunSettled,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    host.store()
        .execution(admitted.fence)
        .await
        .unwrap()
        .settle_run(
            crabber_core::RunStatus::Interrupted,
            None,
            crabber_core::Usage::default(),
            event,
        )
        .await
        .unwrap();
    assert_eq!(
        deletion(&host, &session).await.unwrap(),
        crate::DeletionOutcome::Blocked(Blocker::OwnershipUnknown)
    );
    let state = host.store().state.lock().unwrap();
    let checked = crate::memory::deletion::run_history::checked(
        &state,
        &session,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    )
    .unwrap();
    assert!(checked.uncovered_runs());
    assert_eq!(checked.sources().len(), 1);
}
#[tokio::test]
async fn real_live_owner_then_pending_input_preserve_earlier_blockers() {
    let (host, session) = completed(false, 1).await;
    let mut reservation = crate::session_owner_contract::reservation(session.clone());
    reservation.binding.workspace_id = "workspace".into();
    reservation.binding.directory = "/workspace".into();
    reservation.lease_ms = 30_000;
    host.store()
        .reserve_session_owner(reservation, None)
        .await
        .unwrap();
    assert_eq!(
        deletion(&host, &session).await.unwrap(),
        crate::DeletionOutcome::Blocked(Blocker::SessionOwner)
    );
    let message = crate::deletion::contract::admission(&session).user_message;
    host.store()
        .enqueue_inbox(&session, crate::InboxKind::FollowUp, message)
        .await
        .unwrap();
    assert_eq!(
        deletion(&host, &session).await.unwrap(),
        crate::DeletionOutcome::Blocked(Blocker::PendingInput)
    );
}
#[tokio::test]
async fn actual_fixed_deletion_preserves_revision_and_permanent_tombstone() {
    let (host, session) = completed(false, 1).await;
    let revision = host.store().session_revision(&session).await.unwrap();
    assert_ne!(revision, SessionRevision::INITIAL);
    assert_eq!(
        host.store()
            .delete_session(crate::DeleteSession {
                id: ControlId::new(),
                session_id: session.clone(),
                expected_revision: SessionRevision::INITIAL
            })
            .await
            .unwrap_err(),
        Error::RevisionConflict
    );
    assert_eq!(
        host.store().session_revision(&session).await.unwrap(),
        revision
    );
    assert!(matches!(
        deletion(&host, &session).await.unwrap(),
        crate::DeletionOutcome::Applied(_)
    ));
    assert_eq!(
        host.store()
            .delete_session(crate::DeleteSession {
                id: ControlId::new(),
                session_id: session.clone(),
                expected_revision: revision
            })
            .await
            .unwrap_err(),
        Error::Deleted
    );
    assert_eq!(
        host.store().session_revision(&session).await.unwrap_err(),
        crate::StoreError::SessionDeleted
    );
    let message = crate::deletion::contract::admission(&session).user_message;
    assert_eq!(
        host.store()
            .enqueue_inbox(&session, crate::InboxKind::Steer, message)
            .await
            .unwrap_err(),
        crate::StoreError::SessionDeleted
    );
    assert!(host.store().get_session(&session).await.unwrap().is_none());
}
