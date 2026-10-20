//! Live inspection must precede factory work; original receipts remain replayable.
use super::*;
pub(crate) async fn owned_inspection(store: &dyn Store) {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let authority = claim(store, &f).await;
    let revision = store.session_revision(&f.session).await.unwrap();
    let context =
        crabber_core::SessionContext::new(f.session.clone(), "bound-workspace", "/bound/workspace");
    assert!(store.supports_owned_queue_preparation_inspection());
    let retained = store
        .inspect_owned_queue_preparation(&context, &authority, revision)
        .await
        .unwrap();
    assert_eq!(retained.summary.queue_id, f.queue);
    let wrong =
        crabber_core::SessionContext::new(f.session.clone(), "other-workspace", "/bound/workspace");
    assert!(matches!(
        store
            .inspect_owned_queue_preparation(&wrong, &authority, revision)
            .await,
        Err(StoreError::SessionIdentityMismatch)
    ));
    let stale = SessionRevision::INITIAL;
    assert!(matches!(
        store
            .inspect_owned_queue_preparation(&context, &authority, stale)
            .await,
        Err(StoreError::AdmissionRevisionConflict)
    ));
    let cancel = mutation(
        store,
        &f,
        QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    store.mutate_queue(cancel.clone()).await.unwrap();
    assert!(
        store
            .inspect_owned_queue_preparation(
                &context,
                &authority,
                store.session_revision(&f.session).await.unwrap()
            )
            .await
            .is_err()
    );
    for original in [f.enqueue, cancel] {
        assert!(matches!(
            store.mutate_queue(original.clone()).await.unwrap(),
            QueueOutcome::Replayed(_)
        ));
        assert!(
            store
                .lookup_queue(&f.session, &original.id)
                .await
                .unwrap()
                .is_some()
        );
    }
}

/// Construct only the predecessor's original mutable rows. This fixture omits
/// the new marker on INSERT so migration-default legacy compatibility is tested.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn legacy_rows(
    store: &dyn Store,
) -> (Fixture, crate::queue::Record, crate::queue::Audit) {
    let f = fixture(store).await;
    let summary = store.queue_preparation_snapshot(&f.session).await.unwrap();
    let change = crate::queue::apply_guarded(
        &f.enqueue,
        &summary,
        None,
        None,
        1,
        OffsetDateTime::UNIX_EPOCH,
        None,
    )
    .unwrap();
    let receipt = QueueReceipt {
        session_id: f.session.clone(),
        operation_id: f.enqueue.id.clone(),
        semantic_digest: f.enqueue.fingerprint().unwrap(),
        revision: SessionRevision::new(summary.revision.get() + 1).unwrap(),
        summary: change.record.summary.clone(),
    };
    let audit = crate::queue::Audit {
        request: f.enqueue.clone(),
        receipt,
        authority: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    (f, change.record, audit)
}
