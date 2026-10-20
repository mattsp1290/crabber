//! Finite native disposition requires an actual retained join or explicit cleanup.
use super::*;
pub(super) async fn retained(store: &impl Store, fault: &impl Fn(u8)) {
    let session = SessionId::new();
    let initial = fixture(&session, SessionRevision::INITIAL);
    let revision = create(store, &initial).await;
    let bound = fixture(&session, revision);
    let reservation = reservation(&bound);
    let grant = begin(store, &reservation).await;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = calls.clone();
    let original = bound.clone();
    let ((), retained) = JoinedPreparation::observe(
        grant,
        tokio::spawn(async move {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, ()>(((), PreparedOperation::AdmitKeyed(Box::new(original))))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    fault(1);
    assert!(store.complete_preparation(retained.clone()).await.is_err());
    fault(0);
    // This reuses the actual joined task output; it does not rerun preparation.
    let PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { receipt, .. }) =
        store.complete_preparation(retained.clone()).await.unwrap()
    else {
        panic!("fresh successor")
    };
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let published_revision = store.session_revision(&session).await.unwrap();
    assert!(published_revision > revision);
    assert_eq!(
        store
            .execution_binding(&session, &receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .capsule,
        bound.binding
    );
    assert!(
        matches!(store.complete_preparation(retained).await.unwrap(),
        PreparationCompletion::Keyed(KeyedAdmitOutcome::Replayed(saved)) if saved == receipt)
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        store.session_revision(&session).await.unwrap(),
        published_revision
    );
}
pub(super) async fn after_cleanup(store: &impl Store, session: &SessionId) {
    let revision = store.session_revision(session).await.unwrap();
    let mut next = fixture(session, revision);
    next.keyed.options.key =
        crabber_core::AdmissionKey::new("explicit-new-work-after-cleanup").unwrap();
    let digest = next.keyed.semantic_digest().unwrap();
    next.keyed.execution.as_mut().unwrap().semantic_digest = digest;
    let request = reservation(&next);
    let grant = begin(store, &request).await;
    let PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { receipt, .. }) = store
        .complete_preparation(joined(grant, next).await)
        .await
        .unwrap()
    else {
        panic!("new work after actual cleanup")
    };
    assert_eq!(receipt.session_id, *session);
    assert!(store.session_revision(session).await.unwrap() > revision);
}
