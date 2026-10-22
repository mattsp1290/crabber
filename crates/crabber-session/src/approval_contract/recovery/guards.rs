use super::{
    ApprovalClaimOutcome, ApprovalError, ControlId, Duration, ManualClock, OffsetDateTime, Store,
    evidence, prepare,
};
pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    for native_live in [false, true] {
        let p = prepare(store, clock).await;
        clock.set(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(31));
        // A legacy fence claim changes only run ownership. Renewing that fence
        // cannot renew a native attempt still bound to the old run token.
        let fence = store
            .claim_expired_run(&p.original.run_id, "lease-skew-fixture")
            .await
            .unwrap();
        if native_live {
            clock.set(OffsetDateTime::UNIX_EPOCH);
            store
                .execution(fence)
                .await
                .unwrap()
                .renew_lease(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(10))
                .await
                .unwrap();
            clock.set(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(11));
        }
        let before = evidence(store, &p).await;
        assert_eq!(
            store
                .reclaim_approval_continuation(p.request.clone())
                .await
                .unwrap_err(),
            ApprovalError::LiveOwner
        );
        assert_eq!(evidence(store, &p).await, before);
    }
    let p = prepare(store, clock).await;
    clock.set(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1));
    let before = evidence(store, &p).await;
    for field in 0..8 {
        let mut bad = p.request.clone();
        match field {
            0 => bad.original.id = ControlId::new(),
            1 => bad.original.session_id = crabber_core::SessionId::new(),
            2 => bad.original.run_id = crabber_core::RunId::new(),
            3 => {
                bad.original.continuation.generation =
                    bad.original.continuation.generation.next().unwrap();
            }
            4 => bad.attempt.generation = bad.attempt.generation.next().unwrap(),
            5 => {
                bad.original.expected_plan =
                    crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
            }
            6 => bad.expected_revision = bad.expected_revision.next().unwrap(),
            _ => bad.original.expected_revision = bad.original.expected_revision.next().unwrap(),
        }
        assert!(store.reclaim_approval_continuation(bad).await.is_err());
        assert_eq!(evidence(store, &p).await, before);
    }
    let (a, b) = futures::join!(
        store.reclaim_approval_continuation(p.request.clone()),
        store.reclaim_approval_continuation(p.request.clone())
    );
    assert_eq!(
        [&a, &b]
            .iter()
            .filter(|r| matches!(r, Ok(ApprovalClaimOutcome::Applied { .. })))
            .count(),
        1
    );
    assert_eq!(
        [&a, &b]
            .iter()
            .filter(|r| matches!(r, Ok(ApprovalClaimOutcome::Replayed(_))))
            .count(),
        1
    );
    let mut changed = p.request.clone();
    changed.original.id = ControlId::new();
    assert_eq!(
        store
            .reclaim_approval_continuation(changed)
            .await
            .unwrap_err(),
        ApprovalError::SemanticConflict
    );
    let p = prepare(store, clock).await;
    clock.set(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1));
    let mut other = p.request.clone();
    other.id = ControlId::new();
    let (a, b) = futures::join!(
        store.reclaim_approval_continuation(p.request),
        store.reclaim_approval_continuation(other)
    );
    assert_eq!([&a, &b].iter().filter(|r| r.is_ok()).count(), 1);
    assert!(matches!(
        (a, b),
        (Ok(_), Err(ApprovalError::RevisionConflict))
            | (Err(ApprovalError::RevisionConflict), Ok(_))
    ));
}
