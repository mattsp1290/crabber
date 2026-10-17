use crate::Store;
#[tokio::test]
async fn handoff_refuses_changed_predecessor_run_plan_before_mutation() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for domain in [
        crate::continuation_handoff::Domain::Approval,
        crate::continuation_handoff::Domain::Tool,
    ] {
        let (mut run, parent) =
            crate::continuation_handoff::claimed_fixture(&store, &clock, domain).await;
        let original = run.clone();
        let revision = store.session_revision(&run.session_id).await.unwrap();
        let target = crate::ContinuationTarget {
            id: crabber_core::ContinuationId::new(),
            generation: crabber_core::ContinuationGeneration::INITIAL,
        };
        let operation = crabber_core::ControlId::new();
        let digest = crabber_core::InputFingerprint::new("d".repeat(64)).unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        assert!(
            super::prepare(&mut tx, &run, domain, &target, &operation, &digest, &parent)
                .await
                .unwrap()
                .is_some()
        );
        run.plan_fingerprint = "e".repeat(64);
        assert!(matches!(
            super::prepare(&mut tx, &run, domain, &target, &operation, &digest, &parent).await,
            Err(crate::ApprovalError::InvalidTransition)
        ));
        tx.rollback().await.unwrap();
        assert_eq!(store.get_run(&run.id).await.unwrap().unwrap(), original);
        assert_eq!(
            store.session_revision(&run.session_id).await.unwrap(),
            revision
        );
    }
}
