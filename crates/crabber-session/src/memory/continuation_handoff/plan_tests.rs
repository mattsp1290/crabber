use crate::Store;
#[tokio::test]
async fn handoff_refuses_changed_predecessor_run_plan_before_mutation() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::super::MemoryStore::with_clock(clock.clone());
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
        let state = store.state.lock().unwrap().clone();
        assert!(
            super::prepare(&state, &run, domain, &target, &operation, &digest, &parent)
                .unwrap()
                .is_some()
        );
        run.plan_fingerprint = "e".repeat(64);
        assert!(matches!(
            super::prepare(&state, &run, domain, &target, &operation, &digest, &parent),
            Err(crate::ApprovalError::InvalidTransition)
        ));

        assert_eq!(store.get_run(&run.id).await.unwrap().unwrap(), original);
        assert_eq!(
            store.session_revision(&run.session_id).await.unwrap(),
            revision
        );
    }
}
