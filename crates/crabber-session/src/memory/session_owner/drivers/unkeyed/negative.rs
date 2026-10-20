//! A missing original join or a genuine legacy admission grants no dispatch.
use super::*;
#[tokio::test]
async fn unkeyed_original_join_erasure_refuses_register_activation_and_metadata_replay() {
    for activating in [false, true] {
        let (store, request) = setup().await;
        let grant = if activating {
            Some(register(&store, &request).await)
        } else {
            None
        };
        let revision = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        {
            let mut state = store.state.lock().unwrap();
            let witness = state
                .preparation_admissions
                .get(&request.fence.run_id)
                .unwrap();
            let join = witness.audit_ids().unwrap()[2].clone();
            state
                .session_owners
                .erase_historical_audit_for_test(request.context.session_id(), &join);
        }
        assert!(
            store
                .register_execution_driver(request.clone())
                .await
                .is_err()
        );
        if let Some(grant) = grant {
            activation_refuses(&store, grant).await;
        }
        assert_eq!(
            store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            revision
        );
    }
}
#[tokio::test]
async fn unkeyed_genuine_legacy_admission_cannot_infer_an_owned_source_from_absent_init() {
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let store = MemoryStore::with_clock(Arc::new(crabber_core::ManualClock::new(now)));
    let mut fixture = crate::typed_admission_contract::fixture(
        &crabber_core::SessionId::new(),
        now,
        "legacy-unkeyed",
    );
    fixture.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(crate::BoundAdmitRequest {
            request: fixture.keyed.request,
            binding: fixture.binding,
        })
        .await
        .unwrap();
    let request = RegisterExecutionDriver {
        context: crabber_core::SessionContext::new(
            admitted.session.id,
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    };
    assert!(matches!(
        store.register_execution_driver(request).await,
        Err(Error::Unsupported)
    ));
}
#[tokio::test]
async fn unkeyed_original_some_erasure_is_corruption_not_legacy_fallback() {
    let (store, request) = setup().await;
    {
        let mut state = store.state.lock().unwrap();
        let original = state
            .preparation_admissions
            .get(&request.fence.run_id)
            .unwrap()
            .audit_ids()
            .unwrap()[0]
            .clone();
        state
            .session_owners
            .erase_driver_original_declaration_for_test(request.context.session_id(), &original);
    }
    assert!(matches!(
        store.register_execution_driver(request).await,
        Err(Error::UnknownStoreOutcome)
    ));
}
