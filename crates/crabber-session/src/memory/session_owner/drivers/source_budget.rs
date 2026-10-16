//! One caller allowance spans every borrowed original source.
use super::super::*;
use crate::public_session::v2::proof::Failure;
#[tokio::test]
async fn original_driver_source_preserves_shared_budget_failure() {
    for unkeyed in [false, true] {
        let store = MemoryStore::new();
        let admitted = if unkeyed {
            crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(&store)
                .await
        } else {
            crate::session_owner::preparation_contract::resources::initial_driver(&store).await
        };
        let request = RegisterExecutionDriver {
            context: crabber_core::SessionContext::new(
                admitted.session.id,
                admitted.session.workspace_id,
                admitted.session.directory,
            ),
            fence: admitted.fence,
        };
        let mut state = store.state.lock().unwrap();
        let link = fresh(
            &state,
            &request,
            store.clock.now(),
            &mut Proof::bounded(32 * 1024 * 1024),
            None,
        )
        .unwrap();
        let mut budget = Proof::bounded(32 * 1024 * 1024);
        original_read(&state, &link, &mut budget).unwrap();
        let used = budget.used();
        assert!(used > 0);
        original_read(&state, &link, &mut Proof::bounded(used)).unwrap();
        assert!(matches!(
            original_read(&state, &link, &mut Proof::bounded(used - 1)),
            Err(ReadError::Budget(Failure::Limited(
                crate::PublicSessionLimitV2::ProofBytes
            )))
        ));
        let join = state
            .preparation_admissions
            .get(&request.fence.run_id)
            .unwrap()
            .join_id()
            .unwrap();
        state
            .session_owners
            .erase_historical_audit_for_test(request.context.session_id(), &join);
        assert!(matches!(
            original_read(&state, &link, &mut Proof::bounded(32 * 1024 * 1024)),
            Err(ReadError::Domain(Error::UnknownStoreOutcome))
        ));
    }
}
