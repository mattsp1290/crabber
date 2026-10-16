//! Actual native Join, original source, and complete Run policy are separate.
use super::*;
use crate::session_owner::driver::history_fixture;
#[tokio::test]
async fn selected_driver_join_requires_actual_join_and_original_sources() {
    for unkeyed in [false, true] {
        for dispatch in [false, true] {
            let store = std::sync::Arc::new(crate::MemoryStore::new());
            let f = history_fixture::joined(store.clone(), unkeyed, dispatch).await;
            let mut state = store.state.lock().unwrap();
            let record = state
                .session_owners
                .source_audit(f.request.context.session_id(), &f.join)
                .unwrap()
                .record
                .clone();
            let mut proof = Proof::bounded(32 * 1024 * 1024);
            assert!(classified(&state, &record, &mut proof).unwrap());
            let used = proof.used();
            assert!(classified(&state, &record, &mut Proof::bounded(used)).unwrap());
            assert!(matches!(
                classified(&state, &record, &mut Proof::bounded(used - 1)),
                Err(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))
            ));
            let mut label_only = record.clone();
            label_only.owned_join = None;
            label_only.owned_scope = None;
            assert!(!classified(&state, &label_only, &mut Proof::bounded(used)).unwrap());
            let join = state
                .preparation_admissions
                .get(&f.request.fence.run_id)
                .unwrap()
                .join_id()
                .unwrap();
            state
                .session_owners
                .erase_historical_audit_for_test(f.request.context.session_id(), &join);
            assert!(matches!(
                classified(&state, &record, &mut Proof::bounded(32 * 1024 * 1024)),
                Err(Failure::Error(_))
            ));
        }
    }
}
