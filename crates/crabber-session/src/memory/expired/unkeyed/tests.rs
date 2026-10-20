//! Coherent current binding is insufficient without the independently original input.
use crate::session_owner::preparation::ExpiredRunOrigin;
use crate::{MemoryStore, Store};
use crabber_core::ManualClock;
use std::sync::Arc;
#[tokio::test]
async fn unkeyed_expired_source_rejects_coherent_binding_and_missing_original_join() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    let (source, binding) =
        crate::run_control_contract::expired::unkeyed::source(&store, &clock).await;
    let session = &source.request().session_id;
    let run_id = &source.request().expected_fence.run_id;
    let original = store.get_run(run_id).await.unwrap().unwrap();
    let raw = store
        .state
        .lock()
        .unwrap()
        .execution_bindings
        .get(run_id)
        .unwrap()
        .clone();
    let mut retained = crate::RetainedExecutionBinding::decode(&raw).unwrap();
    retained.capsule.plan_fingerprint =
        crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    retained.capsule.input_fingerprint = retained.capsule.computed_input_fingerprint().unwrap();
    let mut changed = original.clone();
    changed.plan_fingerprint = retained.capsule.plan_fingerprint.as_str().into();
    {
        let mut state = store.state.lock().unwrap();
        state.runs.insert(run_id.clone(), changed);
        state
            .execution_bindings
            .insert(run_id.clone(), retained.encode().unwrap());
    }
    assert!(
        store
            .execution_binding(session, run_id)
            .await
            .unwrap()
            .is_some()
    );
    crate::run_control_contract::expired::unkeyed::assert_refuses(&store, &source, &binding).await;
    {
        let mut state = store.state.lock().unwrap();
        state.runs.insert(run_id.clone(), original);
        state.execution_bindings.insert(run_id.clone(), raw);
    }
    store
        .inspect_expired_run_preparation(source.request().clone())
        .await
        .unwrap();
    let ExpiredRunOrigin::UnkeyedAdmission(join) = &source.target().origin else {
        panic!("explicit original");
    };
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .erase_historical_audit_for_test(session, join);
    crate::run_control_contract::expired::unkeyed::assert_refuses(&store, &source, &binding).await;
}
