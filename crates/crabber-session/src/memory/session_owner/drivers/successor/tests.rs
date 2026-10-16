use super::*;
use std::sync::Arc;
#[tokio::test]
async fn successor_proof_budget_and_commit_faults() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    for admission in [true, false] {
        let request =
            crate::successor_driver_contract::fixture::request(store.as_ref(), &clock, admission)
                .await;
        let state = store.state.lock().unwrap();
        let before = revision(&state, request.context.session_id());
        let mut proof = Proof::bounded(32 << 20);
        fresh(&state, &request, None, store.clock.now(), &mut proof).unwrap();
        let used = proof.used();
        assert!(used > 1000);
        assert!(
            fresh(
                &state,
                &request,
                None,
                store.clock.now(),
                &mut Proof::bounded(used - 1)
            )
            .is_err()
        );
        let mut proof = Proof::bounded(32 << 20);
        source::original(&state, &request, &mut proof, None).unwrap();
        assert!(
            source::original(
                &state,
                &request,
                &mut Proof::bounded(proof.used() - 1),
                None
            )
            .is_err()
        );
        assert_eq!(revision(&state, request.context.session_id()), before);
    }
    let fault = store.session_owner_fault.clone();
    crate::successor_driver_contract::faults(store, &clock, move |value| {
        fault.store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}
