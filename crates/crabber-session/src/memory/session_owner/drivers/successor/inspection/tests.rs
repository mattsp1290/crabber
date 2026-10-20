use super::*;
use crate::successor_driver_contract::approval::pending::{commit_faults, inspected};
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn pending_approval_commit_faults_exact_budget_and_cold_metadata() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    commit_faults(&store, &clock, |value| {
        store.session_owner_fault.store(value, Ordering::SeqCst);
    })
    .await;
    for recovery in [false, true] {
        let request = inspected(&store, &clock, recovery).await;
        let state = store.state.lock().unwrap();
        let mut proof = Proof::bounded(32 << 20);
        let result = inspect(&state, &request, &mut proof).unwrap();
        assert!(proof.used() > 1000);
        assert!(inspect(&state, &request, &mut Proof::bounded(proof.used() - 1)).is_err());
        assert_eq!(
            result,
            inspect(&state, &request, &mut Proof::bounded(proof.used())).unwrap()
        );
    }
}
