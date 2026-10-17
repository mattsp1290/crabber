use super::*;
use crate::Store;
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn pending_approval_preflight_is_pure_and_one_budget() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let request = crate::successor_driver_contract::approval::pending::unprepared(
            &store, &clock, recovery, unkeyed,
        )
        .await;
        let before = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        let metadata = store
            .inspect_pending_approval_preparation(request.clone())
            .await
            .unwrap();
        request.verify_metadata(&metadata).unwrap();
        let mut wrong_source = metadata.clone();
        wrong_source.source_digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        assert!(request.verify_metadata(&wrong_source).is_err());
        let mut wrong = metadata.clone();
        wrong.expected_revision = wrong.expected_revision.next().unwrap();
        assert!(request.verify_metadata(&wrong).is_err());
        {
            let state = store.state.lock().unwrap();
            let metadata = choice::metadata(&request).unwrap();
            let mut proof = Proof::bounded(32 << 20);
            inspect(&state, &request, metadata.clone(), clock.now(), &mut proof).unwrap();
            assert!(proof.used() > 1000);
            assert!(
                inspect(
                    &state,
                    &request,
                    metadata,
                    clock.now(),
                    &mut Proof::bounded(proof.used() - 1)
                )
                .is_err()
            );
        }
        assert_eq!(
            store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            before
        );
        let mut wrong = request.clone();
        match &mut wrong.source {
            PendingApprovalSource::Claim(c) => {
                c.expected_revision = c.expected_revision.next().unwrap();
            }
            PendingApprovalSource::Recovery(r) => {
                r.expected_revision = r.expected_revision.next().unwrap();
            }
        }
        assert!(
            store
                .inspect_pending_approval_preparation(wrong)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn legacy_pending_approval_preflight_remains_unsupported() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let request = crate::successor_driver_contract::approval::pending::unprepared(
        &store, &clock, false, false,
    )
    .await;
    // Explicit old-format import. No protected source is captured retroactively.
    store.state.lock().unwrap().continuation_inputs = crate::continuation_input::Ledger::default();
    assert!(matches!(
        store.inspect_pending_approval_preparation(request).await,
        Err(Error::Unsupported)
    ));
}
