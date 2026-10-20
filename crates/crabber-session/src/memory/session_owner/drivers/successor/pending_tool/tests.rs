use super::*;
use crate::Store;
use crate::session_owner::preparation::PendingToolSource;
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn pending_tool_preflight_is_pure_and_one_budget() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    for &(kind, recovery, unkeyed) in crate::successor_driver_contract::tools::pending::CASES {
        let request = crate::successor_driver_contract::tools::pending::unprepared(
            &store, &clock, kind, recovery, unkeyed,
        )
        .await;
        let before = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        let metadata = store
            .inspect_pending_tool_preparation(request.clone())
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
            let mut proof = Proof::bounded(32 << 20);
            read(&state, &request, clock.now(), &mut proof).unwrap();
            assert!(proof.used() > 1000);
            read(
                &state,
                &request,
                clock.now(),
                &mut Proof::bounded(proof.used()),
            )
            .unwrap();
            assert!(
                read(
                    &state,
                    &request,
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
            PendingToolSource::Claim(c) => {
                c.expected_revision = c.expected_revision.next().unwrap();
            }
            PendingToolSource::Recovery(r) => {
                r.expected_revision = r.expected_revision.next().unwrap();
            }
            PendingToolSource::FrontendClaim(c) => {
                c.claim.expected_revision = c.claim.expected_revision.next().unwrap();
            }
            PendingToolSource::FrontendRecovery(r) => {
                r.expected_revision = r.expected_revision.next().unwrap();
            }
        }
        assert!(store.inspect_pending_tool_preparation(wrong).await.is_err());
    }
}

#[tokio::test]
async fn legacy_pending_tool_preflight_remains_unsupported() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let request = crate::successor_driver_contract::tools::pending::unprepared(
        &store, &clock, 0, false, false,
    )
    .await;
    // Explicit old-format import. No protected source is captured retroactively.
    store.state.lock().unwrap().continuation_inputs = crate::continuation_input::Ledger::default();
    assert!(matches!(
        store.inspect_pending_tool_preparation(request).await,
        Err(Error::Unsupported)
    ));
}
