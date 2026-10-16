//! A mutable Unstarted restore cannot erase the actual independent Begin.
use crate::{AttemptCommand, AttemptMutation, MemoryStore, Store};
use crabber_core::{ControlId, ManualClock};
use std::sync::Arc;
#[tokio::test]
async fn expired_unstarted_refuses_actual_begin_erasure_and_current_restore() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    let (admitted, reserved, authority) =
        crate::run_control_contract::expired::unstarted::reserved(&store, &clock, true).await;
    let (original, events) = {
        let state = store.state.lock().unwrap();
        (
            state
                .attempts
                .get(&reserved.attempt.target.id)
                .unwrap()
                .clone(),
            state.events.clone(),
        )
    };
    let id = ControlId::new();
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .mutate_attempt(
            AttemptMutation {
                id: id.clone(),
                expected_revision: reserved.revision,
                command: AttemptCommand::Begin {
                    target: reserved.attempt.target.clone(),
                },
            },
            Some(authority),
        )
        .await
        .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        assert!(
            state
                .native_parent_witnesses
                .values()
                .any(|witness| witness.source.run_id == admitted.run.id)
        );
        state.attempts.insert(reserved.attempt.target.id, original);
        state.events = events;
        state
            .operation_receipts
            .remove(&(admitted.session.id.clone(), id));
        state
            .public_revisions
            .insert(admitted.session.id.clone(), reserved.revision);
    }
    clock.set(admitted.run.lease_until);
    assert!(
        store
            .inspect_expired_run_preparation(
                crate::session_owner::preparation::InspectExpiredRunPreparation {
                    session_id: admitted.session.id.clone(),
                    expected_fence: admitted.fence,
                    owner: "must refuse".into(),
                    lease: std::time::Duration::from_secs(30),
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        reserved.revision
    );
}
