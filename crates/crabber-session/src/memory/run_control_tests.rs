use super::*;
use crate::{RunControlAction, RunControlRequest, RunControlStatus, RunControlTarget};
use crabber_core::{Clock, ControlId, InputEnvelope, ManualClock, TurnId};
#[path = "run_control/tool_tests.rs"]
mod tools;
async fn fixture() -> (MemoryStore, crate::AdmitOutcome, RunControlRequest) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    let admitted = crate::public_state_contract::admit(&store, clock.now()).await;
    let revision = store.session_revision(&admitted.session.id).await.unwrap();
    let request = RunControlRequest {
        id: ControlId::new(),
        target: RunControlTarget::of(&admitted.run),
        expected_revision: revision,
        action: RunControlAction::Steer {
            input: InputEnvelope::text("original", revision).unwrap(),
        },
    };
    store.control_run(request.clone()).await.unwrap();
    (store, admitted, request)
}
#[tokio::test]
async fn private_payload_corruption_never_replaces_original_accepted_intent() {
    let (store, admitted, request) = fixture().await;
    let before = store.run_controls(&request.target).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state
            .run_controls
            .get_mut(&(request.target.session_id.clone(), request.id.clone()))
            .unwrap()
            .request
            .action = RunControlAction::Steer {
            input: InputEnvelope::text("x".repeat(1_000_000), request.expected_revision).unwrap(),
        };
    }
    let allocation = allocation_counter::measure(|| {
        let page = futures::executor::block_on(store.run_controls(&request.target)).unwrap();
        assert_eq!(page, before);
    });
    assert!(
        allocation.bytes_total < 65_536,
        "metadata read loaded private payload: {}",
        allocation.bytes_total
    );
    let execution = store.execution(admitted.fence).await.unwrap();
    assert_eq!(
        execution
            .observe_run_controls(Some(&TurnId::new()))
            .await
            .unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .list_all_messages(&request.target.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test]
async fn deleted_record_and_rolled_back_progress_refuse() {
    let (store, admitted, request) = fixture().await;
    let key = (request.target.session_id.clone(), request.id.clone());
    let original = store
        .state
        .lock()
        .unwrap()
        .run_controls
        .get(&key)
        .unwrap()
        .clone();
    let execution = store.execution(admitted.fence).await.unwrap();
    execution
        .observe_run_controls(Some(&TurnId::new()))
        .await
        .unwrap();
    store
        .state
        .lock()
        .unwrap()
        .run_controls
        .insert(key.clone(), original);
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    store.state.lock().unwrap().run_controls.remove(&key);
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_run_control(&request.target.session_id, &request.id)
            .await
            .unwrap()
            .is_some()
    );
}
#[tokio::test]
async fn incorporated_turn_is_part_of_protected_progress_evidence() {
    let (store, admitted, request) = fixture().await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let turn = TurnId::new();
    execution.observe_run_controls(Some(&turn)).await.unwrap();
    let page = store.run_controls(&request.target).await.unwrap();
    let summary = &page.controls[0];
    assert_eq!(
        summary.status,
        RunControlStatus::Incorporated { turn_id: turn }
    );
    let mut event = store
        .state
        .lock()
        .unwrap()
        .events
        .get(summary.event_cursor)
        .unwrap()
        .clone();
    event.turn_id = Some(TurnId::new());
    assert_eq!(
        crate::run_control::verify_event(summary, &event).unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    assert!(execution.append_event(event).await.is_err());
}

#[tokio::test]
async fn acceptance_origin_cannot_be_hidden_by_deleting_or_rerouting_carriers() {
    let (store, admitted, request) = fixture().await;
    let key = (request.target.session_id.clone(), request.id.clone());
    let original = store
        .state
        .lock()
        .unwrap()
        .operation_receipts
        .get(&key)
        .unwrap()
        .clone();
    {
        let mut state = store.state.lock().unwrap();
        let OperationReceipt::RunControl(receipt) = state.operation_receipts.get_mut(&key).unwrap()
        else {
            panic!("control receipt");
        };
        receipt.target.run_id = RunId::new();
    }
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    {
        let mut state = store.state.lock().unwrap();
        state.operation_receipts.insert(key.clone(), original);
        state.operation_receipts.remove(&key);
        state.run_controls.remove(&key);
        state.run_control_audits.retain(|(session, id, _), _| {
            session != &request.target.session_id || id != &request.id
        });
    }
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    let execution = store.execution(admitted.fence).await.unwrap();
    assert_eq!(
        execution.observe_run_controls(None).await.unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
}

#[tokio::test]
async fn deleted_progress_carrier_cannot_restore_an_older_control_state() {
    let (store, admitted, request) = fixture().await;
    let key = (request.target.session_id.clone(), request.id.clone());
    let original = store
        .state
        .lock()
        .unwrap()
        .run_controls
        .get(&key)
        .unwrap()
        .clone();
    let execution = store.execution(admitted.fence).await.unwrap();
    execution
        .observe_run_controls(Some(&TurnId::new()))
        .await
        .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        let current = state.run_controls.get(&key).unwrap().summary.clone();
        state
            .run_control_audits
            .remove(&(key.0.clone(), key.1.clone(), current.event_cursor.0));
        state.run_controls.insert(key, original);
    }
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    assert_eq!(
        execution
            .observe_run_controls(Some(&TurnId::new()))
            .await
            .unwrap_err(),
        crate::RunControlError::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .list_all_messages(&request.target.session_id)
            .await
            .unwrap()
            .len(),
        2
    );
}
