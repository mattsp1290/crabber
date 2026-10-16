use super::EventLog;
use crate::{MemoryStore, PublicStateMutation, Store};
use crabber_core::{ControlId, EventCursor, EventKind, EventRecord, PublicStateContents};

fn source(cursor: u64) -> EventRecord {
    EventRecord {
        cursor: Some(EventCursor(cursor)),
        session_id: "session".into(),
        run_id: "run".into(),
        turn_id: None,
        kind: EventKind::ExtensionNotice,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
#[test]
fn cursor_allocation_refuses_mismatch_and_exhaustion_without_mutation() {
    let mut log = EventLog::default();
    assert!(log.append(source(0)).is_err());
    assert_eq!(log.high_water, 0);
    assert!(log.records.is_empty());
    log.append(source(1)).unwrap();
    let original = log.clone();
    assert!(log.append(source(1)).is_err());
    assert!(log.append(source(3)).is_err());
    assert_eq!(log, original);
    log.records.clear();
    assert_eq!(log.next_cursor().unwrap(), EventCursor(2));
    log.high_water = u64::MAX;
    let original = log.clone();
    assert!(log.append(source(0)).is_err());
    assert_eq!(log, original);
}

#[tokio::test]
async fn erasure_holes_preserve_other_receipts_and_never_reuse_tail_cursors() {
    let store = MemoryStore::new();
    let now = time::OffsetDateTime::now_utc();
    let first = crate::public_state_contract::admit(&store, now).await;
    notice(&store, &first).await;
    let survivor = crate::public_state_contract::admit(&store, now).await;
    notice(&store, &survivor).await;
    let middle = crate::public_state_contract::admit(&store, now).await;
    notice(&store, &middle).await;
    let execution = store.execution(survivor.fence.clone()).await.unwrap();
    let operation = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(&survivor.session.id).await.unwrap(),
        contents: PublicStateContents::default(),
    };
    let receipt = execution
        .mutate_public_state(operation.clone())
        .await
        .unwrap();
    let tail = crate::public_state_contract::admit(&store, now).await;
    notice(&store, &tail).await;
    let high_water = {
        let mut state = store.state.lock().unwrap();
        let high_water = state.events.high_water;
        // Internal erasure fixture only. This is not an enabled session-deletion
        // API: session ownership/readiness is qualified separately.
        state.events.records.retain(|_, event| {
            ![&first.session.id, &middle.session.id, &tail.session.id].contains(&&event.session_id)
        });
        assert_eq!(state.events.high_water, high_water);
        assert!(state.events.get(EventCursor(high_water)).is_none());
        high_water
    };
    assert_eq!(
        store
            .lookup_public_state_mutation(&survivor.session.id, &operation.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        execution.mutate_public_state(operation).await.unwrap(),
        receipt
    );
    let before = store
        .list_events(&survivor.session.id, None, 100)
        .await
        .unwrap();
    let mut event = source(0);
    event.run_id = survivor.run.id;
    event.session_id = survivor.session.id.clone();
    execution.append_event(event).await.unwrap();
    let after = store
        .list_events(&survivor.session.id, None, 100)
        .await
        .unwrap();
    assert_eq!(&after[..before.len()], before.as_slice());
    assert_eq!(
        after.last().unwrap().cursor,
        Some(EventCursor(high_water + 1))
    );
    assert_eq!(
        store
            .list_events(&survivor.session.id, Some(EventCursor(high_water)), 100)
            .await
            .unwrap(),
        after[before.len()..]
    );
}

async fn notice(store: &MemoryStore, admitted: &crate::AdmitOutcome) {
    let mut event = source(0);
    event.run_id = admitted.run.id.clone();
    event.session_id = admitted.session.id.clone();
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .append_event(event)
        .await
        .unwrap();
}
