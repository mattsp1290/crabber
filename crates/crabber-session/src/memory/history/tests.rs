use super::SessionSequence;
use crate::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store};
use crabber_core::{ContentBlock, Message, MessageId, Part, PartId, PartKind, Role, SessionId};

fn message(session: &SessionId, text: &str) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text { text: text.into() },
        }],
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
#[test]
fn unrelated_scope_erasure_preserves_captured_offsets() {
    let first = SessionId::new();
    let survivor = SessionId::new();
    let tail = SessionId::new();
    let mut log = SessionSequence::default();
    log.push(first.clone(), 1);
    log.push(survivor.clone(), 2);
    log.push(first.clone(), 3);
    log.push(survivor.clone(), 4);
    log.push(tail.clone(), 5);
    let captured = log.for_session(&survivor).to_vec();
    log.records.remove(&first);
    log.records.remove(&tail);
    log.push(survivor.clone(), 6);
    assert_eq!(&log.for_session(&survivor)[..captured.len()], captured);
}

#[tokio::test]
async fn surviving_snapshot_continuation_retains_history_and_cutoff_after_erasure() {
    let store = MemoryStore::new();
    let now = time::OffsetDateTime::now_utc();
    let first = crate::public_state_contract::admit(&store, now).await;
    let survivor = crate::public_state_contract::admit(&store, now).await;
    let tail = crate::public_state_contract::admit(&store, now).await;
    let _ = pending(&store, &first).await;
    let survivor_call = pending(&store, &survivor).await;
    let _ = pending(&store, &tail).await;
    let appended = message(&survivor.session.id, "captured survivor tail");
    {
        let mut state = store.state.lock().unwrap();
        state
            .messages
            .push(survivor.session.id.clone(), appended.clone());
    }
    let mut request = SnapshotRequest {
        session_id: survivor.session.id.clone(),
        continuation: None,
        limits: SnapshotLimits {
            messages: 1,
            tool_calls: 10,
            parts: 100,
            text_bytes: 100_000,
            encoded_bytes: 1_000_000,
        },
    };
    let SnapshotOutcome::Page(page) = store.snapshot(request.clone()).await.unwrap() else {
        panic!("first page")
    };
    request.continuation = page.continuation;
    assert!(request.continuation.is_some());
    {
        let mut state = store.state.lock().unwrap();
        // Internal purge fixture, not an enabled deletion operation.
        for removed in [&first.session.id, &tail.session.id] {
            state.messages.records.remove(removed);
            state.call_order.records.remove(removed);
        }
        state.messages.push(
            survivor.session.id.clone(),
            message(&survivor.session.id, "new outside capture"),
        );
    }
    let SnapshotOutcome::Page(continued) = store.snapshot(request).await.unwrap() else {
        panic!("surviving continuation")
    };
    assert_eq!(continued.messages, vec![appended]);
    assert_eq!(continued.tool_calls, vec![survivor_call]);
    assert_eq!(continued.high_water, page.high_water);
    assert_eq!(continued.captured_revision, page.captured_revision);
    assert!(continued.continuation.is_none());
}

async fn pending(
    store: &MemoryStore,
    admitted: &crate::AdmitOutcome,
) -> crabber_core::ToolCallRecord {
    let call = crabber_core::ToolCallRecord {
        id: crabber_core::ToolCallId::new(),
        run_id: admitted.run.id.clone(),
        name: "pending fixture".into(),
        arguments: serde_json::json!({}),
        status: crabber_core::ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .create_tool_call(
            call.clone(),
            crabber_core::EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: crabber_core::EventKind::ToolCallPending,
                payload: serde_json::Value::Null,
                correlation: Some(call.id.0.clone()),
                live_only: false,
                created_at: admitted.run.created_at,
            },
        )
        .await
        .unwrap();
    call
}
