//! Actual native Started authority plus one exact persisted assistant call.
use crate::Store;
use crabber_core::{ContentBlock, Message, MessageId, Part, PartId, PartKind, Role};

pub(crate) async fn run(store: &impl Store) {
    let (admitted, execution, authority, claim) = crate::phase_contract::fixture(store).await;
    let target = claim.command.target();
    let message_id = MessageId::new();
    let message = Message {
        id: message_id.clone(),
        session_id: admitted.session.id.clone(),
        run_id: Some(admitted.run.id.clone()),
        role: Role::Assistant,
        parent_id: None,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        parts: vec![Part {
            id: PartId::new(),
            message_id: message_id.clone(),
            ordinal: 0,
            kind: PartKind::FunctionToolCall,
            content: ContentBlock::ToolCall {
                call_id: target.call_id.clone(),
                name: "phase-owned".into(),
                arguments: serde_json::json!({}),
            },
        }],
    };
    execution.append_message(message).await.unwrap();
    let source = store
        .public_continuation_source(&admitted.fence, &authority)
        .await
        .unwrap();
    assert_eq!(source.session_id, admitted.session.id);
    assert_eq!(source.run_id, admitted.run.id);
    assert_eq!(source.attempt.id, target.attempt.id);
    assert_eq!(source.calls.len(), 1);
    assert_eq!(source.calls[0].message_id, message_id);
    assert_eq!(source.calls[0].call_id, target.call_id);
    assert_eq!(source.calls[0].name, "phase-owned");
    assert!(source.contents.state().is_null());
    assert_eq!(
        source.revision,
        store.session_revision(&admitted.session.id).await.unwrap()
    );
    let (_, _, foreign, _) = crate::phase_contract::fixture(store).await;
    assert!(
        store
            .public_continuation_source(&admitted.fence, &foreign)
            .await
            .is_err()
    );
}

pub(crate) async fn waiting_atomic(
    store: &impl Store,
    clock: &crabber_core::ManualClock,
    fault: &std::sync::atomic::AtomicU8,
) {
    use std::sync::atomic::Ordering;
    let fixture = crate::approval_contract::fixture::staged(store, clock).await;
    let session = &fixture.run.session_id;
    fault.store(1, Ordering::SeqCst);
    assert!(
        fixture
            .execution
            .suspend_for_approval(fixture.batch.clone(), fixture.owner.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_approval(session, &fixture.batch.id)
            .await
            .unwrap()
            .is_none()
    );
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert!(!events.iter().any(|event| matches!(
        event.kind,
        crabber_core::EventKind::RunPaused | crabber_core::EventKind::ContinuationWaiting
    )));
    fault.store(2, Ordering::SeqCst);
    assert!(
        fixture
            .execution
            .suspend_for_approval(fixture.batch.clone(), fixture.owner.clone())
            .await
            .is_err()
    );
    fault.store(0, Ordering::SeqCst);
    let receipt = store
        .lookup_approval(session, &fixture.batch.id)
        .await
        .unwrap()
        .unwrap();
    let events = store.list_events(session, None, 1000).await.unwrap();
    let waiting = events
        .iter()
        .filter(|event| event.kind == crabber_core::EventKind::ContinuationWaiting)
        .collect::<Vec<_>>();
    assert_eq!(waiting.len(), 1);
    let mut expected = fixture.batch.waiting_event(&receipt).unwrap();
    expected.cursor = waiting[0].cursor;
    assert_eq!(waiting[0], &expected);
    assert_eq!(
        store
            .get_run(&fixture.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber_core::RunStatus::Paused
    );
    assert_eq!(
        fixture
            .execution
            .suspend_for_approval(fixture.batch.clone(), fixture.owner)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store.list_events(session, None, 1000).await.unwrap(),
        events
    );
}

pub(crate) async fn tool_waiting_atomic(store: &impl Store, fault: &std::sync::atomic::AtomicU8) {
    use std::sync::atomic::Ordering;
    let fixture = crate::tool_continuation_contract::fixture::fixture(store).await;
    let session = &fixture.admitted.session.id;
    fault.store(1, Ordering::SeqCst);
    assert!(
        fixture
            .execution
            .suspend_tool_continuation(fixture.suspension.clone(), fixture.authority.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_tool_continuation(session, &fixture.suspension.id)
            .await
            .unwrap()
            .is_none()
    );
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert!(!events.iter().any(|event| matches!(
        event.kind,
        crabber_core::EventKind::RunPaused | crabber_core::EventKind::ContinuationWaiting
    )));
    fault.store(2, Ordering::SeqCst);
    assert!(
        fixture
            .execution
            .suspend_tool_continuation(fixture.suspension.clone(), fixture.authority.clone())
            .await
            .is_err()
    );
    fault.store(0, Ordering::SeqCst);
    let receipt = store
        .lookup_tool_continuation(session, &fixture.suspension.id)
        .await
        .unwrap()
        .unwrap();
    let events = store.list_events(session, None, 1000).await.unwrap();
    let waiting = events
        .iter()
        .filter(|event| event.kind == crabber_core::EventKind::ContinuationWaiting)
        .collect::<Vec<_>>();
    assert_eq!(waiting.len(), 1);
    let mut expected = fixture.suspension.waiting_event(&receipt).unwrap();
    expected.cursor = waiting[0].cursor;
    assert_eq!(waiting[0], &expected);
    assert!(!expected.payload.to_string().contains("never a result"));
    assert_eq!(
        store
            .get_run(&fixture.admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber_core::RunStatus::Paused
    );
    assert_eq!(
        fixture
            .execution
            .suspend_tool_continuation(fixture.suspension, fixture.authority)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store.list_events(session, None, 1000).await.unwrap(),
        events
    );
}
