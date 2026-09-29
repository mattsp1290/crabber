//! Behavioral contract reusable by every `Store` implementation.

use crate::{AdmitRequest, InboxKind, Store, StoreError};
use crabber_core::{
    ContentBlock, ContextEpoch, EpochId, EventKind, EventRecord, ManualClock, Message, MessageId,
    Part, PartId, PartKind, Role, RunId, RunStatus, SessionId, ToolCallId, ToolCallRecord,
    ToolCallStatus, ToolResult, ToolResultStatus, Usage,
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

fn message(
    session: &SessionId,
    run: Option<RunId>,
    role: Role,
    kind: PartKind,
    text: &str,
    now: OffsetDateTime,
) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: run,
        role,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind,
            content: ContentBlock::Text { text: text.into() },
        }],
        created_at: now,
    }
}

fn event(session: &SessionId, run: &RunId, kind: EventKind, now: OffsetDateTime) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: session.clone(),
        run_id: run.clone(),
        turn_id: None,
        kind,
        payload: Value::Null,
        correlation: None,
        live_only: false,
        created_at: now,
    }
}

/// Runs the common store contract against a fresh store from `factory`.
///
/// The factory receives a controllable clock so lease behavior is deterministic.
///
/// # Panics
///
/// Panics when a backend violates the store contract.
#[allow(clippy::too_many_lines)] // One sequential scenario exercises the transaction lifecycle.
pub async fn run_contract<S, F>(factory: F)
where
    S: Store,
    F: Fn(Arc<ManualClock>) -> S,
{
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid test timestamp");
    let clock = Arc::new(ManualClock::new(now));
    let store = factory(Arc::clone(&clock));
    let session_id = SessionId::new();
    let user = message(
        &session_id,
        None,
        Role::User,
        PartKind::UserInputText,
        "hello",
        now,
    );
    let request = AdmitRequest {
        session_id: None,
        workspace_id: "workspace".into(),
        directory: "/tmp".into(),
        title: "contract".into(),
        user_message: user.clone(),
        config_hash: "config".into(),
        plan_fingerprint: "plan".into(),
        owner: "worker-a".into(),
        lease: Duration::from_secs(1),
    };
    let admitted = store
        .admit_run(request.clone())
        .await
        .expect("first admission");
    assert_eq!(admitted.session.id, session_id);
    assert!(admitted.prior_history.is_empty());

    let mut second = request;
    second.session_id = Some(session_id.clone());
    second.user_message.id = MessageId::new();
    assert_eq!(store.admit_run(second).await.unwrap_err(), StoreError::Busy);

    let mut bad_fence = admitted.fence.clone();
    bad_fence.claim_token = "stale".into();
    assert!(matches!(
        store.execution(bad_fence).await,
        Err(StoreError::Conflict)
    ));
    let execution = store
        .execution(admitted.fence.clone())
        .await
        .expect("valid execution");
    let live = event(&session_id, &admitted.run.id, EventKind::TextDelta, now);
    assert!(matches!(
        execution.append_event(live).await,
        Err(StoreError::Validation(_))
    ));
    assert!(
        store
            .list_events(&session_id, None, 100)
            .await
            .unwrap()
            .is_empty()
    );

    let old = message(
        &session_id,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::AssistantText,
        "old",
        now,
    );
    execution.append_message(old.clone()).await.unwrap();
    let custom = message(
        &session_id,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::Custom {
            custom_type: "state".into(),
        },
        "hidden",
        now,
    );
    execution.append_message(custom).await.unwrap();
    let custom_message = message(
        &session_id,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::CustomMessage {
            custom_type: "notice".into(),
        },
        "visible",
        now,
    );
    execution
        .append_message(custom_message.clone())
        .await
        .unwrap();
    let tail = message(
        &session_id,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::AssistantText,
        "tail",
        now,
    );
    let epoch_id = EpochId::new();
    execution
        .start_epoch(ContextEpoch {
            id: epoch_id.clone(),
            session_id: session_id.clone(),
            run_id: admitted.run.id.clone(),
            parent: Some(admitted.epoch),
            summarized_range: Some((user.id.clone(), old.id.clone())),
            summary_message_id: None,
            tail_start_message_id: Some(tail.id.clone()),
            provider_id: "fake".into(),
            model_id: "test".into(),
            reason: "compact".into(),
            next_policy: None,
        })
        .await
        .unwrap();
    execution.append_message(tail.clone()).await.unwrap();
    let summary = message(
        &session_id,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::CompactionSummary,
        "summary",
        now,
    );
    execution
        .finish_epoch(&epoch_id, summary.clone())
        .await
        .unwrap();
    let projected = store.list_messages(&session_id, None).await.unwrap();
    assert_eq!(
        projected.iter().map(|entry| &entry.id).collect::<Vec<_>>(),
        vec![&summary.id, &tail.id]
    );
    let full = store
        .list_messages(&session_id, Some(admitted.run.epoch_id.clone()))
        .await
        .unwrap();
    assert!(full.iter().any(|entry| entry.id == custom_message.id));
    assert!(full.iter().all(|entry| {
        entry
            .parts
            .iter()
            .all(|part| !matches!(part.kind, PartKind::Custom { .. }))
    }));

    let call_id = ToolCallId::new();
    let call = ToolCallRecord {
        id: call_id.clone(),
        run_id: admitted.run.id.clone(),
        name: "echo".into(),
        arguments: Value::Null,
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    execution
        .create_tool_call(
            call,
            event(
                &session_id,
                &admitted.run.id,
                EventKind::ToolCallPending,
                now,
            ),
        )
        .await
        .unwrap();
    execution
        .claim_tool_call(
            &call_id,
            event(
                &session_id,
                &admitted.run.id,
                EventKind::ToolCallRunning,
                now,
            ),
        )
        .await
        .unwrap();
    let mut result_message = message(
        &session_id,
        Some(admitted.run.id.clone()),
        Role::Tool,
        PartKind::FunctionToolResult,
        "done",
        now,
    );
    result_message.parts[0].content = ContentBlock::ToolResult {
        call_id: call_id.clone(),
        content: vec![ContentBlock::Text {
            text: "done".into(),
        }],
        is_error: false,
    };
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![ContentBlock::Text {
            text: "done".into(),
        }],
    };
    let mut invalid_event = event(
        &session_id,
        &admitted.run.id,
        EventKind::ToolCallSettled,
        now,
    );
    invalid_event.live_only = true;
    let event_count = store
        .list_events(&session_id, None, 100)
        .await
        .unwrap()
        .len();
    assert!(matches!(
        execution
            .settle_tool_call(
                &call_id,
                result.clone(),
                result_message.clone(),
                invalid_event
            )
            .await,
        Err(StoreError::Validation(_))
    ));
    assert_eq!(
        store
            .list_events(&session_id, None, 100)
            .await
            .unwrap()
            .len(),
        event_count
    );
    assert!(
        store
            .list_messages(&session_id, Some(epoch_id.clone()))
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.id != result_message.id)
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()[0]
            .status,
        ToolCallStatus::Running
    );
    execution
        .settle_tool_call(
            &call_id,
            result,
            result_message,
            event(
                &session_id,
                &admitted.run.id,
                EventKind::ToolCallSettled,
                now,
            ),
        )
        .await
        .unwrap();
    assert!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()
            .is_empty()
    );

    clock.set(now + time::Duration::seconds(2));
    assert_eq!(
        execution
            .append_event(event(
                &session_id,
                &admitted.run.id,
                EventKind::ExtensionNotice,
                now
            ))
            .await
            .unwrap_err(),
        StoreError::Conflict
    );
    let reclaimed = store
        .claim_expired_run(&admitted.run.id, "worker-b")
        .await
        .expect("expired lease can be reclaimed");
    assert_ne!(reclaimed.claim_token, admitted.fence.claim_token);
    assert_eq!(
        execution
            .renew_lease(now + time::Duration::seconds(30))
            .await
            .unwrap_err(),
        StoreError::Conflict
    );
    let execution = store.execution(reclaimed).await.unwrap();

    let follow_up = message(
        &session_id,
        None,
        Role::User,
        PartKind::UserInputText,
        "next",
        now,
    );
    store
        .enqueue_inbox(&session_id, InboxKind::FollowUp, follow_up.clone())
        .await
        .unwrap();
    let before = store
        .list_events(&session_id, None, 100)
        .await
        .unwrap()
        .len();
    let settled = event(&session_id, &admitted.run.id, EventKind::RunSettled, now);
    assert_eq!(
        execution
            .settle_run(
                RunStatus::Completed,
                None,
                Usage::default(),
                settled.clone()
            )
            .await
            .unwrap_err(),
        StoreError::PendingInput
    );
    assert_eq!(
        store
            .list_events(&session_id, None, 100)
            .await
            .unwrap()
            .len(),
        before
    );
    assert_eq!(
        execution.claim_inbox(InboxKind::FollowUp).await.unwrap(),
        vec![follow_up]
    );
    assert!(
        execution
            .claim_inbox(InboxKind::FollowUp)
            .await
            .unwrap()
            .is_empty()
    );
    execution
        .settle_run(RunStatus::Completed, None, Usage::default(), settled)
        .await
        .unwrap();
    assert!(store.list_unfinished_runs().await.unwrap().is_empty());

    let new_user = message(
        &session_id,
        None,
        Role::User,
        PartKind::UserInputText,
        "again",
        now,
    );
    let resumed = store
        .admit_run(AdmitRequest {
            session_id: Some(session_id.clone()),
            workspace_id: "workspace".into(),
            directory: "/tmp".into(),
            title: "contract".into(),
            user_message: new_user.clone(),
            config_hash: "config".into(),
            plan_fingerprint: "plan".into(),
            owner: "worker-c".into(),
            lease: Duration::from_secs(1),
        })
        .await
        .unwrap();
    assert_eq!(
        resumed.prior_history.first().map(|entry| &entry.id),
        Some(&summary.id)
    );
    assert_eq!(
        resumed.prior_history.get(1).map(|entry| &entry.id),
        Some(&tail.id)
    );
    for selected_epoch in [None, Some(resumed.epoch.clone())] {
        let projected = store
            .list_messages(&session_id, selected_epoch)
            .await
            .unwrap();
        assert_eq!(projected.first().map(|entry| &entry.id), Some(&summary.id));
        assert_eq!(projected.get(1).map(|entry| &entry.id), Some(&tail.id));
        assert_eq!(projected.last().map(|entry| &entry.id), Some(&new_user.id));
        assert!(projected.iter().all(|entry| {
            entry.id != user.id && entry.id != old.id && entry.id != custom_message.id
        }));
    }
}
