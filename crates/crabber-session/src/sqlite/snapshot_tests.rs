use super::records::insert_event;
use super::{
    test_support::{child, close, wait_writers},
    tests::{input, request, temp_store},
    *,
};
use crate::*;
use crate::{
    SnapshotContinuation, SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotPage,
    SnapshotRequest,
};
use crabber_core::*;
use crabber_core::{ContentBlock, EventKind, ManualClock, PartId, Role};
use time::OffsetDateTime;

pub(super) fn query(session: &SessionId) -> SnapshotRequest {
    SnapshotRequest {
        session_id: session.clone(),
        continuation: None,
        limits: SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 100,
            text_bytes: 1_000_000,
            encoded_bytes: 2_000_000,
        },
    }
}
pub(super) fn page(outcome: SnapshotOutcome) -> SnapshotPage {
    let SnapshotOutcome::Page(page) = outcome else {
        panic!("expected page")
    };
    page
}
pub(super) fn event(a: &AdmitOutcome, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: a.session.id.clone(),
        run_id: a.run.id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: a.run.created_at,
    }
}
pub(super) fn call(a: &AdmitOutcome, id: &str) -> ToolCallRecord {
    ToolCallRecord {
        id: id.into(),
        run_id: a.run.id.clone(),
        name: "test".into(),
        arguments: serde_json::json!({"decimal":1e30,"escaped":"\n\"é"}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    }
}
pub(super) async fn seed(path: &Path) -> (SqliteStore, AdmitOutcome) {
    let store = SqliteStore::connect(path)
        .await
        .unwrap()
        .with_clock(Arc::new(ManualClock::new(OffsetDateTime::now_utc())));
    let a = store.admit_run(request(&SessionId::new())).await.unwrap();
    (store, a)
}
pub(super) fn assistant(a: &AdmitOutcome, text: &str) -> Message {
    let mut m = input(&a.session.id, text);
    m.run_id = Some(a.run.id.clone());
    m.role = Role::Assistant;
    m
}
pub(super) async fn settled(
    store: &SqliteStore,
    a: &AdmitOutcome,
    id: &ToolCallId,
) -> (ToolResult, Message) {
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![ContentBlock::ToolResult {
            call_id: id.clone(),
            content: vec![
                ContentBlock::Text {
                    text: "é\n".into()
                },
                ContentBlock::Reasoning {
                    text: "deep".into(),
                    provider_state: None,
                },
            ],
            is_error: false,
        }],
    };
    let mut message = assistant(a, "");
    message.role = Role::Tool;
    message.parts[0].content = ContentBlock::ToolResult {
        call_id: id.clone(),
        content: result.content.clone(),
        is_error: false,
    };
    store
        .execution(a.fence.clone())
        .await
        .unwrap()
        .settle_tool_call(
            id,
            result.clone(),
            message.clone(),
            event(a, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    (result, message)
}

#[tokio::test]
async fn exact_message_limits_are_metadata_first_and_outage_is_an_error() {
    let (_setup, path, _directory) = temp_store().await;
    let (store, a) = seed(&path).await;
    let original = store
        .list_all_messages(&a.session.id)
        .await
        .unwrap()
        .remove(0);
    let bytes = serde_json::to_vec(&original).unwrap().len();
    for (limits, expected) in [
        (
            SnapshotLimits {
                messages: 0,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::Messages,
        ),
        (
            SnapshotLimits {
                parts: 0,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::Parts,
        ),
        (
            SnapshotLimits {
                text_bytes: 4,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::TextBytes,
        ),
        (
            SnapshotLimits {
                encoded_bytes: bytes - 1,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::EncodedBytes,
        ),
    ] {
        let mut q = query(&a.session.id);
        q.limits = limits;
        let SnapshotOutcome::Limited {
            limit,
            continuation,
            ..
        } = store.snapshot(q).await.unwrap()
        else {
            panic!("limit")
        };
        assert_eq!(limit, expected);
        let mut q = query(&a.session.id);
        q.limits.text_bytes = 5;
        q.limits.encoded_bytes = bytes;
        q.continuation = Some(continuation);
        let p = page(store.snapshot(q).await.unwrap());
        assert_eq!(p.messages, vec![original.clone()]);
        assert_eq!(p.usage.encoded_bytes, bytes);
        assert!(p.continuation.is_none());
    }
    // An invalid domain payload with enormous accounting must be limited without
    // attempting client decode. This fixture directly simulates damaged storage.
    let record = serde_json::json!({"damaged":"x".repeat(1_000_000)}).to_string();
    sqlx::query("UPDATE messages SET data=$2,snapshot_bytes=$3,snapshot_text=1000000 WHERE id=$1")
        .bind(&original.id.0)
        .bind(&record)
        .bind(i64::try_from(record.len()).unwrap())
        .execute(&store.writer)
        .await
        .unwrap();
    let mut q = query(&a.session.id);
    q.limits.text_bytes = usize::MAX;
    q.limits.encoded_bytes = 4096;
    assert!(matches!(
        store.snapshot(q.clone()).await.unwrap(),
        SnapshotOutcome::Limited {
            limit: SnapshotLimit::EncodedBytes,
            ..
        }
    ));
    q.limits.encoded_bytes = 2_000_000;
    assert!(matches!(
        store.snapshot(q).await,
        Err(StoreError::Validation(_))
    ));
    close(&store).await;
    assert!(
        store.snapshot(query(&a.session.id)).await.is_err(),
        "outage is not a limit"
    );
}

#[tokio::test]
async fn independent_pools_append_pages_freeze_cutoffs_and_keep_settled_relationships() {
    let (_setup, path, _directory) = temp_store().await;
    let (store, a) = seed(&path).await;
    let other = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(store.clock.clone());
    let execution = other.execution(a.fence.clone()).await.unwrap();
    let first = call(&a, &format!("z-{}", uuid::Uuid::new_v4()));
    let second = call(&a, &format!("a-{}", uuid::Uuid::new_v4()));
    execution
        .create_tool_call(first.clone(), event(&a, EventKind::ToolCallPending))
        .await
        .unwrap();
    execution
        .create_tool_call(second.clone(), event(&a, EventKind::ToolCallPending))
        .await
        .unwrap();
    execution
        .claim_tool_call(&first.id, event(&a, EventKind::ToolCallRunning))
        .await
        .unwrap();
    let (result, result_message) = settled(&other, &a, &first.id).await;
    let p = page(store.snapshot(query(&a.session.id)).await.unwrap());
    let high_water = p.high_water;
    let mut messages = p.messages;
    let mut calls = p.tool_calls;
    let mut continuation = p.continuation;
    let writer = tokio::spawn(async move {
        execution
            .append_message(assistant(&a, "after boundary"))
            .await
            .unwrap();
        other
            .enqueue_inbox(
                &a.session.id,
                InboxKind::FollowUp,
                input(&a.session.id, "claimed after boundary"),
            )
            .await
            .unwrap();
        assert_eq!(
            execution
                .claim_inbox_into_history(InboxKind::FollowUp)
                .await
                .unwrap()
                .len(),
            1
        );
        execution
            .append_event(event(&a, EventKind::ExtensionNotice))
            .await
            .unwrap();
        a
    });
    let reopened = SqliteStore::connect(&path).await.unwrap();
    while let Some(token) = continuation {
        let mut q = query(&messages[0].session_id);
        q.continuation = Some(token);
        let p = page(reopened.snapshot(q).await.unwrap());
        assert_eq!(p.high_water, high_water);
        messages.extend(p.messages);
        calls.extend(p.tool_calls);
        continuation = p.continuation;
    }
    let a = writer.await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1], result_message);
    assert_eq!(
        calls.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    assert_eq!(calls[0].result, Some(result));
    let after = reopened
        .list_events(&a.session.id, Some(high_water), 100)
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert!(after[0].cursor.unwrap() > high_water);
}

#[tokio::test]
async fn tool_nested_text_and_exact_canonical_encoded_limits() {
    let (_setup, path, _directory) = temp_store().await;
    let (store, a) = seed(&path).await;
    let execution = store.execution(a.fence.clone()).await.unwrap();
    let mut c = call(&a, &uuid::Uuid::new_v4().to_string());
    execution
        .create_tool_call(c.clone(), event(&a, EventKind::ToolCallPending))
        .await
        .unwrap();
    execution
        .claim_tool_call(&c.id, event(&a, EventKind::ToolCallRunning))
        .await
        .unwrap();
    let (result, _) = settled(&store, &a, &c.id).await;
    c.status = ToolCallStatus::Completed;
    c.result = Some(result);
    let mut q = query(&a.session.id);
    let p = page(store.snapshot(q.clone()).await.unwrap());
    q.continuation = p.continuation;
    // Read the remaining result message with zero tool allowance to capture the
    // continuation immediately before the tool record.
    q.limits.tool_calls = 0;
    let p = page(store.snapshot(q.clone()).await.unwrap());
    let token = p.continuation.unwrap();
    let bytes = serde_json::to_vec(&c).unwrap().len();
    for (limits, expected) in [
        (
            SnapshotLimits {
                tool_calls: 0,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::ToolCalls,
        ),
        (
            SnapshotLimits {
                text_bytes: 6,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::TextBytes,
        ),
        (
            SnapshotLimits {
                encoded_bytes: bytes - 1,
                ..query(&a.session.id).limits
            },
            SnapshotLimit::EncodedBytes,
        ),
    ] {
        let mut q = query(&a.session.id);
        q.continuation = Some(token.clone());
        q.limits = limits;
        assert!(
            matches!(store.snapshot(q).await.unwrap(),SnapshotOutcome::Limited {limit,..} if limit==expected)
        );
    }
    let mut q = query(&a.session.id);
    q.continuation = Some(token);
    q.limits.text_bytes = 7;
    q.limits.encoded_bytes = bytes;
    let p = page(store.snapshot(q).await.unwrap());
    assert_eq!(p.tool_calls, vec![c]);
    assert_eq!(p.usage.text_bytes, 7);
    assert_eq!(p.usage.encoded_bytes, bytes);
}

async fn limited_token(store: &SqliteStore, session: &SessionId) -> SnapshotContinuation {
    let mut q = query(session);
    q.limits.messages = 0;
    let SnapshotOutcome::Limited { continuation, .. } = store.snapshot(q).await.unwrap() else {
        panic!("limit")
    };
    continuation
}
async fn invalidated(store: &SqliteStore, session: &SessionId, token: SnapshotContinuation) {
    let mut q = query(session);
    q.continuation = Some(token);
    assert!(matches!(
        store.snapshot(q).await.unwrap(),
        SnapshotOutcome::Invalidated { .. }
    ));
}
#[tokio::test]
async fn public_message_claim_and_settlement_mutations_invalidate_authenticated_tokens() {
    let (_setup, path, _directory) = temp_store().await;
    let (store, a) = seed(&path).await;
    let execution = store.execution(a.fence.clone()).await.unwrap();
    let message = assistant(&a, "mutable");
    execution.append_message(message.clone()).await.unwrap();
    let token = limited_token(&store, &a.session.id).await;
    let mut part = message.parts[0].clone();
    part.id = PartId::new();
    part.ordinal = 1;
    execution.append_part(part).await.unwrap();
    invalidated(&store, &a.session.id, token).await;
    let tool_call = call(&a, &uuid::Uuid::new_v4().to_string());
    execution
        .create_tool_call(tool_call.clone(), event(&a, EventKind::ToolCallPending))
        .await
        .unwrap();
    let token = limited_token(&store, &a.session.id).await;
    execution
        .claim_tool_call(&tool_call.id, event(&a, EventKind::ToolCallRunning))
        .await
        .unwrap();
    invalidated(&store, &a.session.id, token).await;
    let token = limited_token(&store, &a.session.id).await;
    settled(&store, &a, &tool_call.id).await;
    invalidated(&store, &a.session.id, token).await;
    let token = limited_token(&store, &a.session.id).await;
    for value in [
        SnapshotContinuation("x".repeat(2049)),
        SnapshotContinuation(format!("{}x", token.0)),
    ] {
        let mut q = query(&a.session.id);
        q.continuation = Some(value);
        assert!(matches!(
            store.snapshot(q).await,
            Err(StoreError::Validation(_))
        ));
    }
    let other_admission = store.admit_run(request(&SessionId::new())).await.unwrap();
    let mut q = query(&other_admission.session.id);
    q.continuation = Some(token);
    assert!(matches!(
        store.snapshot(q).await,
        Err(StoreError::Validation(_))
    ));
}

#[tokio::test]
async fn sqlite_snapshot_child_process() {
    let Ok(serialized) = std::env::var("CRABBER_SQLITE_SNAPSHOT_CHILD") else {
        return;
    };
    let q: SnapshotRequest = serde_json::from_str(&serialized).unwrap();
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = SqliteStore::connect(path).await.unwrap();
    let p = page(store.snapshot(q).await.unwrap());
    assert_eq!(p.messages.len(), 1);
    assert!(p.continuation.is_none());
}
#[tokio::test]
async fn continuation_survives_fresh_process() {
    let (_setup, path, _directory) = temp_store().await;
    let (store, a) = seed(&path).await;
    store
        .execution(a.fence.clone())
        .await
        .unwrap()
        .append_message(assistant(&a, "second"))
        .await
        .unwrap();
    let p = page(store.snapshot(query(&a.session.id)).await.unwrap());
    let mut q = query(&a.session.id);
    q.continuation = p.continuation;
    assert!(q.continuation.is_some());
    child(
        &path,
        "sqlite::snapshot_tests::sqlite_snapshot_child_process",
        &[(
            "CRABBER_SQLITE_SNAPSHOT_CHILD",
            serde_json::to_string(&q).unwrap(),
        )],
    )
    .wait()
    .await;
}

#[tokio::test]
async fn delayed_event_commit_cannot_cross_snapshot_high_water_unseen() {
    let (_setup, path, _directory) = temp_store().await;
    let (store, a) = seed(&path).await;
    let other = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(store.clock.clone());
    let initial = page(other.snapshot(query(&a.session.id)).await.unwrap());
    let mut held = store.begin_write().await.unwrap();
    let run = a.run.clone();
    insert_event(&mut held, &run, &event(&a, EventKind::ExtensionNotice))
        .await
        .unwrap();
    let writer_store = other.clone();
    let writer_fence = a.fence.clone();
    let writer_event = event(&a, EventKind::ExtensionNotice);
    let writer = tokio::spawn(async move {
        writer_store
            .execution(writer_fence)
            .await
            .unwrap()
            .append_event(writer_event)
            .await
            .unwrap();
    });
    wait_writers(&other, 1).await;
    let during = page(other.snapshot(query(&a.session.id)).await.unwrap());
    assert_eq!(during.high_water, initial.high_water);
    assert_eq!(
        other
            .list_events(&a.session.id, Some(initial.high_water), 100)
            .await
            .unwrap(),
        [] as [crabber_core::EventRecord; 0]
    );
    held.commit().await.unwrap();
    writer.await.unwrap();
    let events = other
        .list_events(&a.session.id, Some(initial.high_water), 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(events[0].cursor < events[1].cursor);
    let final_page = page(other.snapshot(query(&a.session.id)).await.unwrap());
    assert_eq!(Some(final_page.high_water), events[1].cursor);
}

#[test]
fn large_sqlite_history_keeps_client_allocation_bounded() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_setup, path, _directory) = runtime.block_on(temp_store());
    // allocation_counter observes the caller thread only, excluding SQLx worker allocations.
    let (store, a) = runtime.block_on(seed(&path));
    runtime.block_on(async {
        let execution = store.execution(a.fence.clone()).await.unwrap();
        let text = "x".repeat(262_144);
        for _ in 0..128 {
            execution
                .append_message(assistant(&a, &text))
                .await
                .unwrap();
        }
    });
    let mut continuation = None;
    let measured = allocation_counter::measure(|| {
        let mut q = query(&a.session.id);
        q.limits.encoded_bytes = 4096;
        let p = runtime.block_on(store.snapshot(q)).unwrap();
        let p = page(p);
        assert_eq!(p.messages.len(), 1);
        continuation = p.continuation;
    });
    assert!(
        measured.bytes_max < 131_072,
        "whole history or oversized record allocated: {measured:?}"
    );
    let measured = allocation_counter::measure(|| {
        let mut q = query(&a.session.id);
        q.limits.encoded_bytes = 4096;
        q.continuation = continuation.take();
        assert!(matches!(
            runtime.block_on(store.snapshot(q)).unwrap(),
            SnapshotOutcome::Limited {
                limit: SnapshotLimit::EncodedBytes,
                ..
            }
        ));
    });
    assert!(
        measured.bytes_max < 131_072,
        "oversized record decoded: {measured:?}"
    );
}

#[tokio::test]
async fn captured_public_revision_survives_metadata_changes_and_rejects_tampering() {
    let (store, _path, _directory) = temp_store().await;
    crate::management_contract::captured_history_revision(&store).await;
}
