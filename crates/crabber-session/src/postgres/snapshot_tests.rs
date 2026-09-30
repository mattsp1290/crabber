use super::{
    tests::{TEST_LOCK, input, request, test_url},
    *,
};
use crate::{
    SnapshotContinuation, SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotPage,
    SnapshotRequest,
};
use crabber_core::{ContentBlock, EventKind, ManualClock, PartId, Role};
use std::process::Command;

fn query(session: &SessionId) -> SnapshotRequest {
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
fn page(outcome: SnapshotOutcome) -> SnapshotPage {
    let SnapshotOutcome::Page(page) = outcome else {
        panic!("expected page")
    };
    page
}
fn event(a: &AdmitOutcome, kind: EventKind) -> EventRecord {
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
fn call(a: &AdmitOutcome, id: &str) -> ToolCallRecord {
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
async fn seed(url: &str) -> (PostgresStore, AdmitOutcome) {
    PostgresStore::migrate(url).await.unwrap();
    let store = PostgresStore::connect(url)
        .await
        .unwrap()
        .with_clock(Arc::new(ManualClock::new(OffsetDateTime::now_utc())));
    let a = store.admit_run(request(&SessionId::new())).await.unwrap();
    (store, a)
}
fn assistant(a: &AdmitOutcome, text: &str) -> Message {
    let mut m = input(&a.session.id, text);
    m.run_id = Some(a.run.id.clone());
    m.role = Role::Assistant;
    m
}
async fn settled(
    store: &PostgresStore,
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
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (store, a) = seed(&url).await;
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
    sqlx::query("UPDATE messages SET data=$2::jsonb,snapshot_record=$2,snapshot_bytes=$3,snapshot_text=1000000 WHERE id=$1").bind(&original.id.0).bind(&record).bind(i64::try_from(record.len()).unwrap()).execute(&store.pool).await.unwrap();
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
    store.pool.close().await;
    assert!(
        store.snapshot(query(&a.session.id)).await.is_err(),
        "outage is not a limit"
    );
}

#[tokio::test]
async fn independent_pools_append_pages_freeze_cutoffs_and_keep_settled_relationships() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (store, a) = seed(&url).await;
    let other = PostgresStore::connect(&url)
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
    let reopened = PostgresStore::connect(&url).await.unwrap();
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
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (store, a) = seed(&url).await;
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

async fn limited_token(store: &PostgresStore, session: &SessionId) -> SnapshotContinuation {
    let mut q = query(session);
    q.limits.messages = 0;
    let SnapshotOutcome::Limited { continuation, .. } = store.snapshot(q).await.unwrap() else {
        panic!("limit")
    };
    continuation
}
async fn invalidated(store: &PostgresStore, session: &SessionId, token: SnapshotContinuation) {
    let mut q = query(session);
    q.continuation = Some(token);
    assert!(matches!(
        store.snapshot(q).await.unwrap(),
        SnapshotOutcome::Invalidated { .. }
    ));
}
#[tokio::test]
async fn public_message_claim_and_settlement_mutations_invalidate_authenticated_tokens() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (store, a) = seed(&url).await;
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
async fn postgres_snapshot_child_process() {
    let Ok(serialized) = std::env::var("CRABBER_SNAPSHOT_CHILD") else {
        return;
    };
    let q: SnapshotRequest = serde_json::from_str(&serialized).unwrap();
    let store = PostgresStore::connect(&test_url().unwrap()).await.unwrap();
    let p = page(store.snapshot(q).await.unwrap());
    assert_eq!(p.messages.len(), 1);
    assert!(p.continuation.is_none());
}
#[tokio::test]
async fn continuation_survives_fresh_process() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (store, a) = seed(&url).await;
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
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "postgres::snapshot_tests::postgres_snapshot_child_process",
            "--nocapture",
        ])
        .env("CRABBER_SNAPSHOT_CHILD", serde_json::to_string(&q).unwrap())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[tokio::test]
async fn delayed_event_commit_cannot_cross_snapshot_high_water_unseen() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (store, a) = seed(&url).await;
    let other = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(store.clock.clone());
    let initial = page(other.snapshot(query(&a.session.id)).await.unwrap());
    let (mut held, run) = store.fenced(&a.fence).await.unwrap();
    insert_event(&mut held, &run, &event(&a, EventKind::ExtensionNotice))
        .await
        .unwrap();
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
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
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(holder_pid)
            .fetch_one(&store.pool)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second writer must wait for lower cursor commit");
    let during = page(other.snapshot(query(&a.session.id)).await.unwrap());
    assert_eq!(during.high_water, initial.high_water);
    assert!(
        other
            .list_events(&a.session.id, Some(initial.high_water), 100)
            .await
            .unwrap()
            .is_empty()
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
fn large_postgres_history_keeps_client_allocation_bounded() {
    let Some(url) = test_url() else { return };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = runtime.block_on(TEST_LOCK.lock());
    let (store, a) = runtime.block_on(seed(&url));
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

const AWKWARD_FLOAT_BITS: u64 = 15_097_766_278_683_351_411;

fn opaque_numbers() -> serde_json::Value {
    serde_json::json!({"negative_zero": -0.0, "awkward_float":f64::from_bits(AWKWARD_FLOAT_BITS), "nested":[-0.0,1e30], "escaped":"\n\"é"})
}

fn assert_opaque_bits(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if key == "awkward_float" {
                    assert_eq!(value.as_f64().unwrap().to_bits(), AWKWARD_FLOAT_BITS);
                }
                if key == "negative_zero" {
                    assert_eq!(value.as_f64().unwrap().to_bits(), (-0.0f64).to_bits());
                }
                assert_opaque_bits(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                assert_opaque_bits(value);
            }
        }
        _ => {}
    }
}
fn assert_returned_accounting(page: &SnapshotPage) {
    let returned_bytes: usize = page
        .messages
        .iter()
        .map(|message| serde_json::to_vec(message).unwrap().len())
        .sum::<usize>()
        + page
            .tool_calls
            .iter()
            .map(|call| serde_json::to_vec(call).unwrap().len())
            .sum::<usize>();
    assert_eq!(
        returned_bytes, page.usage.encoded_bytes,
        "returned records must fit the measured encoded-byte cap"
    );
    assert_opaque_bits(&serde_json::to_value(&page.messages).unwrap());
    assert_opaque_bits(&serde_json::to_value(&page.tool_calls).unwrap());
}
async fn assert_message_caps(store: &dyn Store, session: &SessionId, messages: &[Message]) {
    let mut next = query(session);
    next.limits.tool_calls = 0;
    for expected in messages {
        let bytes = serde_json::to_vec(expected).unwrap().len();
        next.limits.encoded_bytes = bytes - 1;
        let SnapshotOutcome::Limited {
            limit: SnapshotLimit::EncodedBytes,
            continuation,
            ..
        } = store.snapshot(next.clone()).await.unwrap()
        else {
            panic!("message -1 byte must be limited")
        };
        next.continuation = Some(continuation);
        next.limits.encoded_bytes = bytes;
        let actual = page(store.snapshot(next.clone()).await.unwrap());
        assert_eq!(actual.messages.len(), 1);
        assert_eq!(actual.usage.encoded_bytes, bytes);
        assert_returned_accounting(&actual);
        next.continuation = actual.continuation;
    }
}

fn normalize_page(mut page: SnapshotPage) -> SnapshotPage {
    for message in &mut page.messages {
        if message.run_id.is_some() {
            message.run_id = Some(RunId::from("canonical-fixture-run"));
        }
    }
    for call in &mut page.tool_calls {
        call.run_id = RunId::from("canonical-fixture-run");
    }
    page
}
async fn assert_canonical_parity(
    memory: &crate::MemoryStore,
    postgres: &PostgresStore,
    session: &SessionId,
) {
    let mut full = query(session);
    full.limits.messages = 100;
    full.limits.tool_calls = 100;
    let expected = page(memory.snapshot(full.clone()).await.unwrap());
    let actual = page(postgres.snapshot(full.clone()).await.unwrap());
    assert_eq!(
        actual.usage, expected.usage,
        "accounting must survive opaque numeric mutations"
    );
    assert_returned_accounting(&expected);
    assert_returned_accounting(&actual);
    assert_message_caps(memory, session, &expected.messages).await;
    assert_message_caps(postgres, session, &expected.messages).await;
    let first_call_bytes = expected
        .tool_calls
        .first()
        .map(|call| serde_json::to_vec(call).unwrap().len());
    let expected = normalize_page(expected);
    let actual = normalize_page(actual);
    assert_eq!(
        serde_json::to_vec(&actual.messages).unwrap(),
        serde_json::to_vec(&expected.messages).unwrap()
    );
    assert_eq!(
        serde_json::to_vec(&actual.tool_calls).unwrap(),
        serde_json::to_vec(&expected.tool_calls).unwrap()
    );
    full.limits.encoded_bytes = expected.usage.encoded_bytes;
    assert!(
        page(memory.snapshot(full.clone()).await.unwrap())
            .continuation
            .is_none()
    );
    assert!(
        page(postgres.snapshot(full).await.unwrap())
            .continuation
            .is_none()
    );
    if let Some(bytes) = first_call_bytes {
        // Position both backends immediately before their first tool record,
        // then require the exact complete record size to admit that record.
        let mut before = query(session);
        before.limits.messages = 100;
        before.limits.tool_calls = 0;
        let memory_token = page(memory.snapshot(before.clone()).await.unwrap())
            .continuation
            .unwrap();
        let postgres_token = page(postgres.snapshot(before).await.unwrap())
            .continuation
            .unwrap();
        let mut next = query(session);
        next.limits.encoded_bytes = bytes - 1;
        next.continuation = Some(memory_token.clone());
        assert!(matches!(
            memory.snapshot(next.clone()).await.unwrap(),
            SnapshotOutcome::Limited {
                limit: SnapshotLimit::EncodedBytes,
                ..
            }
        ));
        next.continuation = Some(postgres_token.clone());
        assert!(matches!(
            postgres.snapshot(next.clone()).await.unwrap(),
            SnapshotOutcome::Limited {
                limit: SnapshotLimit::EncodedBytes,
                ..
            }
        ));
        next.limits.encoded_bytes = bytes;
        next.continuation = Some(memory_token);
        let memory_page = page(memory.snapshot(next.clone()).await.unwrap());
        assert_eq!(memory_page.usage.encoded_bytes, bytes);
        assert_returned_accounting(&memory_page);
        next.continuation = Some(postgres_token);
        let postgres_page = page(postgres.snapshot(next).await.unwrap());
        assert_eq!(postgres_page.usage.encoded_bytes, bytes);
        assert_returned_accounting(&postgres_page);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One cross-backend opaque-record mutation lifecycle.
async fn canonical_negative_zero_survives_parts_claim_settlement_and_inbox() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let postgres = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let memory = crate::MemoryStore::with_clock(clock);
    let session = SessionId::new();
    let mut admission = request(&session);
    admission.user_message.parts[0].content = ContentBlock::Reasoning {
        text: "opaque input".into(),
        provider_state: Some(opaque_numbers()),
    };
    let message_id = admission.user_message.id.clone();
    let memory_admission = memory.admit_run(admission.clone()).await.unwrap();
    let postgres_admission = postgres.admit_run(admission).await.unwrap();
    let memory_execution = memory
        .execution(memory_admission.fence.clone())
        .await
        .unwrap();
    let postgres_execution = postgres
        .execution(postgres_admission.fence.clone())
        .await
        .unwrap();
    assert_canonical_parity(&memory, &postgres, &session).await;
    let part = Part {
        id: PartId::new(),
        message_id,
        ordinal: 1,
        kind: PartKind::AssistantText,
        content: ContentBlock::Text {
            text: "appended".into(),
        },
    };
    memory_execution.append_part(part.clone()).await.unwrap();
    postgres_execution.append_part(part).await.unwrap();
    assert_canonical_parity(&memory, &postgres, &session).await;
    let mut tool_call = call(&postgres_admission, &uuid::Uuid::new_v4().to_string());
    tool_call.arguments = opaque_numbers();
    let mut memory_call = tool_call.clone();
    memory_call.run_id = memory_admission.run.id.clone();
    memory_execution
        .create_tool_call(
            memory_call,
            event(&memory_admission, EventKind::ToolCallPending),
        )
        .await
        .unwrap();
    postgres_execution
        .create_tool_call(
            tool_call.clone(),
            event(&postgres_admission, EventKind::ToolCallPending),
        )
        .await
        .unwrap();
    assert_canonical_parity(&memory, &postgres, &session).await;
    memory_execution
        .claim_tool_call(
            &tool_call.id,
            event(&memory_admission, EventKind::ToolCallRunning),
        )
        .await
        .unwrap();
    postgres_execution
        .claim_tool_call(
            &tool_call.id,
            event(&postgres_admission, EventKind::ToolCallRunning),
        )
        .await
        .unwrap();
    assert_canonical_parity(&memory, &postgres, &session).await;
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![ContentBlock::Reasoning {
            text: "result".into(),
            provider_state: Some(opaque_numbers()),
        }],
    };
    let mut result_message = assistant(&postgres_admission, "");
    result_message.role = Role::Tool;
    result_message.parts[0].content = ContentBlock::ToolResult {
        call_id: tool_call.id.clone(),
        content: result.content.clone(),
        is_error: false,
    };
    let mut memory_result = result_message.clone();
    memory_result.run_id = Some(memory_admission.run.id.clone());
    memory_execution
        .settle_tool_call(
            &tool_call.id,
            result.clone(),
            memory_result,
            event(&memory_admission, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    postgres_execution
        .settle_tool_call(
            &tool_call.id,
            result,
            result_message,
            event(&postgres_admission, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    assert_canonical_parity(&memory, &postgres, &session).await;
    let mut inbox = input(&session, "");
    inbox.parts[0].content = ContentBlock::Reasoning {
        text: "pending".into(),
        provider_state: Some(opaque_numbers()),
    };
    memory
        .enqueue_inbox(&session, InboxKind::FollowUp, inbox.clone())
        .await
        .unwrap();
    postgres
        .enqueue_inbox(&session, InboxKind::FollowUp, inbox)
        .await
        .unwrap();
    let memory_claimed = memory_execution
        .claim_inbox_into_history(InboxKind::FollowUp)
        .await
        .unwrap();
    let postgres_claimed = postgres_execution
        .claim_inbox_into_history(InboxKind::FollowUp)
        .await
        .unwrap();
    let mut memory_claimed = memory_claimed;
    let mut postgres_claimed = postgres_claimed;
    memory_claimed[0].run_id = None;
    postgres_claimed[0].run_id = None;
    assert_eq!(
        serde_json::to_vec(&postgres_claimed).unwrap(),
        serde_json::to_vec(&memory_claimed).unwrap()
    );
    assert_canonical_parity(&memory, &postgres, &session).await;
    let mut inbox = input(&session, "");
    inbox.parts[0].content = ContentBlock::Reasoning {
        text: "claimed without history".into(),
        provider_state: Some(opaque_numbers()),
    };
    memory
        .enqueue_inbox(&session, InboxKind::Steer, inbox.clone())
        .await
        .unwrap();
    postgres
        .enqueue_inbox(&session, InboxKind::Steer, inbox)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(
            &postgres_execution
                .claim_inbox(InboxKind::Steer)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_vec(
            &memory_execution
                .claim_inbox(InboxKind::Steer)
                .await
                .unwrap()
        )
        .unwrap()
    );
}

#[tokio::test]
async fn awkward_float_initial_snapshot_preserves_bits_and_exact_encoded_cap() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let (postgres, _unused) = seed(&url).await;
    let memory = crate::MemoryStore::with_clock(postgres.clock.clone());
    let session = SessionId::new();
    let mut admission = request(&session);
    admission.user_message.created_at = OffsetDateTime::UNIX_EPOCH;
    admission.user_message.parts[0].kind = PartKind::ProviderState;
    admission.user_message.parts[0].content = ContentBlock::ProviderState {
        codec_id: "fixture".into(),
        payload: serde_json::json!({"awkward_float":f64::from_bits(AWKWARD_FLOAT_BITS)}),
    };
    memory.admit_run(admission.clone()).await.unwrap();
    postgres.admit_run(admission).await.unwrap();
    let mut request = query(&session);
    request.limits.tool_calls = 0;
    request.limits.parts = 1;
    request.limits.text_bytes = 0;
    let expected = page(memory.snapshot(request.clone()).await.unwrap());
    let bytes = serde_json::to_vec(&expected.messages[0]).unwrap().len();
    request.limits.encoded_bytes = bytes - 1;
    for store in [&memory as &dyn Store, &postgres as &dyn Store] {
        let SnapshotOutcome::Limited {
            limit: SnapshotLimit::EncodedBytes,
            continuation,
            ..
        } = store.snapshot(request.clone()).await.unwrap()
        else {
            panic!("one byte below must be limited")
        };
        let mut exact = request.clone();
        exact.limits.encoded_bytes = bytes;
        exact.continuation = Some(continuation);
        let actual = page(store.snapshot(exact).await.unwrap());
        assert_eq!(actual.usage.encoded_bytes, bytes);
        assert_returned_accounting(&actual);
        assert!(actual.continuation.is_none());
        assert_eq!(
            serde_json::to_vec(&normalize_page(actual).messages).unwrap(),
            serde_json::to_vec(&normalize_page(expected.clone()).messages).unwrap()
        );
    }
}
