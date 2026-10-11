use super::{
    snapshot_tests::{assistant, call, event, page, query, seed},
    tests::{input, request, temp_store},
    *,
};
use crate::*;
use crabber_core::*;
use time::OffsetDateTime;
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
    sqlite: &SqliteStore,
    session: &SessionId,
) {
    let mut full = query(session);
    full.limits.messages = 100;
    full.limits.tool_calls = 100;
    let expected = page(memory.snapshot(full.clone()).await.unwrap());
    let actual = page(sqlite.snapshot(full.clone()).await.unwrap());
    assert_eq!(
        actual.usage, expected.usage,
        "accounting must survive opaque numeric mutations"
    );
    assert_returned_accounting(&expected);
    assert_returned_accounting(&actual);
    assert_message_caps(memory, session, &expected.messages).await;
    assert_message_caps(sqlite, session, &expected.messages).await;
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
        page(sqlite.snapshot(full).await.unwrap())
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
        let sqlite_token = page(sqlite.snapshot(before).await.unwrap())
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
        next.continuation = Some(sqlite_token.clone());
        assert!(matches!(
            sqlite.snapshot(next.clone()).await.unwrap(),
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
        next.continuation = Some(sqlite_token);
        let sqlite_page = page(sqlite.snapshot(next).await.unwrap());
        assert_eq!(sqlite_page.usage.encoded_bytes, bytes);
        assert_returned_accounting(&sqlite_page);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One cross-backend opaque-record mutation lifecycle.
async fn canonical_negative_zero_survives_parts_claim_settlement_and_inbox() {
    let (_setup, path, _directory) = temp_store().await;
    SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let sqlite = SqliteStore::connect(&path)
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
    let sqlite_admission = sqlite.admit_run(admission).await.unwrap();
    let memory_execution = memory
        .execution(memory_admission.fence.clone())
        .await
        .unwrap();
    let sqlite_execution = sqlite
        .execution(sqlite_admission.fence.clone())
        .await
        .unwrap();
    assert_canonical_parity(&memory, &sqlite, &session).await;
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
    sqlite_execution.append_part(part).await.unwrap();
    assert_canonical_parity(&memory, &sqlite, &session).await;
    let mut tool_call = call(&sqlite_admission, &uuid::Uuid::new_v4().to_string());
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
    sqlite_execution
        .create_tool_call(
            tool_call.clone(),
            event(&sqlite_admission, EventKind::ToolCallPending),
        )
        .await
        .unwrap();
    assert_canonical_parity(&memory, &sqlite, &session).await;
    memory_execution
        .claim_tool_call(
            &tool_call.id,
            event(&memory_admission, EventKind::ToolCallRunning),
        )
        .await
        .unwrap();
    sqlite_execution
        .claim_tool_call(
            &tool_call.id,
            event(&sqlite_admission, EventKind::ToolCallRunning),
        )
        .await
        .unwrap();
    assert_canonical_parity(&memory, &sqlite, &session).await;
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![ContentBlock::Reasoning {
            text: "result".into(),
            provider_state: Some(opaque_numbers()),
        }],
    };
    let mut result_message = assistant(&sqlite_admission, "");
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
    sqlite_execution
        .settle_tool_call(
            &tool_call.id,
            result,
            result_message,
            event(&sqlite_admission, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    assert_canonical_parity(&memory, &sqlite, &session).await;
    let mut inbox = input(&session, "");
    inbox.parts[0].content = ContentBlock::Reasoning {
        text: "pending".into(),
        provider_state: Some(opaque_numbers()),
    };
    memory
        .enqueue_inbox(&session, InboxKind::FollowUp, inbox.clone())
        .await
        .unwrap();
    sqlite
        .enqueue_inbox(&session, InboxKind::FollowUp, inbox)
        .await
        .unwrap();
    let memory_claimed = memory_execution
        .claim_inbox_into_history(InboxKind::FollowUp)
        .await
        .unwrap();
    let sqlite_claimed = sqlite_execution
        .claim_inbox_into_history(InboxKind::FollowUp)
        .await
        .unwrap();
    let mut memory_claimed = memory_claimed;
    let mut sqlite_claimed = sqlite_claimed;
    memory_claimed[0].run_id = None;
    sqlite_claimed[0].run_id = None;
    assert_eq!(
        serde_json::to_vec(&sqlite_claimed).unwrap(),
        serde_json::to_vec(&memory_claimed).unwrap()
    );
    assert_canonical_parity(&memory, &sqlite, &session).await;
    let mut inbox = input(&session, "");
    inbox.parts[0].content = ContentBlock::Reasoning {
        text: "claimed without history".into(),
        provider_state: Some(opaque_numbers()),
    };
    memory
        .enqueue_inbox(&session, InboxKind::Steer, inbox.clone())
        .await
        .unwrap();
    sqlite
        .enqueue_inbox(&session, InboxKind::Steer, inbox)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(
            &sqlite_execution
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
    let (_setup, path, _directory) = temp_store().await;
    let (sqlite, _unused) = seed(&path).await;
    let memory = crate::MemoryStore::with_clock(sqlite.clock.clone());
    let session = SessionId::new();
    let mut admission = request(&session);
    admission.user_message.created_at = OffsetDateTime::UNIX_EPOCH;
    admission.user_message.parts[0].kind = PartKind::ProviderState;
    admission.user_message.parts[0].content = ContentBlock::ProviderState {
        codec_id: "fixture".into(),
        payload: serde_json::json!({"awkward_float":f64::from_bits(AWKWARD_FLOAT_BITS)}),
    };
    memory.admit_run(admission.clone()).await.unwrap();
    sqlite.admit_run(admission).await.unwrap();
    let mut request = query(&session);
    request.limits.tool_calls = 0;
    request.limits.parts = 1;
    request.limits.text_bytes = 0;
    let expected = page(memory.snapshot(request.clone()).await.unwrap());
    let bytes = serde_json::to_vec(&expected.messages[0]).unwrap().len();
    request.limits.encoded_bytes = bytes - 1;
    for store in [&memory as &dyn Store, &sqlite as &dyn Store] {
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
