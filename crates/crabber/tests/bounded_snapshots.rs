//! Credential-free embedding journey through the public facade.
use crabber::{
    SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotRequest,
    core::*,
    session::{AdmitRequest, MemoryStore, Store},
};
use std::time::Duration;
use time::OffsetDateTime;

fn message(
    session: &SessionId,
    run: Option<RunId>,
    id: &str,
    role: Role,
    content: ContentBlock,
) -> Message {
    let id = MessageId::from(id);
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
            kind: PartKind::AssistantText,
            content,
        }],
        created_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn event(session: &SessionId, run: &RunId, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: session.clone(),
        run_id: run.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn request(session: &SessionId) -> SnapshotRequest {
    SnapshotRequest {
        session_id: session.clone(),
        limits: SnapshotLimits {
            messages: 7,
            tool_calls: 1,
            parts: 7,
            text_bytes: 4096,
            encoded_bytes: 8192,
        },
        continuation: None,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn memory_embedding_pages_settled_relations_then_resumes_events() {
    let store = MemoryStore::new();
    let session = SessionId::from("bounded-fixture-session");
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            workspace_id: "fixture".into(),
            directory: "fixture".into(),
            title: "bounded fixture".into(),
            user_message: message(
                &session,
                None,
                "user",
                Role::User,
                ContentBlock::Text {
                    text: "fake".into(),
                },
            ),
            config_hash: "fixture".into(),
            plan_fingerprint: "fixture".into(),
            owner: "fixture".into(),
            lease: Duration::from_secs(600),
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let run = admitted.run.id.clone();
    for index in 0..128 {
        execution
            .append_message(message(
                &session,
                Some(run.clone()),
                &format!("fake-{index}"),
                Role::Assistant,
                ContentBlock::Text {
                    text: "fake text".into(),
                },
            ))
            .await
            .unwrap();
    }
    let call = ToolCallId::from("fake-call");
    let call_message = message(
        &session,
        Some(run.clone()),
        "call-message",
        Role::Assistant,
        ContentBlock::ToolCall {
            call_id: call.clone(),
            name: "fake".into(),
            arguments: serde_json::Value::Null,
        },
    );
    execution
        .append_message(call_message.clone())
        .await
        .unwrap();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call.clone(),
                run_id: run.clone(),
                name: "fake".into(),
                arguments: serde_json::Value::Null,
                status: ToolCallStatus::Pending,
                retry_safe: true,
                result: None,
            },
            event(&session, &run, EventKind::ToolCallPending),
        )
        .await
        .unwrap();
    execution
        .claim_tool_call(&call, event(&session, &run, EventKind::ToolCallRunning))
        .await
        .unwrap();
    let result_content = vec![ContentBlock::Text {
        text: "fake result".into(),
    }];
    let mut result_message = message(
        &session,
        Some(run.clone()),
        "result-message",
        Role::Tool,
        ContentBlock::ToolResult {
            call_id: call.clone(),
            content: result_content.clone(),
            is_error: false,
        },
    );
    result_message.parent_id = Some(call_message.id.clone());
    execution
        .settle_tool_call(
            &call,
            ToolResult {
                status: ToolResultStatus::Completed,
                content: result_content,
            },
            result_message.clone(),
            event(&session, &run, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    let mut expected = vec![MessageId::from("user")];
    expected.extend((0..128).map(|index| MessageId::from(format!("fake-{index}"))));
    expected.extend([call_message.id.clone(), result_message.id.clone()]);
    let mut query = request(&session);
    query.limits.messages = 0;
    let SnapshotOutcome::Limited {
        limit,
        continuation,
        high_water,
    } = store.snapshot(query).await.unwrap()
    else {
        panic!("explicit limit")
    };
    assert_eq!(limit, SnapshotLimit::Messages);
    assert_eq!(high_water, EventCursor(3));
    let mut query = request(&session);
    query.continuation = Some(continuation);
    let mut message_ids = Vec::new();
    let mut saw_call_message = false;
    let mut saw_result_message = false;
    let mut calls = Vec::new();
    let mut page_number = 0;
    loop {
        let SnapshotOutcome::Page(page) = store.snapshot(query.clone()).await.unwrap() else {
            panic!("bounded page")
        };
        assert_eq!(page.high_water, high_water);
        assert!(page.usage.messages <= query.limits.messages);
        assert!(page.usage.tool_calls <= query.limits.tool_calls);
        assert!(page.usage.parts <= query.limits.parts);
        assert!(page.usage.text_bytes <= query.limits.text_bytes);
        assert!(page.usage.encoded_bytes <= query.limits.encoded_bytes);
        let source = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        println!(
            "source={} backend=memory session={} page={} high_water={} messages={} tool_calls={} parts={} text_bytes={} encoded_bytes={}",
            String::from_utf8(source.stdout).unwrap().trim(),
            session,
            page_number,
            high_water.0,
            page.usage.messages,
            page.usage.tool_calls,
            page.usage.parts,
            page.usage.text_bytes,
            page.usage.encoded_bytes
        );
        saw_call_message |= page.messages.contains(&call_message);
        saw_result_message |= page.messages.contains(&result_message);
        message_ids.extend(page.messages.into_iter().map(|message| message.id));
        calls.extend(page.tool_calls);
        if page_number == 0 {
            // Independent clone writes between page reads. Its new history lies
            // outside the frozen cutoffs; its event remains strictly after H.
            let writer = store.clone();
            let writer_session = session.clone();
            let writer_run = run.clone();
            let writer_fence = admitted.fence.clone();
            tokio::spawn(async move {
                let execution = writer.execution(writer_fence).await.unwrap();
                execution
                    .append_message(message(
                        &writer_session,
                        Some(writer_run.clone()),
                        "after-boundary",
                        Role::Assistant,
                        ContentBlock::Text {
                            text: "fake append".into(),
                        },
                    ))
                    .await
                    .unwrap();
                execution
                    .append_event(event(
                        &writer_session,
                        &writer_run,
                        EventKind::ToolCallSettled,
                    ))
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
        }
        page_number += 1;
        let Some(continuation) = page.continuation else {
            break;
        };
        query.continuation = Some(continuation);
    }
    assert_eq!(message_ids, expected);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, call);
    assert_eq!(calls[0].status, ToolCallStatus::Completed);
    assert!(calls[0].result.is_some());
    assert!(saw_call_message);
    assert!(saw_result_message);
    let events = store
        .list_events(&session, Some(high_water), 100)
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|e| e.cursor.unwrap()).collect::<Vec<_>>(),
        vec![EventCursor(4)]
    );
    assert!(
        store
            .list_events(&session, events.last().unwrap().cursor, 100)
            .await
            .unwrap()
            .is_empty()
    );
    println!(
        "post_boundary_event_ids={:?}",
        events
            .iter()
            .map(|e| e.cursor.unwrap().0)
            .collect::<Vec<_>>()
    );
    // Mutation without an event still invalidates captured relationships.
    let SnapshotOutcome::Page(before_mutation) = store.snapshot(request(&session)).await.unwrap()
    else {
        panic!("page before mutation")
    };
    execution
        .append_part(Part {
            id: PartId::new(),
            message_id: MessageId::from("fake-0"),
            ordinal: 1,
            kind: PartKind::AssistantText,
            content: ContentBlock::Text {
                text: "fake mutable part".into(),
            },
        })
        .await
        .unwrap();
    let mut next = request(&session);
    next.continuation = before_mutation.continuation;
    assert!(
        matches!(store.snapshot(next).await.unwrap(), SnapshotOutcome::Invalidated { high_water } if high_water == before_mutation.high_water)
    );
    assert!(matches!(
        store.snapshot(request(&session)).await.unwrap(),
        SnapshotOutcome::Page(_)
    ));
    let mut tool_limited = request(&session);
    tool_limited.limits.messages = 1000;
    tool_limited.limits.parts = 1000;
    tool_limited.limits.encoded_bytes = 1_000_000;
    tool_limited.limits.tool_calls = 0;
    let SnapshotOutcome::Page(messages_only) = store.snapshot(tool_limited.clone()).await.unwrap()
    else {
        panic!("messages before tool cap")
    };
    tool_limited.continuation = messages_only.continuation;
    assert!(matches!(
        store.snapshot(tool_limited).await.unwrap(),
        SnapshotOutcome::Limited {
            limit: SnapshotLimit::ToolCalls,
            ..
        }
    ));
    let mut missing = request(&SessionId::from("missing"));
    missing.limits.messages = 0;
    assert!(matches!(
        store.snapshot(missing).await,
        Err(CoreError::NotFound)
    ));
}
