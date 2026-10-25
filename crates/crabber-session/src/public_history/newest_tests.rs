//! Shared native call/result projection and bounded newest-record behavior.
use crate::*;
use crabber_core::*;
use time::OffsetDateTime;
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
        created_at: OffsetDateTime::now_utc(),
    }
}
pub(crate) fn request(session: &SessionId) -> SnapshotRequest {
    SnapshotRequest {
        session_id: session.clone(),
        limits: crate::public_session::v2::tests::limits().history,
        continuation: None,
    }
}
async fn closed_tool(store: &dyn Store) -> (SessionId, Message, Message, ToolCallId) {
    let (session, original) = super::tests::seed(store).await;
    let run = store
        .get_run(original.run_id.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let execution = store
        .execution(RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token,
        })
        .await
        .unwrap();
    let ContentBlock::ToolResult { call_id, .. } = &original.parts[4].content else {
        unreachable!()
    };
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call_id.clone(),
                run_id: run.id.clone(),
                name: "echo".into(),
                arguments: serde_json::json!({"visible":true}),
                status: ToolCallStatus::Pending,
                retry_safe: false,
                result: None,
            },
            event(&session, &run.id, EventKind::ToolCallPending),
        )
        .await
        .unwrap();
    let mut assistant = original.clone();
    assistant.id = MessageId::new();
    assistant.parts = vec![Part {
        id: PartId::new(),
        message_id: assistant.id.clone(),
        ordinal: 0,
        kind: PartKind::FunctionToolCall,
        content: ContentBlock::ToolCall {
            call_id: call_id.clone(),
            name: "echo".into(),
            arguments: serde_json::json!({"visible":true}),
        },
    }];
    execution.append_message(assistant.clone()).await.unwrap();
    execution
        .claim_tool_call(
            call_id,
            event(&session, &run.id, EventKind::ToolCallRunning),
        )
        .await
        .unwrap();
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![
            ContentBlock::ProviderState {
                codec_id: "private".into(),
                payload: serde_json::json!({"secret":"PRIVATE_RESULT"}),
            },
            ContentBlock::Text {
                text: "public result".into(),
            },
        ],
    };
    let mut message = original.clone();
    message.id = MessageId::new();
    message.role = Role::Tool;
    message.parts = vec![Part {
        id: PartId::new(),
        message_id: message.id.clone(),
        ordinal: 0,
        kind: PartKind::FunctionToolResult,
        content: ContentBlock::ToolResult {
            call_id: call_id.clone(),
            content: result.content.clone(),
            is_error: false,
        },
    }];
    execution
        .settle_tool_call(
            call_id,
            result,
            message.clone(),
            event(&session, &run.id, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
    (session, assistant, message, call_id.clone())
}
pub(crate) async fn tools(store: &dyn Store) {
    let (session, assistant, message, call_id) = closed_tool(store).await;
    let mut req = request(&session);
    req.limits.messages = 2;
    let PublicNewestHistoryOutcome::Page(page) =
        store.public_newest_history_v2(req.clone()).await.unwrap()
    else {
        panic!("tool window")
    };
    assert_eq!(
        page.messages.iter().map(|m| &m.id).collect::<Vec<_>>(),
        [&assistant.id, &message.id]
    );
    assert_eq!(page.tool_calls.len(), 1);
    assert_eq!(page.tool_calls[0].id, call_id);
    assert_eq!(
        page.tool_calls[0].result.as_ref().unwrap().content,
        vec![ContentBlock::Text {
            text: "public result".into()
        }]
    );
    assert!(page.omitted_older_messages);
    let json = serde_json::to_string(&page).unwrap();
    assert!(!json.contains("PRIVATE_RESULT"));
    assert!(!json.contains("private-"));
    req.limits.tool_calls = 0;
    assert!(matches!(
        store.public_newest_history_v2(req.clone()).await.unwrap(),
        PublicNewestHistoryOutcome::Limited(SnapshotLimit::ToolCalls)
    ));
    req.limits.tool_calls = 64;
    req.limits.encoded_bytes = 1;
    assert!(matches!(
        store.public_newest_history_v2(req).await.unwrap(),
        PublicNewestHistoryOutcome::Limited(SnapshotLimit::EncodedBytes)
    ));
}
pub(crate) async fn raw_bytes(store: &dyn Store, session: &SessionId, total: usize, newest: usize) {
    let mut req = request(session);
    req.limits.encoded_bytes = total - 1;
    let PublicNewestHistoryOutcome::Page(page) =
        store.public_newest_history_v2(req.clone()).await.unwrap()
    else {
        panic!("bounded raw-cost window")
    };
    assert_eq!(page.messages.len(), 1);
    assert!(page.omitted_older_messages);
    assert_eq!(page.read_usage.encoded_bytes, newest);
    req.limits.encoded_bytes = newest - 1;
    assert!(matches!(
        store.public_newest_history_v2(req).await.unwrap(),
        PublicNewestHistoryOutcome::Limited(SnapshotLimit::EncodedBytes)
    ));
}
