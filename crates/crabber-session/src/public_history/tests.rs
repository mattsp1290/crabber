use super::*;
use crate::{SnapshotLimits, SnapshotRequest, Store};
use crabber_core::{MessageId, Part, PartId, Role, SessionId, ToolCallId};

fn secret() -> ContentBlock {
    ContentBlock::ProviderState {
        codec_id: "private-codec".into(),
        payload: serde_json::json!({"secret":"private-payload"}),
    }
}
fn reasoning() -> ContentBlock {
    ContentBlock::Reasoning {
        text: "private-reasoning".into(),
        provider_state: Some(serde_json::json!({"secret":"private-state"})),
    }
}
fn nested(call: &ToolCallId) -> ContentBlock {
    ContentBlock::ToolResult {
        call_id: call.clone(),
        is_error: false,
        content: vec![
            secret(),
            reasoning(),
            ContentBlock::ToolResult {
                call_id: call.clone(),
                is_error: true,
                content: vec![
                    secret(),
                    reasoning(),
                    ContentBlock::Text {
                        text: "visible".into(),
                    },
                ],
            },
        ],
    }
}

async fn seed(store: &dyn Store) -> (SessionId, Message) {
    let session = SessionId::new();
    let mut fixture = crate::execution_binding_contract::fixture(
        &session,
        time::OffsetDateTime::now_utc(),
        "public-history",
        'c',
    );
    fixture.keyed.request.session_id = None;
    let parent = fixture.keyed.request.user_message.id.clone();
    let admitted = store.admit_run(fixture.keyed.request).await.unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    let id = MessageId::new();
    let call = ToolCallId::new();
    let mut message = Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: Some(admitted.run.id),
        role: Role::Assistant,
        parent_id: Some(parent),
        parts: vec![],
        created_at: time::OffsetDateTime::now_utc(),
    };
    for (kind, content) in [
        (
            PartKind::ProviderState,
            ContentBlock::Text {
                text: "private-kind".into(),
            },
        ),
        (
            PartKind::Reasoning,
            ContentBlock::Text {
                text: "private-kind-reasoning".into(),
            },
        ),
        (PartKind::AssistantText, secret()),
        (PartKind::AssistantText, reasoning()),
        (PartKind::FunctionToolResult, nested(&call)),
        (
            PartKind::Media,
            ContentBlock::Media {
                uri: "https://example.test/image".into(),
                mime_type: "image/png".into(),
            },
        ),
    ] {
        message.parts.push(Part {
            id: PartId::new(),
            message_id: id.clone(),
            ordinal: u32::try_from(message.parts.len()).unwrap(),
            kind,
            content,
        });
    }
    execution.append_message(message.clone()).await.unwrap();
    (session, message)
}

pub(crate) async fn run(store: &dyn Store) {
    let (session, message) = seed(store).await;
    let call = match &message.parts[4].content {
        ContentBlock::ToolResult { call_id, .. } => call_id.clone(),
        _ => unreachable!(),
    };
    let request = SnapshotRequest {
        session_id: session,
        continuation: None,
        limits: SnapshotLimits {
            messages: 1,
            tool_calls: 10,
            parts: 100,
            text_bytes: 100_000,
            encoded_bytes: 1_000_000,
        },
    };
    let PublicHistoryOutcome::Page(first) = store.public_history(request.clone()).await.unwrap()
    else {
        panic!("first page")
    };
    assert_eq!(first.messages.len(), 1);
    let mut next = request.clone();
    next.continuation = first.continuation;
    let PublicHistoryOutcome::Page(second) = store.public_history(next.clone()).await.unwrap()
    else {
        panic!("second page")
    };
    let projected = &second.messages[0];
    assert_eq!(projected.id, message.id);
    assert_eq!(projected.parent_id, message.parent_id);
    assert_eq!(
        projected.parts,
        vec![
            Part {
                content: ContentBlock::ToolResult {
                    call_id: call.clone(),
                    is_error: false,
                    content: vec![ContentBlock::ToolResult {
                        call_id: call.clone(),
                        is_error: true,
                        content: vec![ContentBlock::Text {
                            text: "visible".into()
                        }],
                    }],
                },
                ..message.parts[4].clone()
            },
            message.parts[5].clone(),
        ]
    );
    assert_eq!(second.captured_revision, first.captured_revision);
    assert_eq!(second.high_water, first.high_water);
    assert!(second.continuation.is_none());
    let encoded = serde_json::to_string(&second).unwrap();
    assert!(!encoded.contains("private-"));
    assert!(second.read_usage.parts > projected.parts.len());
    // Redaction is downstream of the raw preflight: private bytes cannot bypass
    // bounded allocation even when the visible projection would fit.
    next.limits.encoded_bytes = 1;
    assert!(matches!(
        store.public_history(next).await.unwrap(),
        PublicHistoryOutcome::Limited {
            limit: SnapshotLimit::EncodedBytes,
            ..
        }
    ));
    let crate::SnapshotOutcome::Page(raw) = store
        .snapshot(SnapshotRequest {
            limits: SnapshotLimits {
                messages: 10,
                ..request.limits
            },
            ..request
        })
        .await
        .unwrap()
    else {
        panic!("raw page")
    };
    assert!(
        serde_json::to_string(&raw)
            .unwrap()
            .contains("private-payload")
    );
}

#[test]
fn tool_record_results_are_recursively_redacted() {
    let call = ToolCallId::new();
    let mut expected = ToolCallRecord {
        id: call.clone(),
        run_id: crabber_core::RunId::new(),
        name: "tool".into(),
        arguments: serde_json::json!({"input":1}),
        status: crabber_core::ToolCallStatus::Completed,
        retry_safe: false,
        result: Some(crabber_core::ToolResult {
            status: crabber_core::ToolResultStatus::Completed,
            content: vec![secret(), reasoning(), nested(&call)],
        }),
    };
    let raw = SnapshotPage {
        captured_revision: SessionRevision::INITIAL,
        high_water: EventCursor(0),
        messages: vec![],
        tool_calls: vec![expected.clone()],
        usage: SnapshotUsage::default(),
        continuation: None,
    };
    let projected = PublicHistoryPage::from(raw);
    let result = expected.result.as_mut().unwrap();
    result.content = vec![ContentBlock::ToolResult {
        call_id: call.clone(),
        is_error: false,
        content: vec![ContentBlock::ToolResult {
            call_id: call,
            is_error: true,
            content: vec![ContentBlock::Text {
                text: "visible".into(),
            }],
        }],
    }];
    assert_eq!(projected.tool_calls, vec![expected]);
    assert!(
        !serde_json::to_string(&projected)
            .unwrap()
            .contains("private-")
    );
}
