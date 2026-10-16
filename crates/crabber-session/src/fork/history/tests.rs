use super::*;
use crate::SessionDefaults;
use crabber_core::{ControlId, SessionRevision};
use serde_json::json;

fn message(id: &str, role: Role, content: Vec<ContentBlock>) -> Message {
    Message {
        id: id.into(),
        session_id: "source".into(),
        run_id: Some("actual-source-run".into()),
        role,
        parent_id: None,
        parts: content
            .into_iter()
            .enumerate()
            .map(|(ordinal, content)| crabber_core::Part {
                id: PartId(format!("{id}-{ordinal}")),
                message_id: id.into(),
                ordinal: u32::try_from(ordinal).unwrap(),
                kind: match content {
                    ContentBlock::ToolCall { .. } => PartKind::FunctionToolCall,
                    ContentBlock::ToolResult { .. } => PartKind::FunctionToolResult,
                    ContentBlock::Reasoning { .. } => PartKind::Reasoning,
                    _ => PartKind::AssistantText,
                },
                content,
            })
            .collect(),
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
fn request(message_id: &str) -> ForkSession {
    ForkSession {
        id: ControlId::new(),
        source_session_id: "source".into(),
        workspace_id: "source-workspace".into(),
        directory: "/source-workspace".into(),
        expected_source_revision: SessionRevision::INITIAL,
        boundary: ForkBoundary::AfterMessage {
            message_id: message_id.into(),
        },
        target_session_id: "target".into(),
        title: "fork".into(),
        defaults: SessionDefaults::default(),
        limits: crate::SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 128,
            text_bytes: 1_048_576,
            encoded_bytes: 2_097_152,
        },
        max_proof_bytes: 32 * 1024 * 1024,
    }
}
fn logical(id: &str) -> ToolCallRecord {
    ToolCallRecord {
        id: id.into(),
        run_id: "actual-source-run".into(),
        name: "effect".into(),
        arguments: json!({"literal_call_id":id}),
        status: ToolCallStatus::Completed,
        retry_safe: false,
        result: Some(ToolResult {
            status: ToolResultStatus::Completed,
            content: vec![ContentBlock::Text { text: id.into() }],
        }),
    }
}
fn start(call: &ToolCallRecord) -> ContentBlock {
    ContentBlock::ToolCall {
        call_id: call.id.clone(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
    }
}
fn settle(call: &ToolCallRecord) -> ContentBlock {
    ContentBlock::ToolResult {
        call_id: call.id.clone(),
        content: call.result.as_ref().unwrap().content.clone(),
        is_error: false,
    }
}
#[test]
fn parallel_group_never_forks_a_partial_result_prefix() {
    let first = logical("first");
    let second = logical("second");
    let mut messages = vec![
        message(
            "user",
            Role::User,
            vec![ContentBlock::Text {
                text: "work".into(),
            }],
        ),
        message(
            "assistant",
            Role::Assistant,
            vec![start(&first), start(&second)],
        ),
        message("result-first", Role::Tool, vec![settle(&first)]),
    ];
    let calls = [SourceCall::Native(&first), SourceCall::Native(&second)];
    let mut closed = messages.iter().map(|m| m.id.clone()).collect();
    assert!(
        matches!(prepare(&request("result-first"), &messages, &calls, &closed).unwrap(),
        Decision::Unsafe(ForkBoundary::AfterMessage { message_id }) if message_id == MessageId::from("user"))
    );
    messages.push(message("result-second", Role::Tool, vec![settle(&second)]));
    closed.insert("result-second".into());
    let Decision::Ready(copied) =
        prepare(&request("result-second"), &messages, &calls, &closed).unwrap()
    else {
        panic!("complete group")
    };
    let retained = [
        SourceCall::Native(&first),
        SourceCall::UnquiescentNative(&second),
    ];
    assert!(
        matches!(prepare(&request("result-second"), &messages, &retained, &closed).unwrap(), Decision::Unsafe(ForkBoundary::AfterMessage { message_id }) if message_id == MessageId::from("user"))
    );
    assert_eq!(copied.calls.len(), 2);
    assert!(
        copied
            .messages
            .iter()
            .all(|m| m.run_id.is_none() && m.session_id.0 == "target")
    );
    for imported in copied.calls {
        assert_ne!(imported.id, imported.source_call_id);
        assert_eq!(imported.source_run_id, Some("actual-source-run".into()));
        assert_eq!(
            imported.arguments["literal_call_id"],
            json!(imported.source_call_id)
        );
        assert!(
            copied
                .messages
                .iter()
                .any(|m| m.id == imported.result_message_id)
        );
    }
}
#[test]
fn copied_history_redacts_private_state_and_rewrites_only_typed_ids() {
    let call = logical("original-call");
    let mut assistant = message(
        "assistant",
        Role::Assistant,
        vec![
            ContentBlock::Reasoning {
                text: "private-reasoning".into(),
                provider_state: Some(json!("private-codec")),
            },
            start(&call),
        ],
    );
    assistant.parent_id = Some("user".into());
    let messages = vec![
        message(
            "user",
            Role::User,
            vec![ContentBlock::Text {
                text: "original-call".into(),
            }],
        ),
        assistant,
        message("result", Role::Tool, vec![settle(&call)]),
    ];
    let calls = [SourceCall::Native(&call)];
    let closed = messages.iter().map(|m| m.id.clone()).collect();
    let Decision::Ready(copied) = prepare(&request("result"), &messages, &calls, &closed).unwrap()
    else {
        panic!("closed history")
    };
    assert_eq!(
        copied.messages[1].parent_id.as_ref(),
        Some(&copied.messages[0].id)
    );
    assert_eq!(copied.messages[1].parts.len(), 1);
    assert_eq!(copied.messages[1].parts[0].ordinal, 0);
    assert_eq!(copied.provenance[1].parts.len(), 1);
    let wire = serde_json::to_string(&copied.messages).unwrap();
    assert!(!wire.contains("private-reasoning") && !wire.contains("private-codec"));
    assert!(wire.contains("original-call")); // Literal text and argument JSON survive.
    assert!(
        matches!(&copied.messages[1].parts[0].content, ContentBlock::ToolCall { call_id, .. } if call_id == &copied.calls[0].id)
    );
    assert!(
        matches!(&copied.messages[2].parts[0].content, ContentBlock::ToolResult { call_id, .. } if call_id == &copied.calls[0].id)
    );
}
#[test]
fn missing_commit_and_unresolved_interrupt_require_an_explicit_earlier_cut() {
    let mut call = logical("interrupted");
    call.status = ToolCallStatus::Interrupted;
    call.result.as_mut().unwrap().status = ToolResultStatus::Interrupted;
    let mut result = settle(&call);
    let ContentBlock::ToolResult { is_error, .. } = &mut result else {
        unreachable!()
    };
    *is_error = true;
    let messages = vec![
        message("user", Role::User, vec![]),
        message("assistant", Role::Assistant, vec![start(&call)]),
        message("result", Role::Tool, vec![result]),
    ];
    let calls = [SourceCall::Native(&call)];
    let closed = messages.iter().map(|m| m.id.clone()).collect();
    assert!(
        matches!(prepare(&request("result"), &messages, &calls, &closed).unwrap(),
        Decision::Unsafe(ForkBoundary::AfterMessage { message_id }) if message_id.0 == "user")
    );
    let message = message(
        "unfinished",
        Role::Assistant,
        vec![ContentBlock::Text {
            text: "partial".into(),
        }],
    );
    assert!(matches!(
        prepare(&request("unfinished"), &[message], &[], &BTreeSet::new()).unwrap(),
        Decision::Unsafe(ForkBoundary::Empty)
    ));
}

#[test]
fn presentation_ids_are_opaque_and_redaction_cannot_drop_a_paired_call() {
    assert!(validate_id(&"原".repeat(170)).is_ok());
    for bad in [
        "".into(),
        "原".repeat(171),
        "provider\ncall".into(),
        "provider\0call".into(),
    ] {
        assert!(validate_id(&bad).is_err());
    }
    let call = logical("provider:原本/call");
    let mut messages = vec![
        message(
            "assistant:原本/message",
            Role::Assistant,
            vec![start(&call)],
        ),
        message("result:原本/message", Role::Tool, vec![settle(&call)]),
    ];
    messages[0].parts[0].id = "provider:原本/part".into();
    let calls = [SourceCall::Native(&call)];
    let committed = messages.iter().map(|message| message.id.clone()).collect();
    assert!(matches!(
        prepare(
            &request("result:原本/message"),
            &messages,
            &calls,
            &committed
        )
        .unwrap(),
        Decision::Ready(_)
    ));
    messages[0].parts[0].kind = PartKind::Reasoning;
    assert!(matches!(
        prepare(
            &request("result:原本/message"),
            &messages,
            &calls,
            &committed
        ),
        Err(Error::UnknownStoreOutcome)
    ));
}
