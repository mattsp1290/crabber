//! Advance trusted fixture metadata to verify terminal barrier release; no executor runs.
use super::fixture::Fixture;
use crate::{
    ApprovalGrant, AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation,
    ExecutionStore, RunBoundary, Store,
};
use crabber_core::{
    Clock, ContentBlock, ControlId, EventKind, EventRecord, ManualClock, Message, MessageId, Part,
    PartId, PartKind, Role, RunStatus, ToolResult, ToolResultStatus, Usage,
};
use serde_json::json;

pub(super) async fn complete(
    store: &dyn Store,
    clock: &ManualClock,
    f: &Fixture,
    execution: &dyn ExecutionStore,
    grant: &ApprovalGrant,
) {
    for call in &grant.calls {
        let denied = call.decision == crate::GrantedDecision::Deny;
        let content = vec![ContentBlock::Text {
            text: if denied { "declined" } else { "fixture result" }.into(),
        }];
        execution
            .claim_tool_call(
                &call.target.call_id,
                event(
                    f,
                    clock,
                    EventKind::ToolCallRunning,
                    json!({"call_id":call.target.call_id,"name":"prepared","status":"running"}),
                ),
            )
            .await
            .unwrap();
        let message_id = MessageId::new();
        let message = Message {
            id: message_id.clone(),
            session_id: f.run.session_id.clone(),
            run_id: Some(f.run.id.clone()),
            role: Role::Tool,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id: message_id.clone(),
                ordinal: 0,
                kind: PartKind::FunctionToolResult,
                content: ContentBlock::ToolResult {
                    call_id: call.target.call_id.clone(),
                    content: content.clone(),
                    is_error: denied,
                },
            }],
            created_at: clock.now(),
        };
        let result = ToolResult {
            status: if denied {
                ToolResultStatus::Failed
            } else {
                ToolResultStatus::Completed
            },
            content: content.clone(),
        };
        let settled = event(
            f,
            clock,
            EventKind::ToolCallSettled,
            json!({"call_id":call.target.call_id,"message_id":message_id,"content":content,"is_error":denied}),
        );
        execution
            .settle_tool_call(&call.target.call_id, result, message, settled)
            .await
            .unwrap();
    }
    execution
        .conclude_attempt(
            AttemptConclusion {
                finish: AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
                    command: AttemptCommand::Finish {
                        target: f.batch.next_attempt.clone(),
                        outcome: AttemptFinish::Completed,
                    },
                },
                boundary: RunBoundary::Terminal {
                    status: RunStatus::Completed,
                    error: None,
                    usage: Usage::default(),
                },
                created_at: clock.now(),
                turn_id: None,
                elapsed: std::time::Duration::ZERO,
            },
            Some(grant.authority.clone()),
        )
        .await
        .unwrap();
    let snapshot = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, crate::ApprovalContinuationState::Released);
    assert_eq!(
        store.get_run(&f.run.id).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}
fn event(
    f: &Fixture,
    clock: &ManualClock,
    kind: EventKind,
    payload: serde_json::Value,
) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        turn_id: None,
        kind,
        payload,
        correlation: None,
        live_only: false,
        created_at: clock.now(),
    }
}
