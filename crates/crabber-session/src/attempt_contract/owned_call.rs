use super::{applied, reserve};
use crate::{
    AttemptCleanupResult, AttemptCommand, AttemptError as Error, AttemptFinish, AttemptMutation,
    Store,
};
use crabber_core::{
    Clock, ContentBlock, ControlId, EventKind, EventRecord, ManualClock, Message, MessageId, Part,
    PartId, PartKind, Role, Run, ToolCallId, ToolCallRecord, ToolCallStatus, ToolResult,
    ToolResultStatus,
};
use serde_json::json;

pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let (reserved, authority) = applied(
        execution
            .mutate_attempt(
                reserve(store.session_revision(&admitted.session.id).await.unwrap()),
                None,
            )
            .await
            .unwrap(),
    );
    let target = reserved.attempt.target;
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::Begin {
            target: target.clone(),
        },
    };
    execution
        .mutate_attempt(begin, authority.clone())
        .await
        .unwrap();
    let call = ToolCallId::new();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call.clone(),
                run_id: admitted.run.id.clone(),
                name: "owned".into(),
                arguments: json!({}),
                status: ToolCallStatus::Pending,
                result: None,
                retry_safe: false,
            },
            event(&admitted.run, EventKind::ToolCallPending),
        )
        .await
        .unwrap();
    execution
        .claim_tool_call(&call, event(&admitted.run, EventKind::ToolCallRunning))
        .await
        .unwrap();
    let finish = AttemptMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
        command: AttemptCommand::Finish {
            target: target.clone(),
            outcome: AttemptFinish::Suspended,
        },
    };
    let (finished, _) = applied(
        execution
            .mutate_attempt(finish, authority.clone())
            .await
            .unwrap(),
    );
    let mut cleanup = AttemptMutation {
        id: ControlId::new(),
        expected_revision: finished.revision,
        command: AttemptCommand::RecordCleanup {
            target,
            result: AttemptCleanupResult::Complete,
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(cleanup.clone(), authority.clone())
            .await
            .unwrap_err(),
        Error::CleanupBlocked
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        finished.revision
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&admitted.session.id, &cleanup.id)
            .await
            .unwrap(),
        None
    );
    settle(execution.as_ref(), &admitted.run, &call).await;
    cleanup.expected_revision = store.session_revision(&admitted.session.id).await.unwrap();
    execution.mutate_attempt(cleanup, authority).await.unwrap();
}
fn event(run: &Run, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: json!({}),
        correlation: None,
        live_only: false,
        created_at: run.created_at,
    }
}
async fn settle(execution: &dyn crate::ExecutionStore, run: &Run, call: &ToolCallId) {
    let id = MessageId::new();
    let message = Message {
        id: id.clone(),
        session_id: run.session_id.clone(),
        run_id: Some(run.id.clone()),
        role: Role::Tool,
        parent_id: None,
        created_at: run.created_at,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind: PartKind::FunctionToolResult,
            content: ContentBlock::ToolResult {
                call_id: call.clone(),
                content: vec![],
                is_error: false,
            },
        }],
    };
    execution
        .settle_tool_call(
            call,
            ToolResult {
                status: ToolResultStatus::Completed,
                content: vec![],
            },
            message,
            event(run, EventKind::ToolCallSettled),
        )
        .await
        .unwrap();
}
