use super::{claim, fresh, retirement};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalReply,
    AttemptCommand, AttemptConclusion, AttemptError, AttemptFinish, AttemptMutation,
    AttemptMutationOutcome, AttemptTarget, AttemptUncertainty, AttemptUncertaintyCause,
    CallApproval, RunBoundary, Store,
};
use crabber_core::{
    AttemptId, ContentBlock, ControlId, EventKind, EventRecord, ManualClock, Role, ToolCallId,
    ToolCallRecord, ToolCallStatus, Usage,
};
pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    approval::run(store, clock, false).await;
    approval::run(store, clock, true).await;
    checkpoint::run(store, false).await;
    checkpoint::run(store, true).await;
}
async fn result_order(store: &dyn Store, session: &crabber_core::SessionId) -> Vec<ToolCallId> {
    store
        .list_all_messages(session)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.role == Role::Tool)
        .flat_map(|m| m.parts)
        .filter_map(|part| match part.content {
            ContentBlock::ToolResult { call_id, .. } => Some(call_id),
            _ => None,
        })
        .collect()
}

async fn historical_turn(execution: &dyn crate::ExecutionStore, run: &crabber_core::Run) {
    let old = crabber_core::TurnId::new();
    source_event(execution, run, old.clone(), EventKind::TurnStarted).await;
    source_event(execution, run, old, EventKind::TurnCompleted).await;
}
async fn source_event(
    execution: &dyn crate::ExecutionStore,
    run: &crabber_core::Run,
    turn: crabber_core::TurnId,
    kind: EventKind,
) {
    execution
        .append_event(EventRecord {
            cursor: None,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            turn_id: Some(turn),
            kind,
            payload: serde_json::Value::Null,
            correlation: None,
            live_only: false,
            created_at: run.created_at,
        })
        .await
        .unwrap();
}

#[path = "ordering/approval.rs"]
mod approval;
#[path = "ordering/checkpoint.rs"]
mod checkpoint;
