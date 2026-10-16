//! One prepared batch with frozen decisions and ordered terminal boundaries.
use super::{
    AtomicBool, CancellationToken, ExecutionMode, ExecutionStore, Orchestrator, RunPlan,
    RunSession, RuntimeError, ToolCallRecord, TurnId,
};
use crabber_session::{ApprovalError, CallApproval, GrantedApproval, PreparedApproval};
use futures::StreamExt;
use std::sync::atomic::Ordering;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

#[derive(Clone)]
pub(super) struct Batch<'a> {
    pub execution: &'a dyn ExecutionStore,
    pub session: &'a RunSession,
    pub run: &'a crabber_core::RunId,
    pub plan: &'a RunPlan,
    pub turn: Option<TurnId>,
    pub usage: &'a crabber_core::Usage,
    pub turns_used: usize,
    pub cancellation: &'a CancellationToken,
    pub lease_lost: &'a AtomicBool,
}
struct SettlementOrder {
    done: Mutex<Vec<bool>>,
    sender: watch::Sender<usize>,
}
impl SettlementOrder {
    fn complete(&self, index: usize) {
        let mut done = self.done.lock().unwrap();
        done[index] = true;
        let next = done.iter().position(|done| !done).unwrap_or(done.len());
        self.sender.send_replace(next);
    }
}
impl Orchestrator {
    pub(super) async fn execute_prepared_tools(
        &self,
        scope: Batch<'_>,
        calls: Vec<ToolCallRecord>,
        approvals: Vec<GrantedApproval>,
    ) -> Result<(), RuntimeError> {
        let frozen = approvals.clone();
        let prepared = ready(calls, approvals)?;
        let result = async {
            match self.execution_mode {
                ExecutionMode::Sequential => {
                    for (record, approval) in prepared {
                        self.execute_tool(scope.clone(), record, approval, None)
                            .await?;
                        if self.has_tool_suspension() {
                            break;
                        }
                    }
                    Ok(())
                }
                ExecutionMode::Parallel { max } => {
                    let (sender, receiver) = watch::channel(0usize);
                    let order = Arc::new(SettlementOrder {
                        done: Mutex::new(vec![false; prepared.len()]),
                        sender,
                    });
                    let halted = Arc::new(AtomicBool::new(false));
                    let results = futures::stream::iter(prepared.into_iter().enumerate().map(
                        |(index, (record, approval))| {
                            let receiver = receiver.clone();
                            let order = order.clone();
                            let halted = halted.clone();
                            let scope = &scope;
                            async move {
                                if halted.load(Ordering::SeqCst) {
                                    order.complete(index);
                                    return Err(RuntimeError::TaskStopped);
                                }
                                let result = self
                                    .execute_tool(
                                        scope.clone(),
                                        record,
                                        approval,
                                        Some((index, receiver, order.sender.clone())),
                                    )
                                    .await;
                                // A failed local phase must not strand completed siblings.
                                // Its error still prevents another model turn or a false run terminal.
                                if result.is_err() || self.has_tool_suspension() {
                                    halted.store(true, Ordering::SeqCst);
                                }
                                order.complete(index);
                                result
                            }
                        },
                    ))
                    .buffer_unordered(max)
                    .collect::<Vec<_>>()
                    .await;
                    // Retain the strongest recovery requirement after all started
                    // siblings have reached their own boundary.
                    if let Some(index) = results.iter().position(|result| {
                        matches!(
                            result,
                            Err(RuntimeError::PhaseOutcomeUnknown { .. }
                                | RuntimeError::ToolOutcomeUnknown { .. }
                                | RuntimeError::AttemptCleanupIncomplete { .. })
                        )
                    }) {
                        return results.into_iter().nth(index).unwrap();
                    }
                    for result in results {
                        if matches!(result, Err(RuntimeError::TaskStopped))
                            && self.has_tool_suspension()
                        {
                            continue;
                        }
                        result?;
                    }
                    Ok(())
                }
            }
        }
        .await;
        if self.has_tool_suspension() {
            if result.is_err() {
                return Err(self.fail_tool_suspensions(&scope).await);
            }
            return self.commit_tool_suspensions(&scope, frozen).await;
        }
        result
    }
}
fn ready(
    calls: Vec<ToolCallRecord>,
    approvals: Vec<GrantedApproval>,
) -> Result<Vec<(ToolCallRecord, GrantedApproval)>, ApprovalError> {
    if calls.len() != approvals.len() {
        return Err(ApprovalError::SemanticConflict);
    }
    let count = calls.len();
    let mut by_id: BTreeMap<_, _> = calls
        .into_iter()
        .map(|call| (call.id.clone(), call))
        .collect();
    if by_id.len() != count {
        return Err(ApprovalError::SemanticConflict);
    }
    approvals
        .into_iter()
        .map(|approval| {
            let call = by_id
                .remove(&approval.target.call_id)
                .ok_or(ApprovalError::SemanticConflict)?;
            let bound =
                PreparedApproval::new(approval.target.clone(), &call, CallApproval::NotRequired)?;
            if bound.call_binding != approval.call_binding {
                return Err(ApprovalError::SemanticConflict);
            }
            Ok((call, approval))
        })
        .collect()
}

/// A generic native pause froze every prepared call before any phase began.
/// Restore that exact order; Store enumeration order has no execution meaning.
pub(super) fn restore_paused_calls(
    run: &crabber_core::Run,
    calls: Vec<ToolCallRecord>,
) -> Result<Vec<ToolCallRecord>, ApprovalError> {
    let checkpoint = run
        .checkpoint
        .as_ref()
        .ok_or(ApprovalError::SemanticConflict)?;
    if checkpoint
        .get("version")
        .and_then(serde_json::Value::as_u64)
        != Some(4)
    {
        return Err(ApprovalError::SemanticConflict);
    }
    if let Some(turn) = checkpoint
        .get("source_turn_id")
        .filter(|value| !value.is_null())
    {
        let turn = turn.as_str().ok_or(ApprovalError::SemanticConflict)?;
        if turn.is_empty() || turn.len() > 512 || turn.chars().any(char::is_control) {
            return Err(ApprovalError::SemanticConflict);
        }
    }
    let ids = checkpoint
        .get("pending_calls")
        .and_then(serde_json::Value::as_array)
        .ok_or(ApprovalError::SemanticConflict)?;
    if ids.is_empty() || ids.len() > crabber_session::MAX_APPROVAL_CALLS || ids.len() != calls.len()
    {
        return Err(ApprovalError::SemanticConflict);
    }
    let count = calls.len();
    let mut by_id: BTreeMap<_, _> = calls
        .into_iter()
        .map(|call| (call.id.clone(), call))
        .collect();
    if by_id.len() != count {
        return Err(ApprovalError::SemanticConflict);
    }
    ids.iter()
        .map(|id| {
            let id = id.as_str().ok_or(ApprovalError::SemanticConflict)?;
            if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
                return Err(ApprovalError::SemanticConflict);
            }
            let call = by_id
                .remove(&crabber_core::ToolCallId(id.to_owned()))
                .ok_or(ApprovalError::SemanticConflict)?;
            if call.run_id != run.id
                || call.status != crabber_core::ToolCallStatus::Pending
                || call.result.is_some()
            {
                return Err(ApprovalError::SemanticConflict);
            }
            Ok(call)
        })
        .collect()
}
