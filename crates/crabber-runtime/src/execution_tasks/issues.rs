//! Safe original outcome facts survive result-subscriber loss and task reaping.
use super::{ExecutionTasks, RootScope, RunId};
use crate::RuntimeError;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunCloseIssueKind {
    NativeOutcomeUnknown,
    CleanupIncomplete,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCloseIssue {
    pub run_id: RunId,
    pub kind: RunCloseIssueKind,
}
impl RootScope {
    pub(crate) fn record_business_failure(&self, run: &RunId, error: &RuntimeError) {
        let kind = match error {
            RuntimeError::AttemptCleanupIncomplete { .. } => RunCloseIssueKind::CleanupIncomplete,
            RuntimeError::TerminalOutcomeUnknown { .. }
            | RuntimeError::AttemptBoundaryUnknown { .. }
            | RuntimeError::PhaseOutcomeUnknown { .. }
            | RuntimeError::ToolOutcomeUnknown { .. }
            | RuntimeError::ApprovalBoundaryUnknown { .. }
            | RuntimeError::ToolContinuationBoundaryUnknown { .. }
            | RuntimeError::PreparationRecoveryRequired
            | RuntimeError::SessionOwner(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome,
            ) => RunCloseIssueKind::NativeOutcomeUnknown,
            _ => return,
        };
        self.record_issue(RunCloseIssue {
            run_id: run.clone(),
            kind,
        });
    }
}
impl ExecutionTasks {
    pub(crate) fn run_issues(&self) -> Vec<RunCloseIssue> {
        self.owners.lock().unwrap().run_issues.clone()
    }
}
