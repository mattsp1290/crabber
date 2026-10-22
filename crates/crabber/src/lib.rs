#![doc = include_str!("../../../README.md")]

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

pub use crabber_core::{ModelControls, ReasoningEffort};
pub use crabber_runtime::QueueControl;
mod agent;
mod host;
pub use host::ExecutionContext;

pub use agent::{
    Admission, Agent, AgentBuilder, AgentConfig, BuildError, Continuation, ContinueApproval,
    ContinueFrontend, ContinueTool, HostCloseReport, HostExtensionClose, RecoverApproval,
    RecoverFrontend, RunEvents, RunHandle, ToolContinuation,
};
pub use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, EventKind, EventRecord, InputFingerprint,
    SessionContext, SessionId, TraceContext, TraceContextError, TraceLink,
};
pub use crabber_extension::{
    ExtensionError, ExtensionFactory, FactoryContext, FactoryScope, JsonToolExecutor,
    ResumableTool, Scope, ToolDefinition, ToolExecutor, WorkspaceReadError, WorkspaceReadErrorKind,
    WorkspaceReader, WorkspaceReaderResolver,
};
pub use crabber_providers::{FakeProvider, Selection, StreamDelta};
pub use crabber_runtime::{
    CloseObservation, ExecutionTaskDrain, JoinedExecutionTask, JoinedPreparationTask, ModelPurpose,
    MonotonicClock, Observer, OperationKind, OperationalObservation, PermissionDecision,
    PreparationRetryProgress, PreparationTaskId, RecoverReport, RecoverTool, RecoverySkipReason,
    RetainedPreparationProgress, RetainedPreparationStatus, RunControl, RunResult,
    RuntimeCloseReport, RuntimeError, SessionInvocationReport, SessionInvocationStatus, SkippedRun,
    StaticPolicy, SystemMonotonicClock, TaskTermination, TerminalReason, ToolResourceDrain,
    WorkerResourceDrain, WorkerResourceKind,
};

#[cfg(feature = "codex")]
pub use crabber_auth as auth;
pub use crabber_core as core;
pub use crabber_extension as extension;
pub use crabber_providers as providers;
pub use crabber_runtime as runtime;
pub use crabber_session as session;
#[cfg(feature = "wasm")]
pub use crabber_wasm as wasm;

pub use crabber_session::{
    AttemptControlOutcome, AttemptResolverClaim, AttemptRetirement, AttemptRetirementOutcome,
    AttemptUncertainty, AttemptUncertaintyCause, ListSessions, ManageSession, ManagementError,
    ManagementReceipt, PendingCallDisposition, PublicHistoryOutcome, PublicHistoryPage,
    PublicRunOutcome, PublicRunState, PublicRunSummary, SessionChange, SessionDefaults,
    SessionListCursor, SessionListPage, SessionSummary, SnapshotContinuation, SnapshotLimit,
    SnapshotLimits, SnapshotOutcome, SnapshotPage, SnapshotRequest, SnapshotUsage,
};

#[cfg(feature = "datadog")]
pub use crabber_obs as obs;

pub use crabber_session::{
    PublicApprovalBarrier, PublicApprovalSummary, PublicContinuationBarrierV2, PublicControlHeader,
    PublicControlHeaderV2, PublicFrontendRequirement, PublicHeaderOutcome, PublicHeaderOutcomeV2,
    PublicHeaderRequest, PublicHeaderRequestV2, PublicHeaderUsage, PublicRegisteredOwner,
    PublicResyncReason, PublicSessionError, PublicSessionLimit, PublicSessionLimitV2,
    PublicSessionLimits, PublicSessionLimitsV2, PublicSessionOutcome, PublicSessionOutcomeV2,
    PublicSessionPage, PublicSessionPageV2, PublicSessionPoll, PublicSessionPollV2,
    PublicSessionRequest, PublicSessionRequestV2, PublicSessionSubscription,
    PublicSessionSubscriptionV2, PublicToolRequirement, PublicToolSummary,
};

#[cfg(test)]
mod tests {
    #[test]
    fn core_is_available() {
        assert_eq!(crate::core::CRATE_NAME, "crabber-core");
    }
}
