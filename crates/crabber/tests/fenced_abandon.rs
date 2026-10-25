#[path = "../../crabber-session/src/event_persistence.rs"]
mod event_persistence;
// Facade-only abandonment journeys run in the source-embedding harness.
macro_rules! facade_only_test_items {
    ($($item:item)*) => { $($item)* };
}
// The source-embedding adapter retains the canonical read and negative domains.
// Genuine opaque Pending transport remains native-crate-only.
macro_rules! native_pending_initial_items {
    ($($item:item)*) => {};
}
// Generic queue limits run on the canonical MemoryStore and both real SQL
// implementations; this adapter retains its own fenced public histories.
macro_rules! native_store_unit_tests {
    ($($item:item)*) => {};
}
mod pending_initial_test_state {
    #[derive(Clone, Default)]
    pub(crate) struct TestState {}
}
#[path = "../../crabber-session/src/continuation_handoff.rs"]
#[allow(dead_code)]
mod continuation_handoff;
#[path = "../../crabber-session/src/continuation_origin.rs"]
#[allow(dead_code)]
mod continuation_origin;
#[path = "../../crabber-session/src/frontend_contract.rs"]
#[allow(dead_code)]
mod frontend_contract;
#[path = "fenced_support/native_enrollment.rs"]
mod native_enrollment;
#[path = "../../crabber-session/src/native_parent.rs"]
mod native_parent;
#[path = "fenced_support/pending_initial_memory.rs"]
mod pending_initial_memory;
pub use crabber::session::{ForkBoundary, ForkError, ForkOutcome, ForkReceipt, ForkSession};
pub use crabber::session::{QueueHeadsPage, QueueHeadsRequest, WorkspaceRunsRequest};
#[path = "../../crabber-session/src/fork/history.rs"]
#[allow(dead_code)]
pub(crate) mod fork_history;
#[path = "../../crabber-session/src/fork/records.rs"]
#[allow(dead_code)]
mod fork_records;
mod fork {
    pub(crate) use crate::fork_history as history;
    pub(crate) use crate::fork_records::Audit;
}
#[path = "../../crabber-session/src/public_history/redaction.rs"]
#[allow(dead_code)]
mod public_history;
pub use crabber::session::{
    FrontendCallOrigin, FrontendCallStaging, FrontendCallView, FrontendContinuationClaim,
    FrontendContinuationRecovery, FrontendPhaseResume, FrontendResolverClaim,
    FrontendStagingReceipt, FrozenFrontendCall, frontend_descriptor_fingerprint,
};
#[path = "../../crabber-session/src/frontend_continuation/canonical.rs"]
#[allow(dead_code)]
mod frontend_canonical;
#[path = "../../crabber-session/src/frontend_continuation/origin.rs"]
#[allow(dead_code)]
mod frontend_origin;
#[path = "../../crabber-session/src/frontend_continuation/record.rs"]
#[allow(dead_code)]
mod frontend_record;
mod frontend_continuation {
    pub(crate) use crate::frontend_canonical::claim_fingerprint;
    pub(crate) use crate::frontend_origin::{
        FrontendOriginRecord, staging_receipt, verify_assistant_source, verify_declaration,
        verify_receipt as verify_staging_receipt,
    };
    pub(crate) use crate::frontend_record::{FrontendRecoveryRecord, recovery_semantics};
}
type Error = crabber::session::ToolContinuationError;
#[path = "../../crabber-session/src/public_session/rounds.rs"]
#[allow(dead_code)]
pub(crate) mod public_session_rounds;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "fenced_support/run_journal.rs"]
mod run_journal;
pub(crate) mod tests {
    pub(super) fn request(
        session_id: crabber::core::SessionId,
    ) -> crabber::session::PublicSessionRequest {
        crabber::session::PublicSessionRequest {
            session_id,
            continuation: None,
            limits: crabber::session::PublicSessionLimits {
                header_bytes: 65536,
                proof_bytes: 2 * 1024 * 1024,
                interactions: 64,
                history: crabber::session::SnapshotLimits {
                    messages: 64,
                    tool_calls: 64,
                    parts: 1024,
                    text_bytes: 1024 * 1024,
                    encoded_bytes: 2 * 1024 * 1024,
                },
            },
        }
    }
    pub(super) async fn page(
        store: &impl crabber::session::Store,
        request: crabber::session::PublicSessionRequest,
    ) -> crabber::session::PublicSessionPage {
        let crabber::session::PublicSessionOutcome::Page(page) =
            store.public_session_snapshot(request).await.unwrap()
        else {
            panic!("public page")
        };
        *page
    }
}
#[path = "../../crabber-session/src/continuation_input.rs"]
#[allow(dead_code)]
mod continuation_input;
#[path = "fenced_support/preparation.rs"]
mod preparation;
#[path = "../../crabber-session/src/session_owner/preparation_contract.rs"]
#[allow(dead_code)]
mod preparation_contract;
#[path = "../../crabber-session/src/session_owner/audit.rs"]
#[allow(dead_code)]
mod session_owner_audit;
#[path = "../../crabber-session/src/session_owner/coverage.rs"]
#[allow(dead_code)]
mod session_owner_coverage;
#[path = "fenced_support/invocation_parent.rs"]
#[allow(dead_code)]
mod session_owner_invocation_parent;
#[path = "../../crabber-session/src/session_owner/ledger.rs"]
#[allow(dead_code)]
mod session_owner_ledger;
#[path = "fenced_support/invocation_public.rs"]
#[allow(dead_code)]
mod session_owner_public_invocation;
#[path = "../../crabber-session/src/session_owner/record.rs"]
#[allow(dead_code)]
mod session_owner_record;
#[path = "../../crabber-session/src/session_owner/validation.rs"]
#[allow(dead_code)]
mod session_owner_validation;

#[path = "fenced_support/cancellation.rs"]
#[allow(dead_code)]
mod session_owner_cancellation;

#[path = "../../crabber-session/src/session_owner/cancellation_contract.rs"]
#[allow(dead_code)]
mod session_owner_cancellation_contract;

#[path = "fenced_support/driver.rs"]
mod driver;
#[path = "fenced_support/owned_scope.rs"]
mod owned_scope;
mod session_owner {
    pub(crate) use crate::driver;
    pub(crate) use crate::owned_scope;
    pub(crate) use crate::preparation;
    pub(crate) use crate::preparation_contract;
    pub(crate) use crate::session_owner_audit as audit;
    pub(crate) use crate::session_owner_cancellation as cancellation;
    pub(crate) use crate::session_owner_cancellation_contract as cancellation_contract;
    pub(crate) use crate::session_owner_coverage as coverage;
    pub(crate) use crate::session_owner_invocation_parent as invocation_parent;
    pub(crate) use crate::session_owner_ledger::Ledger;
    pub(crate) use crate::session_owner_public_invocation as public_invocation;
    pub(crate) use crate::session_owner_record as record;
    pub(crate) use crate::session_owner_validation as validation;
    pub(crate) use crate::session_owner_validation::{digest, identifier, resolver};
    pub use crabber::session::session_owner::*;
}
#[path = "../../crabber-session/src/public_session/v2/projection.rs"]
#[allow(dead_code)]
pub(crate) mod aggregate_projection;
#[path = "../../crabber-session/src/public_session/v2/proof.rs"]
#[allow(dead_code)]
pub(crate) mod aggregate_proof;
#[path = "../../crabber-session/src/public_session/v2/tests.rs"]
#[allow(dead_code)]
pub(crate) mod aggregate_tests;
#[path = "../../crabber-session/src/public_session/allocation.rs"]
#[allow(dead_code)]
pub(crate) mod public_session_allocation;
#[path = "../../crabber-session/src/public_session/budget.rs"]
#[allow(dead_code)]
pub(crate) mod public_session_budget;
#[path = "../../crabber-session/src/public_session/races.rs"]
#[allow(dead_code, unused_imports)]
pub(crate) mod public_session_races;
#[path = "../../crabber-session/src/session_owner_contract.rs"]
#[allow(dead_code)]
pub mod session_owner_contract;
#[path = "../../crabber-session/src/snapshot_profile.rs"]
#[allow(dead_code)]
mod snapshot_profile;
mod public_session {
    pub(crate) use crate::public_session_races as races;
    pub(crate) mod v2 {
        pub(crate) use crate::aggregate_projection as projection;
        pub(crate) use crate::aggregate_proof as proof;
        pub(crate) use crate::aggregate_tests as tests;
    }
    pub(crate) use crate::public_session_allocation as allocation;
    pub(crate) use crate::public_session_budget as budget;
    pub(crate) use crate::public_session_rounds as rounds;
}
pub use crabber::session::{
    PublicApprovalBarrier, PublicApprovalSummary, PublicControlHeader, PublicHeaderOutcome,
    PublicHeaderRequest, PublicHeaderUsage, PublicHistoryOutcome, PublicSessionError,
    PublicSessionLimit, PublicSessionLimits, PublicSessionOutcome, PublicSessionRequest,
};
pub use crabber::session::{
    PublicContinuationBarrierV2, PublicControlHeaderV2, PublicFrontendRequirement,
    PublicHeaderOutcomeV2, PublicHeaderRequestV2, PublicRegisteredOwner, PublicResyncReason,
    PublicSessionLimitV2, PublicSessionLimitsV2, PublicSessionOutcomeV2, PublicSessionPollV2,
    PublicSessionRequestV2, PublicSessionSubscriptionV2, PublicToolRequirement, PublicToolSummary,
};
pub use crabber::session::{
    RunControlAction, RunControlBinding, RunControlBoundary, RunControlError, RunControlKind,
    RunControlOutcome, RunControlPage, RunControlReceipt, RunControlRequest, RunControlStatus,
    RunControlSummary, RunControlTarget, WaitingCancellation,
};
#[path = "../../crabber-session/src/run_control/records.rs"]
#[allow(dead_code)]
mod run_control_records;
#[path = "../../crabber-session/src/run_control/suspension.rs"]
#[allow(dead_code)]
mod run_control_suspension;
#[path = "../../crabber-session/src/run_control/terminal.rs"]
pub(crate) mod run_control_terminal;
#[allow(unused_imports)]
mod run_control {
    pub(crate) use crate::run_control_records::*;
    pub(crate) use crate::run_control_suspension::*;
    pub(crate) use crate::run_control_terminal as terminal;
    pub(crate) use crabber::session::{
        RunControlAction, RunControlBoundary, RunControlError, RunControlKind, RunControlOutcome,
        RunControlPage, RunControlReceipt, RunControlRequest, RunControlStatus, RunControlSummary,
        RunControlTarget,
    };
}
#[path = "../../crabber-session/src/public_run.rs"]
#[allow(dead_code)]
mod public_run;
#[path = "../../crabber-session/src/control_contract.rs"]
#[allow(dead_code)]
mod run_control_contract;

pub use crabber::session::{
    QueueAction, QueueAuthority, QueueInput, QueueMutation, QueueOutcome, QueuePage,
    QueuePageRequest, QueueReceipt, QueueState, QueueSummary, RetainedQueue,
};
#[path = "../../crabber-session/src/queue/projection.rs"]
mod queue_projection;
#[path = "../../crabber-session/src/queue/records.rs"]
#[allow(dead_code)]
mod queue_records;
#[path = "../../crabber-session/src/queue/source.rs"]
#[allow(dead_code)]
pub(crate) mod queue_source;
#[path = "../../crabber-session/src/queue/transition.rs"]
#[allow(dead_code)]
mod queue_transition;
mod queue {
    pub(crate) use crate::queue_projection::former_encoded_len;
    pub(crate) use crate::queue_records::*;
    pub(crate) use crate::queue_source as source;
    pub(crate) use crate::queue_transition::*;
    pub(crate) use crabber::core::{ControlId, QueueId, SessionId, SessionRevision};
    pub(crate) use crabber::session::{
        BoundKeyedAdmitRequest, QueueAction, QueueAuthority, QueueInput, QueueMutation,
        QueueOutcome, QueuePage, QueuePageRequest, QueueReceipt, QueueState, QueueSummary,
        RetainedQueue, StoreError,
    };
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) fn invalid() -> StoreError {
        StoreError::Validation("invalid future admission queue request".into())
    }
    pub(crate) fn corrupt() -> StoreError {
        StoreError::Validation("invalid retained future admission queue evidence".into())
    }
}
#[path = "../../crabber-session/src/queue_contract.rs"]
#[allow(dead_code)]
mod queue_contract;
pub use crabber::session::{AdmissionSelectionPolicy, MAX_RUN_BOUNDARY_BYTES};
pub use crabber::session::{PublicRunOutcome, PublicRunState, PublicRunSummary};

#[path = "../../crabber-session/src/tool_continuation/audit.rs"]
#[allow(dead_code)]
mod tool_continuation_audit;
#[path = "../../crabber-session/src/tool_continuation/current.rs"]
#[allow(dead_code)]
mod tool_continuation_current;
pub use crabber::session::{
    FrozenToolPhase, RequiredToolInteraction, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationError, ToolContinuationGrant, ToolContinuationOutcome, ToolContinuationReceipt,
    ToolContinuationRecovery, ToolContinuationSnapshot, ToolContinuationState,
    ToolContinuationSuspension, ToolInteractionAnswer, ToolInteractionReply, ToolInteractionView,
    ToolPhaseResume, ToolSuspensionAuthority,
};
#[path = "../../crabber-session/src/tool_continuation/canonical.rs"]
#[allow(dead_code)]
mod tool_continuation_canonical;
#[path = "../../crabber-session/src/tool_continuation/records.rs"]
#[allow(dead_code)]
mod tool_continuation_records;
#[path = "../../crabber-session/src/tool_continuation/termination.rs"]
#[allow(dead_code)]
mod tool_continuation_termination;
#[path = "fenced_support/grant_ack.rs"]
mod tool_grant_ack;
mod tool_continuation {
    pub(crate) use crate::tool_continuation_audit::*;
    pub(crate) use crate::tool_continuation_current::*;
    pub(crate) use crate::tool_continuation_records::*;
    pub(crate) use crate::tool_continuation_termination::*;
    pub(crate) use crate::tool_grant_ack as ack;

    pub(crate) use crabber::session::{
        FrozenToolPhase, ToolContinuationClaim, ToolContinuationError, ToolContinuationOutcome,
        ToolContinuationReceipt, ToolContinuationSnapshot, ToolContinuationState,
        ToolContinuationSuspension, ToolInteractionAnswer, ToolInteractionReply,
        ToolInteractionView,
    };

    pub(crate) mod canonical {
        pub(crate) use crate::tool_continuation_canonical::*;
    }
}
#[path = "../../crabber-session/src/attempt_control/turn.rs"]
#[allow(dead_code)]
mod attempt_control_turn;
#[path = "../../crabber-session/src/interaction_contract.rs"]
#[allow(dead_code)]
mod tool_continuation_contract;
pub use crabber::session::{
    AttemptControlOutcome, AttemptResolverClaim, AttemptRetirement, AttemptRetirementOutcome,
    AttemptUncertainty, AttemptUncertaintyCause, PendingCallDisposition,
};
#[path = "../../crabber-session/src/attempt_control/readiness.rs"]
#[allow(dead_code)]
mod attempt_control_readiness;
#[path = "../../crabber-session/src/attempt_control/records.rs"]
#[allow(dead_code)]
mod attempt_control_records;
#[path = "../../crabber-session/src/attempt_control/terminal.rs"]
#[allow(dead_code)]
pub(crate) mod attempt_control_terminal;
mod attempt_control {
    pub(crate) use crate::attempt_control_readiness::*;
    pub(crate) use crate::attempt_control_records::*;
    pub(crate) use crate::attempt_control_terminal as terminal;
    pub(crate) use crate::attempt_control_turn::*;
}
#[path = "../../crabber-session/src/attempt_control_contract.rs"]
#[allow(dead_code)]
mod attempt_control_contract;
pub use crabber::session::{
    ApprovalAnswer, ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationRecovery,
    ApprovalContinuationState, ApprovalError, ApprovalGrant, ApprovalInteraction, ApprovalOutcome,
    ApprovalReceipt, ApprovalReply, ApprovalResolverClaim, ApprovalSnapshot, ApprovalSuspension,
    ApprovalTermination, ApprovalTerminationKind, CallApproval, ContinuationTarget,
    GrantedApproval, GrantedDecision, ListSessions, MAX_APPROVAL_CALLS, ManageSession,
    ManagementError, ManagementReceipt, PreparedApproval, SessionChange, SessionDefaults,
    SessionListCursor, SessionListPage, SessionSummary,
};
pub use crabber::session::{
    DeleteSession, DeletionBlocker, DeletionError, DeletionOutcome, DeletionReceipt,
    SessionDisposition,
};
#[path = "../../crabber-session/src/deletion/contract.rs"]
#[allow(dead_code)]
pub(crate) mod deletion_contract;
#[path = "../../crabber-session/src/deletion/coordinator.rs"]
mod deletion_coordinator;
#[path = "../../crabber-session/src/deletion/coordinator_contract.rs"]
mod deletion_coordinator_contract;
#[path = "../../crabber-session/src/deletion/coverage.rs"]
#[allow(dead_code)]
pub(crate) mod deletion_coverage;
#[path = "../../crabber-session/src/deletion/history.rs"]
pub(crate) mod deletion_history;
#[path = "../../crabber-session/src/deletion/origin.rs"]
#[allow(dead_code)]
pub(crate) mod deletion_origin;
#[path = "../../crabber-session/src/deletion/records.rs"]
#[allow(dead_code)]
mod deletion_records;
#[path = "../../crabber-session/src/deletion/validation.rs"]
#[allow(dead_code)]
mod deletion_validation;
#[path = "../../crabber-session/src/fork_pending_contract.rs"]
#[allow(dead_code)]
mod fork_pending_contract;
#[path = "../../crabber-session/src/sql_fork_contract.rs"]
#[allow(dead_code)]
mod sql_fork_contract;
mod deletion {
    pub(crate) use crate::deletion_contract as contract;
    pub(crate) use crate::deletion_coordinator as coordinator;
    pub(crate) use crate::deletion_coordinator_contract as coordinator_contract;
    pub(crate) use crate::deletion_coverage as coverage;
    pub(crate) use crate::deletion_history as history;
    pub(crate) use crate::deletion_origin as origin;
    pub(crate) use crate::deletion_records::Tombstone;
    pub(crate) use crate::deletion_validation::{
        fork_error, management_error, owner_error, validate_session,
    };
}
#[path = "../../crabber-session/src/management/records.rs"]
#[allow(dead_code)]
mod management;
#[path = "../../crabber-session/src/session_management_contract.rs"]
#[allow(dead_code)]
mod management_contract;
pub use crabber::session::{
    PhaseCleanup, PhaseControlOutcome, PhasePage, PhasePageRequest, PhaseUncertainty,
    PhaseUncertaintyCause,
};
// Compile private Memory fixtures into this test target; no product fault API.
pub use crabber::session::{
    AdmissionExecutionCapsule, AdmissionExecutionError, AdmissionExecutionRecord,
    AdmissionExecutionState, AdmissionRequestData, AdmitOutcome, AttemptAuthority, AttemptCleanup,
    AttemptCleanupResult, AttemptCommand, AttemptConclusion, AttemptError, AttemptFinish,
    AttemptMutation, AttemptMutationOutcome, AttemptReceipt, AttemptSnapshot, AttemptState,
    AttemptTarget, BoundAdmitRequest, BoundKeyedAdmitRequest, ClaimUnstartedAdmissionRequest,
    ClaimedAdmission, ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT, PROVIDER_BINDING_COMPONENT,
    PhaseAuthority, PhaseCommand, PhaseError, PhaseMutation, PhaseMutationOutcome, PhaseOperation,
    PhaseQuiescence, PhaseQuiescenceResult, PhaseReceipt, PhaseResolution, PhaseResolutionOutcome,
    PhaseResolverClaim, PhaseSettlement, PhaseSettlementRecords, PhaseSnapshot, PhaseStart,
    PhaseState, PhaseTarget, PhaseWriteAuthority, PublicSourceAuthority, PublicSourceMutation,
    PublicSourcePublication, PublicStateError, PublicStateMutation, PublicStateReceipt,
    PublicStateSnapshot, RetainedExecutionBinding, RunBoundary, SnapshotContinuation,
    SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotPage, SnapshotRequest, SnapshotUsage,
    StoreError, UncertainPhaseChoice, VersionedAttempt, VersionedPhase, admission_config_hash,
    ensure_storable, ensure_storable_text,
};
#[path = "../../crabber-session/src/abandonment.rs"]
#[allow(dead_code)]
mod abandonment;
#[path = "../../crabber-session/src/abandonment_contract.rs"]
mod abandonment_contract;
#[path = "../../crabber-session/src/admission_execution_contract.rs"]
#[allow(dead_code)]
mod admission_execution_contract;
#[path = "../../crabber-session/src/admission_journal.rs"]
#[allow(dead_code)]
mod admission_journal;
#[path = "../../crabber-session/src/approval/audit.rs"]
#[allow(dead_code)]
mod approval_audit;
#[path = "../../crabber-session/src/approval/conclusion.rs"]
mod approval_conclusion;
#[path = "../../crabber-session/src/approval/current.rs"]
#[allow(dead_code)]
pub(crate) mod approval_current;
#[path = "../../crabber-session/src/approval/fingerprint.rs"]
mod approval_fingerprint;
#[path = "../../crabber-session/src/approval/origin.rs"]
#[allow(dead_code)]
mod approval_origin;
#[path = "../../crabber-session/src/approval/original.rs"]
pub(crate) mod approval_original;
#[path = "../../crabber-session/src/approval/records.rs"]
#[allow(dead_code)]
mod approval_records;
#[path = "../../crabber-session/src/approval/recovery_provenance.rs"]
#[allow(dead_code)]
mod approval_recovery_provenance;
#[path = "../../crabber-session/src/approval/terminal.rs"]
#[allow(dead_code)]
pub(crate) mod approval_terminal;
#[path = "../../crabber-session/src/approval/terminal_history.rs"]
#[allow(dead_code)]
pub(crate) mod approval_terminal_history;
mod approval {
    pub(crate) use crate::approval_audit::{event, verify_receipt};
    pub(crate) use crate::approval_conclusion::at as conclusion_at;
    pub(crate) use crate::approval_current as current;
    pub(crate) use crate::approval_fingerprint::fingerprint as control_fingerprint;
    pub(crate) use crate::approval_original as original;
    pub(crate) use crate::approval_records::*;
    pub(crate) use crate::approval_recovery_provenance::{
        activate_recovery, verify_recovery_answer,
    };
    pub(crate) use crate::approval_terminal as terminal;
    pub(crate) use crate::approval_terminal_history as terminal_history;
}
#[path = "../../crabber-session/src/approval_contract.rs"]
#[allow(dead_code)]
mod approval_contract;
#[path = "../../crabber-session/src/attempt/records.rs"]
#[allow(dead_code)]
mod attempt;
#[path = "../../crabber-session/src/attempt_conclusion/audit.rs"]
#[allow(dead_code)]
mod attempt_conclusion_audit;
#[path = "../../crabber-session/src/attempt_conclusion/materialize.rs"]
mod attempt_conclusion_materialize;
#[path = "../../crabber-session/src/attempt_conclusion/terminal.rs"]
#[allow(dead_code)]
pub(crate) mod attempt_conclusion_terminal;
mod attempt_conclusion {
    pub(crate) use crate::attempt_conclusion_audit::*;
    pub(crate) use crate::attempt_conclusion_materialize::scoped as materialize_scope;
    pub(crate) use crate::attempt_conclusion_terminal as terminal;
}

#[path = "../../crabber-session/src/attempt_contract.rs"]
#[allow(dead_code)]
mod attempt_contract;
#[path = "../../crabber-session/src/binding_fault.rs"]
mod binding_fault;
#[path = "../../crabber-session/src/context_projection.rs"]
mod context_projection;
#[path = "../../crabber-session/src/execution_binding_contract/fixture.rs"]
#[allow(dead_code)]
mod execution_binding_contract;
#[path = "../../crabber-session/src/memory.rs"]
#[allow(dead_code)]
mod memory;
#[path = "../../crabber-session/src/phase/records.rs"]
#[allow(dead_code)]
mod phase_records;
#[path = "../../crabber-session/src/phase/terminal_history.rs"]
#[allow(dead_code)]
pub(crate) mod phase_terminal_history;
mod phase {
    pub(crate) use crate::phase_records::*;
    pub(crate) use crate::phase_terminal_history as terminal_history;
}
#[path = "../../crabber-session/src/phase_contract.rs"]
#[allow(dead_code)]
mod phase_contract;
#[path = "../../crabber-session/src/public_state/records.rs"]
#[allow(dead_code)]
mod public_state_records;
mod public_state {
    pub(crate) use crate::public_source_fixture::{before_event, verify_before_event};
    pub(crate) use crate::public_state_records::*;
}
#[path = "fenced_support/public_source.rs"]
mod public_source_fixture;
pub(crate) use public_source_fixture::verify_source_event;
#[path = "../../crabber-session/src/public_state_contract.rs"]
#[allow(dead_code)]
mod public_state_contract;
#[path = "../../crabber-session/src/snapshot.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "../../crabber-session/src/typed_admission_contract/fixture.rs"]
#[allow(dead_code)]
mod typed_admission_contract;
use async_trait::async_trait;
use crabber::core::*;
use crabber::extension::{Extension, ExtensionError, Registrar, Scope};
use crabber::providers::{ProviderError, Resolver, Selection, Streamer};
use crabber::session::{
    AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest, Store,
};
use crabber::{AbandonAuthority, AbandonError, AbandonRequest, Agent, AgentConfig};
use memory::MemoryStore;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Unavailable(Arc<AtomicUsize>);
#[async_trait]
impl Resolver for Unavailable {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandon must not resolve a provider")
    }
}
struct Unmountable(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Unmountable {
    fn id(&self) -> &'static str {
        "unavailable"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "missing".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandon must not mount extensions or hooks")
    }
}

struct NeverTool(Arc<AtomicUsize>);
#[async_trait]
impl crabber::JsonToolExecutor for NeverTool {
    async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandon must not execute tools")
    }
}

fn message(session: &SessionId) -> Message {
    Message {
        id: MessageId::new(),
        session_id: session.clone(),
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: vec![],
        created_at: SystemClock.now(),
    }
}
fn event(run: &Run, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: run.created_at,
    }
}
fn admit_request(session: &SessionId) -> AdmitRequest {
    AdmitRequest {
        input_envelope: None,
        additional_messages: Vec::new(),
        expected_revision: None,
        session_id: Some(session.clone()),
        workspace_id: "workspace".into(),
        directory: ".".into(),
        title: "abandon".into(),
        user_message: message(session),
        config_hash: "unavailable-config".into(),
        plan_fingerprint: "unavailable-plan".into(),
        owner: "dead-worker".into(),
        lease: Duration::from_secs(30),
    }
}
fn agent(store: Arc<dyn Store>, counters: Arc<AtomicUsize>) -> Agent {
    Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .provider(Arc::new(Unavailable(counters.clone())))
        .shared_tool(Arc::new(crabber::ToolDefinition {
            info: ToolInfo {
                name: "registered-effect".into(),
                description: "never run".into(),
                parameters: serde_json::json!({}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(NeverTool(counters.clone())),
        }))
        .shared_extension(Arc::new(Unmountable(counters)), Scope::Global)
        .config(AgentConfig::new(Selection {
            provider_id: "missing".into(),
            model_id: "missing".into(),
        }))
        .build()
        .unwrap()
}

#[tokio::test]
async fn public_memory_abandon_running_and_paused_without_execution() {
    let clock = Arc::new(ManualClock::new(SystemClock.now()));
    facade_matrix(
        Arc::new(MemoryStore::with_clock(clock.clone())),
        clock,
        None,
    )
    .await;
}

#[allow(clippy::too_many_lines)]
async fn facade_matrix<S: abandonment_contract::FixtureStore + 'static>(
    store: Arc<S>,
    clock: Arc<ManualClock>,
    fixture_url: Option<&str>,
) {
    for authority in [
        AbandonAuthority::ExpiredLease,
        AbandonAuthority::HostStoppedOwner,
    ] {
        for status in [RunStatus::Pending, RunStatus::Running, RunStatus::Paused] {
            let paused = status == RunStatus::Paused;
            let now = SystemClock.now();
            clock.set(now);
            let session = SessionId::new();
            let key = AdmissionKey::new("original").unwrap();
            let mut worker = OwnerProcess::spawn();
            let mut request = admit_request(&session);
            request.owner = format!("process:{}", worker.0.id());
            let options = AdmissionOptions {
                key: key.clone(),
                fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
                behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
            };
            let KeyedAdmitOutcome::Started { receipt, admitted } = store
                .admit_keyed_run(KeyedAdmitRequest {
                    execution: None,
                    request: request.clone(),
                    options,
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(admitted.run.status, RunStatus::Running);
            let execution = store.execution(admitted.fence.clone()).await.unwrap();
            let mut calls = vec![];
            for index in 0..4 {
                let call = ToolCallRecord {
                    id: ToolCallId::new(),
                    run_id: admitted.run.id.clone(),
                    name: "unregistered-effect".into(),
                    arguments: serde_json::json!({"index": index}),
                    status: ToolCallStatus::Pending,
                    retry_safe: false,
                    result: None,
                };
                execution
                    .create_tool_call(
                        call.clone(),
                        event(&admitted.run, EventKind::ToolCallPending),
                    )
                    .await
                    .unwrap();
                if index > 0 {
                    execution
                        .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                        .await
                        .unwrap();
                }
                calls.push(call);
            }
            for done in &calls[2..] {
                let mut result_message = message(&session);
                result_message.role = Role::Tool;
                result_message.run_id = Some(admitted.run.id.clone());
                result_message.parts.push(Part {
                    id: PartId::new(),
                    message_id: result_message.id.clone(),
                    ordinal: 0,
                    kind: PartKind::FunctionToolResult,
                    content: ContentBlock::ToolResult {
                        call_id: done.id.clone(),
                        content: vec![],
                        is_error: false,
                    },
                });
                execution
                    .settle_tool_call(
                        &done.id,
                        ToolResult {
                            status: if done.id == calls[2].id {
                                ToolResultStatus::Completed
                            } else {
                                ToolResultStatus::Failed
                            },
                            content: vec![],
                        },
                        result_message.clone(),
                        event(&admitted.run, EventKind::ToolCallSettled),
                    )
                    .await
                    .unwrap();
            }
            if paused {
                execution
                    .pause_run(
                        serde_json::json!({"unavailable_continuation": true}),
                        event(&admitted.run, EventKind::RunPaused),
                    )
                    .await
                    .unwrap();
            }
            let mut seeded = store.get_run(&admitted.run.id).await.unwrap().unwrap();
            seeded.status = status;
            seeded.lease_until = admitted.run.lease_until;
            seeded.usage = Usage {
                input_tokens: 123,
                output_tokens: 45,
            };
            abandonment_contract::FixtureStore::seed_run(
                store.as_ref(),
                seeded.clone(),
                fixture_url,
            )
            .await;
            let original_calls = abandonment_contract::FixtureStore::calls(
                store.as_ref(),
                &admitted.run.id,
                fixture_url,
            )
            .await;
            if paused {
                assert_eq!(
                    store
                        .get_run(&admitted.run.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    RunStatus::Paused
                );
            }
            let counters = Arc::new(AtomicUsize::new(0));
            let agent = agent(store.clone(), counters.clone());
            let abandon = AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner.clone(),
                authority,
            };
            {
                assert_eq!(
                    agent
                        .abandon(AbandonRequest {
                            authority: AbandonAuthority::ExpiredLease,
                            ..abandon.clone()
                        })
                        .await
                        .unwrap_err(),
                    AbandonError::LiveLease
                );
            }
            worker.stop();
            if authority == AbandonAuthority::ExpiredLease {
                clock.set(now + time::Duration::seconds(30));
            }
            for kind in [InboxKind::Steer, InboxKind::FollowUp] {
                store
                    .enqueue_inbox(&session, kind, message(&session))
                    .await
                    .unwrap();
            }
            let before = store.list_all_messages(&session).await.unwrap();
            let outcome = agent.abandon(abandon.clone()).await.unwrap();
            assert_eq!(outcome.run.status, RunStatus::Interrupted);
            assert_eq!(outcome.run.usage, seeded.usage);
            assert_eq!(outcome.run.checkpoint, seeded.checkpoint);
            let settled_calls = abandonment_contract::FixtureStore::calls(
                store.as_ref(),
                &admitted.run.id,
                fixture_url,
            )
            .await;
            for prior in &original_calls {
                if matches!(
                    prior.status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                ) {
                    assert!(settled_calls.contains(prior));
                }
            }
            assert_ne!(outcome.run.claim_token, admitted.fence.claim_token);
            assert_eq!(outcome.interrupted_tools.len(), 2);
            assert_eq!(
                store
                    .list_unfinished_tool_calls(&admitted.run.id)
                    .await
                    .unwrap(),
                [] as [crabber_core::ToolCallRecord; 0]
            );
            assert_eq!(
                store.lookup_admission(&session, &key).await.unwrap(),
                Some(receipt)
            );
            let messages = store.list_all_messages(&session).await.unwrap();
            assert_eq!(&messages[..before.len()], &before);
            for preserved in &before {
                assert!(messages.contains(preserved));
            }
            for call in &calls[..2] {
                assert_eq!(
                    messages
                        .iter()
                        .flat_map(|m| &m.parts)
                        .filter(|p| matches!(&p.content,
                ContentBlock::ToolResult { call_id, is_error: true, .. } if call_id == &call.id))
                        .count(),
                    1
                );
            }
            let events = store.list_events(&session, None, 100).await.unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.kind == EventKind::RunSettled)
                    .count(),
                1
            );
            assert!(outcome.terminal_event.cursor.is_some());
            assert_eq!(agent.abandon(abandon).await.unwrap(), outcome);
            assert_eq!(
                store.list_events(&session, None, 100).await.unwrap(),
                events
            );
            assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
            assert_eq!(
                execution
                    .renew_lease(now + time::Duration::hours(1))
                    .await
                    .unwrap_err(),
                CoreError::Conflict
            );
            assert_eq!(
                execution
                    .append_event(event(
                        &admitted.run,
                        EventKind::Custom {
                            name: "stale".into()
                        }
                    ))
                    .await
                    .unwrap_err(),
                CoreError::Conflict
            );
            assert_eq!(
                execution
                    .settle_run(
                        RunStatus::Completed,
                        None,
                        Usage::default(),
                        event(&admitted.run, EventKind::RunSettled)
                    )
                    .await
                    .unwrap_err(),
                CoreError::Conflict
            );
            assert_all_owner_writes_revoked(execution.as_ref(), &admitted.run).await;
            for alteration in 0..3 {
                let mut changed = AbandonRequest {
                    expected: admitted.fence.clone(),
                    expected_owner: admitted.run.owner.clone(),
                    authority,
                };
                match alteration {
                    0 => changed.expected_owner = "wrong".into(),
                    1 => changed.expected.claim_token = "wrong".into(),
                    _ => {
                        changed.authority = if authority == AbandonAuthority::ExpiredLease {
                            AbandonAuthority::HostStoppedOwner
                        } else {
                            AbandonAuthority::ExpiredLease
                        }
                    }
                }
                assert_eq!(
                    agent.abandon(changed).await.unwrap_err(),
                    AbandonError::StaleOwner
                );
            }
            assert_eq!(
                abandonment_contract::FixtureStore::unconsumed_inbox(
                    store.as_ref(),
                    &session,
                    fixture_url
                )
                .await,
                2
            );
            let next = store
                .admit_run(AdmitRequest {
                    user_message: message(&session),
                    ..request
                })
                .await
                .unwrap();
            let next_execution = store.execution(next.fence).await.unwrap();
            assert_eq!(
                next_execution
                    .claim_inbox(InboxKind::Steer)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                next_execution
                    .claim_inbox_into_history(InboxKind::FollowUp)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(counters.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn public_host_stopped_requires_exact_owner_and_fence() {
    stopped_owner(Arc::new(MemoryStore::new())).await;
}
async fn stopped_owner(store: Arc<dyn Store>) {
    let mut worker = OwnerProcess::spawn();
    let admitted = store
        .admit_run(AdmitRequest {
            input_envelope: None,
            additional_messages: Vec::new(),
            expected_revision: None,
            session_id: None,
            owner: format!("process:{}", worker.0.id()),
            ..admit_request(&SessionId::new())
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let agent = agent(store.clone(), Arc::new(AtomicUsize::new(0)));
    assert_eq!(
        agent
            .abandon(AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner.clone(),
                authority: AbandonAuthority::ExpiredLease
            })
            .await
            .unwrap_err(),
        AbandonError::LiveLease
    );
    let mut request = AbandonRequest {
        expected: admitted.fence.clone(),
        expected_owner: "wrong-owner".into(),
        authority: AbandonAuthority::HostStoppedOwner,
    };
    assert_eq!(
        agent.abandon(request.clone()).await.unwrap_err(),
        AbandonError::StaleOwner
    );
    request.expected_owner.clone_from(&admitted.run.owner);
    request.expected.claim_token = "wrong-token".into();
    assert_eq!(
        agent.abandon(request.clone()).await.unwrap_err(),
        AbandonError::StaleOwner
    );
    request.expected = admitted.fence.clone();
    // This child represents the authoritative worker process. A host must stop
    // actual execution and renewal, and wait for confirmed exit.
    worker.stop();
    assert_eq!(
        agent.abandon(request).await.unwrap().run.status,
        RunStatus::Interrupted
    );
    assert_all_owner_writes_revoked(execution.as_ref(), &admitted.run).await;
}

#[allow(clippy::too_many_lines)]
async fn assert_all_owner_writes_revoked(execution: &dyn ExecutionStore, run: &Run) {
    let mut msg = message(&run.session_id);
    msg.run_id = Some(run.id.clone());
    let part = Part {
        id: PartId::new(),
        message_id: msg.id.clone(),
        ordinal: 0,
        kind: PartKind::AssistantText,
        content: ContentBlock::Text {
            text: "stale".into(),
        },
    };
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: run.id.clone(),
        name: "stale".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    let epoch = ContextEpoch {
        id: EpochId::new(),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        parent: Some(run.epoch_id.clone()),
        summarized_range: None,
        summary_message_id: None,
        tail_start_message_id: None,
        provider_id: "missing".into(),
        model_id: "missing".into(),
        reason: "stale".into(),
        next_policy: None,
    };
    let writes = [
        execution
            .renew_lease(SystemClock.now() + time::Duration::hours(1))
            .await,
        execution.append_message(msg.clone()).await,
        execution.append_part(part).await,
        execution
            .append_event(event(run, EventKind::MessageCommitted))
            .await,
        execution
            .create_tool_call(call.clone(), event(run, EventKind::ToolCallPending))
            .await,
        execution
            .claim_tool_call(&call.id, event(run, EventKind::ToolCallRunning))
            .await,
        execution
            .settle_tool_call(
                &call.id,
                ToolResult {
                    status: ToolResultStatus::Interrupted,
                    content: vec![],
                },
                msg.clone(),
                event(run, EventKind::ToolCallSettled),
            )
            .await,
        execution.start_epoch(epoch).await,
        execution.finish_epoch(&run.epoch_id, msg).await,
        execution
            .pause_run(serde_json::json!({}), event(run, EventKind::RunPaused))
            .await,
        execution
            .settle_run(
                RunStatus::Completed,
                None,
                Usage::default(),
                event(run, EventKind::RunSettled),
            )
            .await,
        execution
            .put_extension_state("stale", vec![("key".into(), Some("value".into()))])
            .await,
        execution.claim_inbox(InboxKind::Steer).await.map(|_| ()),
        execution
            .claim_inbox_into_history(InboxKind::FollowUp)
            .await
            .map(|_| ()),
    ];
    for write in writes {
        assert_eq!(write.unwrap_err(), CoreError::Conflict);
    }
}

#[cfg(feature = "postgres")]
#[path = "fenced_support/postgres.rs"]
mod postgres;

#[tokio::test]
async fn facade_memory_missing_and_unrelated_terminal_are_honest() {
    error_semantics(Arc::new(MemoryStore::new())).await;
}
async fn error_semantics(store: Arc<dyn Store>) {
    let counters = Arc::new(AtomicUsize::new(0));
    let host = agent(store.clone(), counters.clone());
    let missing = AbandonRequest {
        expected: RunFence {
            run_id: RunId::new(),
            claim_token: "missing".into(),
        },
        expected_owner: "missing".into(),
        authority: AbandonAuthority::ExpiredLease,
    };
    assert_eq!(
        host.abandon(missing).await.unwrap_err(),
        AbandonError::NotFound
    );
    let admitted = store
        .admit_run(AdmitRequest {
            input_envelope: None,
            additional_messages: Vec::new(),
            expected_revision: None,
            session_id: None,
            ..admit_request(&SessionId::new())
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted.run, EventKind::RunSettled),
        )
        .await
        .unwrap();
    let before = store
        .list_events(&admitted.run.session_id, None, 100)
        .await
        .unwrap();
    let request = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::HostStoppedOwner,
    };
    assert_eq!(
        host.abandon(request).await.unwrap_err(),
        AbandonError::AlreadyTerminal
    );
    assert_eq!(
        store
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap(),
        before
    );
    assert_eq!(counters.load(Ordering::SeqCst), 0);
}
#[path = "fenced_support/response_loss.rs"]
mod response_loss;

struct OwnerProcess(std::process::Child);
impl OwnerProcess {
    fn spawn() -> Self {
        Self(
            std::process::Command::new("sleep")
                .arg("60")
                .spawn()
                .unwrap(),
        )
    }
    fn stop(&mut self) {
        assert!(self.0.try_wait().unwrap().is_none());
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
        assert!(self.0.try_wait().unwrap().is_some());
    }
}
impl Drop for OwnerProcess {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[cfg(feature = "sqlite")]
#[path = "fenced_support/sqlite.rs"]
mod sqlite;

#[path = "server_host/tool_continuation/handoff/journey.rs"]
mod handoff_journey;
#[tokio::test]
async fn memory_cross_kind_handoff_private_source_corruption_and_budget() {
    let store = Arc::new(MemoryStore::new());
    let (session, run) = handoff_journey::journey(store.clone()).await;
    store.test_handoff_corruption(&session, &run);
}

#[tokio::test]
async fn memory_mixed_cancel_rejects_coherent_private_output_replacement() {
    let store = Arc::new(MemoryStore::new());
    handoff_journey::cancel_handoffs(store.clone()).await;
    handoff_journey::cancel_with_probe(store.clone(), |request| async move {
        store.test_terminal_history_corruption(&request);
    })
    .await;
}

#[tokio::test]
async fn memory_waiting_cancel_rejects_coherent_ordinary_terminal_output_replacement() {
    for approval in [true, false] {
        let store = Arc::new(MemoryStore::new());
        handoff_journey::cancel_completed_with_probe(
            store.clone(),
            approval,
            |request| async move {
                store.test_terminal_history_corruption(&request);
            },
        )
        .await;
    }
}
use crabber::session::session_owner::*;

#[path = "../../crabber-session/src/driver_raw_access_contract.rs"]
#[allow(dead_code)]
mod driver_raw_access_contract;
#[path = "../../crabber-session/src/successor_driver_contract.rs"]
#[allow(dead_code)]
mod successor_driver_contract;
