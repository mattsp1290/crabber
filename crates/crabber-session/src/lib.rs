//! Session persistence boundary and in-memory implementation.

/// Required version of the bundled PostgreSQL schema. Connect never migrates.
pub const POSTGRES_SCHEMA_VERSION: i32 = 17;
/// Required version of the bundled SQLite schema. Older files must be recreated
/// explicitly using the matching binary/backup; connect/migrate never upgrade them.
pub const SQLITE_SCHEMA_VERSION: i64 = 13;

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod abandonment;
mod admission_execution;
mod admission_journal;
#[cfg(test)]
mod binding_fault;
mod execution_binding;
mod typed_admission;
pub mod typed_admission_contract;
pub use execution_binding::{
    BoundAdmitRequest, BoundKeyedAdmitRequest, ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT,
    PROVIDER_BINDING_COMPONENT, RetainedExecutionBinding,
};
mod frontend_continuation;
#[cfg(test)]
mod frontend_contract;
pub use frontend_continuation::*;
mod tool_continuation;
pub mod tool_continuation_contract;
pub use tool_continuation::*;
mod approval;
mod approval_termination;
mod continuation_handoff;
pub use approval_termination::{ApprovalTermination, ApprovalTerminationKind};
pub mod approval_contract;
pub use approval::*;
mod attempt;
mod attempt_control;
pub mod attempt_control_contract;
pub use attempt_control::{
    AttemptControlOutcome, AttemptResolverClaim, AttemptRetirement, AttemptRetirementOutcome,
    AttemptUncertainty, AttemptUncertaintyCause, PendingCallDisposition,
};
mod attempt_conclusion;
pub use attempt_conclusion::{AttemptConclusion, MAX_RUN_BOUNDARY_BYTES, RunBoundary};
mod native_parent;
mod phase;
mod phase_resolution;
pub use phase_resolution::{PhaseResolution, PhaseResolutionOutcome};
mod phase_settlement;
pub use phase_settlement::{PhaseSettlement, PhaseSettlementRecords};
mod phase_start;
pub use phase_start::PhaseStart;
pub mod phase_contract;
pub use phase::*;
pub mod attempt_contract;
pub use admission_execution::*;
pub use attempt::{
    AttemptAuthority, AttemptCleanup, AttemptCleanupResult, AttemptCommand, AttemptError,
    AttemptFinish, AttemptMutation, AttemptMutationOutcome, AttemptReceipt, AttemptSnapshot,
    AttemptState, AttemptTarget, VersionedAttempt,
};
pub mod admission_contract;
#[cfg(test)]
mod admission_execution_contract;
pub mod execution_binding_contract;
pub mod queue;
#[cfg(test)]
mod queue_contract;
pub use queue::{
    QueueAction, QueueAuthority, QueueInput, QueueMutation, QueueOutcome, QueuePage,
    QueuePageRequest, QueueReceipt, QueueState, QueueSummary, RetainedQueue,
};
pub mod run_control;
pub mod run_control_contract;
pub use run_control::{
    RunControlAction, RunControlBinding, RunControlBoundary, RunControlError, RunControlKind,
    RunControlOutcome, RunControlPage, RunControlReceipt, RunControlRequest, RunControlStatus,
    RunControlSummary, RunControlTarget, WaitingCancellation,
};
mod deletion;
mod management;
pub use deletion::{
    DeleteSession, DeletionBlocker, DeletionError, DeletionOutcome, DeletionReceipt,
    SessionDisposition,
};
mod fork;
pub use fork::{ForkBoundary, ForkError, ForkOutcome, ForkReceipt, ForkSession};
pub mod management_contract;
pub mod session_owner;
pub mod session_owner_contract;
pub use management::{
    ListSessions, ManageSession, ManagementError, ManagementReceipt, SessionChange,
    SessionDefaults, SessionListCursor, SessionListPage, SessionSummary,
};
mod memory;
#[cfg(feature = "postgres")]
mod postgres;
mod public_history;
mod public_run;
mod public_session;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod run_facts;
mod snapshot_profile;
pub use public_history::{PublicHistoryOutcome, PublicHistoryPage};
pub use public_session::*;
mod public_state;
pub mod public_state_contract;
mod snapshot;
pub use public_run::{PublicRunOutcome, PublicRunState, PublicRunSummary};
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sql_snapshot;
#[cfg(feature = "sqlite")]
mod sqlite;
mod storable;
pub use public_state::{
    PublicStateError, PublicStateMutation, PublicStateReceipt, PublicStateSnapshot,
};
mod store;
pub use snapshot::{
    SnapshotContinuation, SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotPage,
    SnapshotRequest, SnapshotUsage,
};
pub mod revision_contract;
pub mod storetest;

pub use memory::MemoryStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStore;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStore;
pub use storable::{ensure_storable, ensure_storable_text};
pub use store::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest,
    Store, StoreError,
};

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn public_history_projection_contract() {
        crate::public_history::tests::run(&crate::MemoryStore::new()).await;
    }
    #[test]
    fn public_run_private_allocation_is_bounded() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        crate::public_run::tests::allocation_contract(&runtime, &crate::MemoryStore::new());
    }
    #[tokio::test]
    async fn public_run_state_contract() {
        crate::public_run::tests::run(&crate::MemoryStore::new()).await;
    }
    #[tokio::test]
    async fn memory_approval_contract() {
        crate::approval_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }
    #[tokio::test]
    async fn memory_execution_binding_fault_contract() {
        let store = crate::MemoryStore::new();
        crate::execution_binding_contract::faults::run(&store, |stage| {
            store.arm_binding_fault(stage);
        })
        .await;
    }

    #[tokio::test]
    async fn memory_execution_binding_contract() {
        crate::execution_binding_contract::run_contract(|clock| {
            crate::MemoryStore::with_clock(clock)
        })
        .await;
    }

    #[tokio::test]
    async fn memory_phase_contract() {
        crate::phase_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }

    #[tokio::test]
    async fn memory_attempt_contract() {
        crate::attempt_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }
    #[tokio::test]
    async fn memory_public_state_contract() {
        crate::public_state_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock))
            .await;
    }
    #[tokio::test]
    async fn memory_typed_admission_contract() {
        crate::typed_admission_contract::run_contract(|clock| {
            crate::MemoryStore::with_clock(clock)
        })
        .await;
    }
    #[tokio::test]
    async fn memory_revision_contract() {
        crate::revision_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }

    #[tokio::test]
    async fn memory_admission_contract() {
        crate::admission_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock))
            .await;
    }

    #[tokio::test]
    async fn memory_contract() {
        crate::storetest::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }

    #[tokio::test]
    async fn memory_workspace_identity_contract() {
        crate::storetest::run_workspace_identity_contract(|clock| {
            crate::MemoryStore::with_clock(clock)
        })
        .await;
    }

    #[tokio::test]
    async fn memory_storable_record_contract() {
        crate::storetest::run_storable_record_contract(|clock| {
            crate::MemoryStore::with_clock(clock)
        })
        .await;
    }
}

#[cfg(test)]
mod abandonment_contract;

#[cfg(test)]
#[tokio::test]
async fn memory_admission_execution_contract() {
    admission_execution_contract::run_contract(|clock| MemoryStore::with_clock(clock)).await;
}

mod phase_control;
pub use phase_control::{
    PhaseCleanup, PhaseControlOutcome, PhasePage, PhasePageRequest, PhaseUncertainty,
    PhaseUncertaintyCause,
};

mod admission_selection;
pub use admission_selection::{
    AdmissionSelectionPolicy, SELECTION_POLICY_COMPONENT, SelectionFallback,
};

mod continuation_origin;
#[cfg(test)]
#[tokio::test]
async fn memory_frontend_contract() {
    frontend_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
}

#[cfg(test)]
#[tokio::test]
async fn memory_frontend_near_limit_recovery() {
    frontend_contract::large::run(|clock| crate::MemoryStore::with_clock(clock)).await;
}

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
mod sql_fork_contract;

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
mod fork_pending_contract;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sql_deletion;
