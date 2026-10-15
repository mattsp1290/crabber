//! Session persistence boundary and in-memory implementation.

/// Required version of the bundled PostgreSQL schema. Connect never migrates.
pub const POSTGRES_SCHEMA_VERSION: i32 = 7;
/// Required version of the bundled SQLite schema. Older files must be recreated
/// explicitly using the matching binary/backup; connect/migrate never upgrade them.
pub const SQLITE_SCHEMA_VERSION: i64 = 3;

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod abandonment;
mod admission_execution;
#[cfg(test)]
mod binding_fault;
mod execution_binding;
mod typed_admission;
pub mod typed_admission_contract;
pub use execution_binding::{
    BoundAdmitRequest, BoundKeyedAdmitRequest, ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT,
    PROVIDER_BINDING_COMPONENT, RetainedExecutionBinding,
};
mod approval;
pub mod approval_contract;
pub use approval::*;
mod attempt;
mod attempt_conclusion;
pub use attempt_conclusion::{AttemptConclusion, MAX_RUN_BOUNDARY_BYTES, RunBoundary};
mod phase;
mod phase_settlement;
pub use phase_settlement::{PhaseSettlement, PhaseSettlementRecords};
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
mod memory;
#[cfg(feature = "postgres")]
mod postgres;
mod public_state;
pub mod public_state_contract;
mod snapshot;
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
