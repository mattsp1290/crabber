//! Session persistence boundary and in-memory implementation.

/// Required version of the bundled PostgreSQL schema. Connect never migrates.
pub const POSTGRES_SCHEMA_VERSION: i32 = 6;
/// Required version of the bundled SQLite schema. Older files must be recreated
/// explicitly using the matching binary/backup; connect/migrate never upgrade them.
pub const SQLITE_SCHEMA_VERSION: i64 = 2;

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod abandonment;
mod admission_execution;
mod attempt;
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
    async fn memory_attempt_contract() {
        crate::attempt_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }
    #[tokio::test]
    async fn memory_public_state_contract() {
        crate::public_state_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock))
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
