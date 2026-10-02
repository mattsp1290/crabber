//! Session persistence boundary and in-memory implementation.

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod abandonment;
mod admission_execution;
pub use admission_execution::*;
pub mod admission_contract;
#[cfg(test)]
mod admission_execution_contract;
mod memory;
#[cfg(feature = "postgres")]
mod postgres;
mod snapshot;
mod store;
pub use snapshot::{
    SnapshotContinuation, SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotPage,
    SnapshotRequest, SnapshotUsage,
};
pub mod storetest;

pub use memory::MemoryStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStore;
pub use store::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest,
    Store, StoreError,
};

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn memory_admission_contract() {
        crate::admission_contract::run_contract(|clock| crate::MemoryStore::with_clock(clock))
            .await;
    }

    #[tokio::test]
    async fn memory_contract() {
        crate::storetest::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }
}

#[cfg(test)]
mod abandonment_contract;

#[cfg(test)]
#[tokio::test]
async fn memory_admission_execution_contract() {
    admission_execution_contract::run_contract(|clock| MemoryStore::with_clock(clock)).await;
}
