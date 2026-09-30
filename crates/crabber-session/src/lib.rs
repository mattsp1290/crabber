//! Session persistence boundary and in-memory implementation.

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod abandonment;
pub mod admission_contract;
mod memory;
#[cfg(feature = "postgres")]
mod postgres;
mod store;
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
