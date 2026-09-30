//! Session persistence boundary and in-memory implementation.

mod memory;
#[cfg(feature = "postgres")]
mod postgres;
mod store;
pub mod storetest;

pub use memory::MemoryStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStore;
pub use store::{AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, Store, StoreError};

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn memory_contract() {
        crate::storetest::run_contract(|clock| crate::MemoryStore::with_clock(clock)).await;
    }
}
