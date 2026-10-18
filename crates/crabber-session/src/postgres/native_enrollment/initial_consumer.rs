//! Closed original Initial consumer under the actual Session transaction.
mod birth;
mod members;
pub(in crate::postgres) mod storage;
mod write;
pub(super) use birth::{check_birth, close, complete, open};

mod operations;
use crate::session_owner::owned_scope::{
    PendingInitialConsumerNativeCall, PendingInitialConsumerNativeReply,
};
use crate::session_owner::{SessionOwnerError as Error, preparation::PreparationCommitError};
impl crate::postgres::PostgresStore {
    pub(in crate::postgres) async fn pending_initial_consumer_native_impl(
        &self,
        call: PendingInitialConsumerNativeCall<'_>,
    ) -> Result<PendingInitialConsumerNativeReply, PreparationCommitError> {
        let mut parts = call.into_native()?;
        let mut tx = self
            .session_transaction(parts.context.session_id())
            .await
            .map_err(crate::deletion::owner_error)?;
        let result = Box::pin(operations::apply(self, &mut tx, &mut parts)).await;
        match result {
            Ok(reply) => {
                tx.commit().await.map_err(|_| Error::UnknownStoreOutcome)?;
                Ok(reply)
            }
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests;
