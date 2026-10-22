//! Backend-local persistence; transaction and failure ordering are unchanged.
use crate::StoreError;
use crate::postgres::{PostgresStore, admission_execution, db, lookup_receipt};
use crabber_core::{AdmissionKey, RunId, SessionId};

impl PostgresStore {
    pub(in crate::postgres) async fn read_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Admission)
                .await
                .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?;
        let receipt = lookup_receipt(&mut tx, session, key)
            .await
            .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?;
        let record = if let Some(receipt) = receipt {
            admission_execution::load_record(&mut tx, &receipt.run_id)
                .await
                .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?
        } else {
            None
        };
        tx.commit()
            .await
            .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?;
        Ok(record)
    }
    pub(in crate::postgres) async fn read_admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Admission)
                .await
                .map_err(db)?;
        let state = admission_execution::load_record(&mut tx, run)
            .await?
            .map(|record| record.state);
        tx.commit().await.map_err(db)?;
        Ok(state)
    }
}
