//! Read-only admission metadata transactions, without dispatch authority.
use crate::StoreError;
use crate::sqlite::records::lookup_receipt;
use crate::sqlite::transactions::finish;
use crate::sqlite::{SqliteStore, admission_execution, db};
use crabber_core::{AdmissionKey, RunId, SessionId};

impl SqliteStore {
    pub(in crate::sqlite) async fn read_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?;
        let result = async {
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
            Ok(record)
        }
        .await;
        finish(tx, result, |_| {
            crate::AdmissionExecutionError::UnknownStoreFailure
        })
        .await
    }
    pub(in crate::sqlite) async fn read_admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        let mut tx = crate::transaction::begin(&self.readers).await.map_err(db)?;
        let result = async {
            let state = admission_execution::load_record(&mut tx, run)
                .await?
                .map(|record| record.state);
            Ok(state)
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
}
