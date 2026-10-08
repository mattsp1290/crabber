use super::records::{decode_row, load_run, lookup_receipt, save_run};
use super::{SqliteExecution, SqliteStore, db, text};
use crate::StoreError;
use crate::{
    AdmissionExecutionError as Error, AdmissionExecutionRecord, AdmissionExecutionState,
    ClaimUnstartedAdmissionRequest, ClaimedAdmission,
};
use crabber_core::{Message, Run, RunFence, RunId};
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn load_record(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
) -> Result<Option<AdmissionExecutionRecord>, StoreError> {
    sqlx::query("SELECT data,start_state FROM admission_executions WHERE run_id=$1")
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .map(|row| {
            let record: AdmissionExecutionRecord = decode_row(&row, "data")?;
            let state: String = row.try_get("start_state").map_err(db)?;
            if state != format!("{:?}", record.state) {
                return Err(StoreError::AdmissionConflict);
            }
            Ok(record)
        })
        .transpose()
}

async fn validate_binding(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AdmissionExecutionRecord,
    run: &Run,
) -> Result<(), Error> {
    let row = sqlx::query("SELECT data FROM messages WHERE id=$1")
        .bind(&record.receipt.user_message_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?
        .ok_or(Error::SemanticConflict)?;
    let text: String = row.try_get("data").map_err(|_| Error::SemanticConflict)?;
    let user: Message = super::decode(&text).map_err(|_| Error::SemanticConflict)?;
    record.validate_binding(run, &user)
}

pub(super) async fn claim(
    store: &SqliteStore,
    request: ClaimUnstartedAdmissionRequest,
) -> Result<ClaimedAdmission, Error> {
    // Ownership paths are run-first. No session row/advisory lock here.
    let mut tx = store.begin_write().await.map_err(unknown)?;
    let result = async {
        let mut run = load_run(&mut tx, &request.expected_fence.run_id)
            .await
            .map_err(unknown)?;
        let record = load_record(&mut tx, &run.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::MissingEvidence)?;
        let receipt = lookup_receipt(&mut tx, &request.session_id, &request.key)
            .await
            .map_err(unknown)?;
        if receipt.as_ref() != Some(&record.receipt) {
            return Err(Error::SemanticConflict);
        }
        validate_binding(&mut tx, &record, &run).await?;
        record.verify_claim(&request, &run)?;
        let now = store.clock.now();
        if run.lease_until > now {
            return Err(Error::LiveLease);
        }
        let lease = time::Duration::try_from(request.lease).map_err(|_| Error::SemanticConflict)?;
        if lease <= time::Duration::ZERO {
            return Err(Error::SemanticConflict);
        }
        run.owner = request.owner;
        run.claim_token = uuid::Uuid::new_v4().to_string();
        run.lease_until = now.checked_add(lease).ok_or(Error::SemanticConflict)?;
        run.updated_at = now;
        save_run(&mut tx, &run).await.map_err(unknown)?;
        Ok(ClaimedAdmission {
            record,
            fence: RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
            run,
        })
    }
    .await;
    super::transactions::finish(tx, result, unknown).await
}

fn unknown(_: StoreError) -> Error {
    Error::UnknownStoreFailure
}

pub(super) async fn begin(execution: &SqliteExecution) -> Result<(), Error> {
    let store = &execution.store;
    let mut tx = store.begin_write().await.map_err(unknown)?;
    let result = async {
        let run = load_run(&mut tx, &execution.fence.run_id)
            .await
            .map_err(unknown)?;
        if run.claim_token != execution.fence.claim_token {
            return Err(Error::StaleOwner);
        }
        if run.status.is_terminal() {
            return Err(Error::AlreadyTerminal);
        }
        if run.lease_until <= store.clock.now() {
            return Err(Error::StaleOwner);
        }
        let mut record = load_record(&mut tx, &run.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::MissingEvidence)?;
        validate_binding(&mut tx, &record, &run).await?;
        if lookup_receipt(&mut tx, &run.session_id, &record.key)
            .await
            .map_err(unknown)?
            .as_ref()
            != Some(&record.receipt)
        {
            return Err(Error::SemanticConflict);
        }
        if record.receipt.run_id != run.id || record.receipt.session_id != run.session_id {
            return Err(Error::SemanticConflict);
        }
        if record.state != AdmissionExecutionState::Unstarted {
            return Err(Error::AlreadyStarted);
        }
        record.state = AdmissionExecutionState::Started;
        sqlx::query(
            "UPDATE admission_executions SET start_state='Started',data=$2 WHERE run_id=$1",
        )
        .bind(&run.id.0)
        .bind(text(&record).map_err(unknown)?)
        .execute(&mut *tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
        Ok(())
    }
    .await;
    super::transactions::finish(tx, result, unknown).await
}
