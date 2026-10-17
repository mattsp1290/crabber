#[path = "admission_execution/prepared.rs"]
mod prepared;
pub(super) use prepared::claim_prepared;
#[path = "admission_execution/inspection.rs"]
mod inspection;
use super::records::{load_run, lookup_receipt};
use super::{SqliteExecution, SqliteStore, db};
use crate::StoreError;
use crate::{
    AdmissionExecutionError as Error, AdmissionExecutionRecord, AdmissionExecutionState,
    ClaimUnstartedAdmissionRequest, ClaimedAdmission,
};
use crabber_core::{Run, RunFence, RunId};
pub(super) use inspection::{inspect, validate_source};
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn load_record(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
) -> Result<Option<AdmissionExecutionRecord>, StoreError> {
    let record = load_unchecked(tx, run).await?;
    if let Some(record) = &record {
        super::admission_journal::read(tx, record).await?;
    }
    Ok(record)
}
pub(super) async fn load_record_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<AdmissionExecutionRecord>, crate::public_session::v2::proof::ReadError<StoreError>>
{
    use crate::public_session::v2::proof::ReadError;
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM admission_executions WHERE run_id=$1").bind(&run.0),proof).await.map_err(ReadError::Budget)?;
    let record = load_unchecked(tx, run).await?;
    if let Some(record) = &record {
        super::admission_journal::read_with_proof(tx, record, proof).await?;
    }
    Ok(record)
}
async fn load_unchecked(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
) -> Result<Option<AdmissionExecutionRecord>, StoreError> {
    let record = sqlx::query("SELECT capsule_version,CASE WHEN length(CAST(data AS BLOB))<=16777216 THEN data END AS data,start_state FROM admission_executions WHERE run_id=$1")
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .map(|row| {
            let encoded = row.try_get::<Option<String>, _>("data").map_err(db)?.ok_or(StoreError::AdmissionConflict)?;
            let record = AdmissionExecutionRecord::decode(&encoded)?;
            let version: i32 = row.try_get("capsule_version").map_err(db)?;
            if u32::try_from(version).ok() != Some(record.capsule.version) || !matches!(version, 1 | 2) { return Err(StoreError::AdmissionConflict); }
            let state: String = row.try_get("start_state").map_err(db)?;
            if state != format!("{:?}", record.state) {
                return Err(StoreError::AdmissionConflict);
            }
            Ok(record)
        })
        .transpose()?;
    if let Some(record) = &record
        && record.receipt.run_id != *run
    {
        return Err(StoreError::AdmissionConflict);
    }
    if record.is_none() {
        let orphan: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM admission_execution_sources WHERE run_id=$1)",
        )
        .bind(&run.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        if orphan {
            return Err(StoreError::AdmissionConflict);
        }
    }
    Ok(record)
}

async fn validate_binding(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AdmissionExecutionRecord,
    run: &Run,
) -> Result<(), Error> {
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages = super::execution_binding::original_messages(tx, &run.session_id, &run.id, count)
        .await
        .map_err(|_| Error::SemanticConflict)?;
    record.validate_messages(run, &messages)
}

pub(super) async fn claim(
    store: &SqliteStore,
    request: ClaimUnstartedAdmissionRequest,
) -> Result<ClaimedAdmission, Error> {
    let mut tx = store.begin_write().await.map_err(unknown)?;
    let run = load_run(&mut tx, &request.expected_fence.run_id)
        .await
        .map_err(unknown)?;
    super::session_owner::guard_native_claim(&mut tx, &request.session_id)
        .await
        .map_err(|error| match error {
            crate::session_owner::SessionOwnerError::Busy => Error::PreparationBusy,
            _ => Error::UnknownStoreFailure,
        })?;
    let value = claim_in_transaction(
        store,
        &mut tx,
        run,
        request,
        None,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await?;
    tx.commit().await.map_err(|_| Error::UnknownStoreFailure)?;
    Ok(value)
}
pub(in crate::sqlite) async fn claim_in_transaction(
    store: &SqliteStore,
    tx: &mut Transaction<'_, Sqlite>,
    mut run: Run,
    request: ClaimUnstartedAdmissionRequest,
    completion: Option<crate::native_parent::CompletionSource>,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ClaimedAdmission, Error> {
    let record = load_record(tx, &run.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::MissingEvidence)?;
    let receipt = lookup_receipt(tx, &request.session_id, &request.key)
        .await
        .map_err(unknown)?;
    if receipt.as_ref() != Some(&record.receipt) {
        return Err(Error::SemanticConflict);
    }
    validate_binding(tx, &record, &run).await?;
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
    super::records::save_run_with_proof(tx, &run, completion.as_ref(), read_proof)
        .await
        .map_err(unknown)?;
    Ok(ClaimedAdmission {
        record,
        fence: RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token.clone(),
        },
        run,
    })
}

fn unknown(_: StoreError) -> Error {
    Error::UnknownStoreFailure
}

pub(super) async fn begin(execution: &SqliteExecution) -> Result<(), Error> {
    begin_with_proof(
        execution,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await
}
pub(super) async fn begin_with_proof(
    execution: &SqliteExecution,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    proof
        .measure(&execution.fence)
        .map_err(|_| Error::UnknownStoreFailure)?;
    let store = &execution.store;
    let mut tx = store
        .begin_write()
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    let result = async {
        let run = super::records::load_run_with_proof(&mut tx, &execution.fence.run_id, proof)
            .await
            .map_err(|_| Error::UnknownStoreFailure)?;
        if run.claim_token != execution.fence.claim_token {
            return Err(Error::StaleOwner);
        }
        if run.status.is_terminal() {
            return Err(Error::AlreadyTerminal);
        }
        if run.lease_until <= store.clock.now() {
            return Err(Error::StaleOwner);
        }
        let mut record = load_record_with_proof(&mut tx, &run.id, proof)
            .await
            .map_err(|_| Error::UnknownStoreFailure)?
            .ok_or(Error::MissingEvidence)?;
        validate_binding_with_proof(&mut tx, &record, &run, proof).await?;
        super::read_proof::charge(&mut tx,
            sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2")
                .bind(&run.session_id.0).bind(record.key.as_str()), proof).await.map_err(|_| Error::UnknownStoreFailure)?;
        if lookup_receipt(&mut tx, &run.session_id, &record.key)
            .await
            .map_err(|_| Error::UnknownStoreFailure)?
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
        let sources = super::admission_journal::begin_captured_with_proof(&mut tx, &record, &run, proof)
            .await
            .map_err(|_| Error::UnknownStoreFailure)?;
        record.state = AdmissionExecutionState::Started;
        sqlx::query(
            "UPDATE admission_executions SET start_state='Started',data=$2 WHERE run_id=$1",
        )
        .bind(&run.id.0)
        .bind(record.encode().map_err(|_| Error::UnknownStoreFailure)?)
        .execute(&mut *tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
        super::native_enrollment::record_raw_native(&mut tx,
            crate::native_enrollment::caller::Caller::Raw,
            &crate::native_enrollment::caller::Fresh::AdmissionBegin(crate::native_enrollment::caller::AdmissionBegin {
                fence: &execution.fence, run: &run, record: &record,
                sources: sources.as_ref().map(|(init, begin)| (init, begin)),
            }), proof, #[cfg(test)] &store.native_raw_probe).await.map_err(|_| Error::UnknownStoreFailure)?;
        Ok(())
    }
    .await;
    let committed = super::transactions::finish(tx, result, unknown).await;
    #[cfg(test)]
    if committed.is_ok() && store.native_raw_probe.after_begin(&execution.fence.run_id) {
        return Err(Error::UnknownStoreFailure);
    }
    committed
}

async fn validate_binding_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AdmissionExecutionRecord,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages = super::execution_binding::original_messages_with_proof(
        tx,
        &run.session_id,
        &run.id,
        count,
        proof,
    )
    .await
    .map_err(|_| Error::SemanticConflict)?;
    record.validate_messages(run, &messages)
}

#[cfg(test)]
#[path = "admission_execution/source_tests.rs"]
mod source_tests;
