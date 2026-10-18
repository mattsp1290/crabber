//! Canonical keyed Begin body supplied with the already-held native transaction.
use super::{
    AdmissionExecutionState, Error, load_record_with_proof, lookup_receipt,
    validate_binding_with_proof,
};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crabber_core::{Run, RunFence};
use sqlx::Postgres;
pub(in crate::postgres) struct BeginPublication {
    pub record: crate::AdmissionExecutionRecord,
    pub sources: Option<(
        crate::admission_journal::Init,
        crate::admission_journal::Begin,
    )>,
}
pub(in crate::postgres) async fn begin_initial_in(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    fence: &RunFence,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<BeginPublication, Error> {
    if run.id != fence.run_id || run.claim_token != fence.claim_token {
        return Err(Error::StaleOwner);
    }
    if run.status.is_terminal() {
        return Err(Error::AlreadyTerminal);
    }
    if run.lease_until <= now {
        return Err(Error::StaleOwner);
    }
    let mut record = load_record_with_proof(tx, &run.id, proof)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?
        .ok_or(Error::MissingEvidence)?;
    validate_binding_with_proof(tx, &record, run, proof).await?;
    super::super::read_proof::charge(tx,
            sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2")
                .bind(&run.session_id.0).bind(record.key.as_str()), proof).await.map_err(|_| Error::UnknownStoreFailure)?;
    if lookup_receipt(tx, &run.session_id, &record.key)
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
    let sources =
        super::super::admission_journal::begin_captured_with_proof(tx, &record, run, proof)
            .await
            .map_err(|_| Error::UnknownStoreFailure)?;
    record.state = AdmissionExecutionState::Started;
    proof
        .measure(&record)
        .map_err(|_| Error::UnknownStoreFailure)?;
    if record.capsule.version == 2 {
        sqlx::query(
            "UPDATE admission_executions SET start_state='Started',data_json=$2 WHERE run_id=$1",
        )
        .bind(&run.id.0)
        .bind(record.encode().map_err(|_| Error::UnknownStoreFailure)?)
        .execute(&mut **tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    } else {
        sqlx::query(
            "UPDATE admission_executions SET start_state='Started',data=$2 WHERE run_id=$1",
        )
        .bind(&run.id.0)
        .bind(super::super::json(&record).map_err(|_| Error::UnknownStoreFailure)?)
        .execute(&mut **tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    }
    Ok(BeginPublication { record, sources })
}
