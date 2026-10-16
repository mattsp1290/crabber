//! Original keyed input and protected Init/Begin, independently corroborated.
use super::super::super::super::super::super as backend;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{AdmissionReceipt, RunId, SessionId};
use serde::Deserialize;
use sqlx::{Postgres, Transaction};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fields {
    config_hash: String,
    plan_fingerprint: String,
    owner: String,
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<bool> {
    // Bound-provider inputs can be retained without a keyed execution record.
    // Missing legacy binding is unproven, never synthesized from current data.
    backend::execution_binding::read_with_proof(tx, session, run, proof).await?;
    let Some(record) = backend::admission_execution::load_record_with_proof(tx, run, proof).await?
    else {
        return Ok(false);
    };
    if record.receipt.session_id != *session || record.receipt.run_id != *run {
        return Err(unknown("execution scope"));
    }
    backend::read_proof::charge(
        tx,
        sqlx::query_scalar(RECEIPT_COST)
            .bind(&session.0)
            .bind(record.key.as_str()),
        proof,
    )
    .await?;
    let raw: Option<String> = sqlx::query_scalar(RECEIPT)
        .bind(&session.0)
        .bind(record.key.as_str())
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let original: AdmissionReceipt =
        serde_json::from_str(&raw.ok_or_else(|| unknown("original receipt bound"))?)
            .map_err(unknown)?;
    if original != record.receipt {
        return Err(unknown("original execution receipt"));
    }
    backend::read_proof::charge(
        tx,
        sqlx::query_scalar(FIELDS_COST)
            .bind(&session.0)
            .bind(&run.0),
        proof,
    )
    .await?;
    let raw: Option<String> = sqlx::query_scalar(FIELDS)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let fields: Fields =
        serde_json::from_str(&raw.ok_or_else(|| unknown("execution metadata bound"))?)
            .map_err(unknown)?;
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages =
        backend::execution_binding::original_messages_with_proof(tx, session, run, count, proof)
            .await?;
    record
        .validate_messages_scope(
            session,
            run,
            &fields.config_hash,
            &fields.plan_fingerprint,
            &fields.owner,
            &messages,
        )
        .map_err(unknown)?;
    // load_record_with_proof already authenticates the journal against this
    // protected cohort and independent original issuers on the same allowance.
    proof.charge(4)?;
    let protocol: i32 = sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2").bind(&session.0).bind(&run.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    Ok(protocol == 1)
}
const RECEIPT_COST: &str = "SELECT CAST(octet_length(data::text)+octet_length(run_id)+octet_length(user_message_id)+octet_length(admission_key) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const RECEIPT: &str = "SELECT CASE WHEN octet_length(data::text)<=8192 AND data->>'session_id'=session_id AND data->>'run_id'=run_id AND data->>'user_message_id'=user_message_id THEN data::text END FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const FIELDS_COST: &str = "SELECT CAST(COALESCE(octet_length(data->>'config_hash'),0)+COALESCE(octet_length(data->>'plan_fingerprint'),0)+COALESCE(octet_length(data->>'owner'),0) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const FIELDS: &str = "SELECT CASE WHEN octet_length(data->>'config_hash')<=64 AND octet_length(data->>'plan_fingerprint')<=64 AND octet_length(data->>'owner')<=512 THEN json_build_object('config_hash',data->>'config_hash','plan_fingerprint',data->>'plan_fingerprint','owner',data->>'owner')::text END FROM runs WHERE session_id=$1 AND id=$2";
