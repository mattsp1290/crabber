//! Bounded immutable Init/Begin sources, independent of mutable execution state.
use super::db;
use crate::admission_journal::{self, Begin, Init, Kind, Source};
use crate::native_parent::RunIssuance;
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::{AdmissionExecutionRecord, StoreError};
use crabber_core::{Run, RunId, SessionId};
use sqlx::{Postgres, Row, Transaction};
fn invalid() -> StoreError {
    StoreError::AdmissionConflict
}
async fn issuer(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    sequence: Option<u64>,
) -> Result<RunIssuance, StoreError> {
    let row=sqlx::query("SELECT sequence,CASE WHEN octet_length(data_json)<=2048 THEN data_json END AS data FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND ($3 IS NULL OR sequence=$3) ORDER BY sequence DESC LIMIT 1")
        .bind(&session.0).bind(&run.0).bind(sequence.map(i64::try_from).transpose().map_err(|_|invalid())?)
        .fetch_one(&mut **tx).await.map_err(db)?;
    let value: RunIssuance = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(db)?
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    value.validate().map_err(|_| invalid())?;
    if value.state.session_id != *session
        || value.state.run_id != *run
        || i64::try_from(value.sequence).map_err(|_| invalid())?
            != row.try_get::<i64, _>("sequence").map_err(db)?
    {
        return Err(invalid());
    }
    Ok(value)
}
async fn insert(tx: &mut Transaction<'_, Postgres>, source: Source) -> Result<(), StoreError> {
    let state = &source.issuance().state;
    sqlx::query("INSERT INTO admission_execution_sources(session_id,run_id,kind,data_json) VALUES($1,$2,$3,$4)")
        .bind(&state.session_id.0).bind(&state.run_id.0).bind(source.kind().name()).bind(source.encode()?)
        .execute(&mut **tx).await.map_err(db)?;
    Ok(())
}
pub(super) async fn init(
    tx: &mut Transaction<'_, Postgres>,
    record: &AdmissionExecutionRecord,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    let issuance = issuance_with_proof(tx, &run.session_id, &run.id, Some(1), proof)
        .await
        .map_err(|_| invalid())?;
    issuance.verify(run).map_err(|_| invalid())?;
    insert(tx, Source::Init(Init::new(record, issuance)?)).await
}
/// None denotes an authenticated legacy cohort, never missing new evidence.
pub(super) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    record: &AdmissionExecutionRecord,
) -> Result<Option<Init>, StoreError> {
    read_with_proof(tx, record, &mut Proof::legacy())
        .await
        .map_err(|e| e.into_domain_or(invalid()))
}
pub(super) async fn read_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    record: &AdmissionExecutionRecord,
    proof: &mut Proof,
) -> Result<Option<Init>, ReadError<StoreError>> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(SOURCE_COST).bind(&record.receipt.run_id.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    let protocol:i32=sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2")
        .bind(&record.receipt.session_id.0).bind(&record.receipt.run_id.0).fetch_one(&mut **tx).await.map_err(db)?;
    if !matches!(protocol, 0 | 1) {
        return Err(invalid().into());
    }
    let rows=sqlx::query("SELECT kind,session_id,CASE WHEN octet_length(data_json)<=8192 THEN data_json END AS data FROM admission_execution_sources WHERE run_id=$1 ORDER BY kind LIMIT 3")
        .bind(&record.receipt.run_id.0).fetch_all(&mut **tx).await.map_err(db)?;
    let mut init = None;
    let mut begin = None;
    for row in rows {
        if row.try_get::<String, _>("session_id").map_err(db)? != record.receipt.session_id.0 {
            return Err(invalid().into());
        }
        let source = Source::decode(
            &row.try_get::<Option<String>, _>("data")
                .map_err(db)?
                .ok_or_else(invalid)?,
        )?;
        if row.try_get::<String, _>("kind").map_err(db)? != source.kind().name() {
            return Err(invalid().into());
        }
        let issuance = source.issuance();
        if *issuance
            != issuance_with_proof(
                tx,
                &record.receipt.session_id,
                &record.receipt.run_id,
                Some(issuance.sequence),
                proof,
            )
            .await?
        {
            return Err(invalid().into());
        }
        match source.kind() {
            Kind::Init => {
                if init.replace(source).is_some() {
                    return Err(invalid().into());
                }
            }
            Kind::Begin => {
                if begin.replace(source).is_some() {
                    return Err(invalid().into());
                }
            }
        }
    }
    admission_journal::verify(record, protocol == 1, init.as_ref(), begin.as_ref())?;
    Ok(match init {
        Some(Source::Init(init)) => Some(init),
        _ => None,
    })
}

pub(super) async fn begin_captured_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    record: &AdmissionExecutionRecord,
    run: &Run,
    proof: &mut Proof,
) -> Result<Option<(Init, Begin)>, ReadError<StoreError>> {
    let Some(init) = read_with_proof(tx, record, proof).await? else {
        return Ok(None);
    };
    let issuance = issuance_with_proof(tx, &run.session_id, &run.id, None, proof).await?;
    proof
        .measure(&(&init, &issuance))
        .map_err(ReadError::Budget)?;
    issuance.verify(run).map_err(|_| invalid())?;
    let begin = Begin::new(&init, issuance)?;
    proof.measure(&begin).map_err(ReadError::Budget)?;
    insert(tx, Source::Begin(begin.clone())).await?;
    Ok(Some((init, begin)))
}

#[cfg(test)]
#[path = "admission_journal/tests.rs"]
mod tests;

pub(super) async fn no_effect(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &crabber_core::AbandonRequest,
) -> Result<Option<admission_journal::NoEffect>, StoreError> {
    let Some(record) = super::admission_execution::load_record(tx, &run.id).await? else {
        return Ok(None);
    };
    let Some(init) = read(tx, &record).await? else {
        return Ok(None);
    };
    if record.state != crate::AdmissionExecutionState::Unstarted {
        return Ok(None);
    }
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages =
        super::execution_binding::original_messages(tx, &run.session_id, &run.id, count).await?;
    record
        .validate_messages(run, &messages)
        .map_err(|_| invalid())?;
    if super::lookup_receipt(tx, &run.session_id, &record.key)
        .await?
        .as_ref()
        != Some(&record.receipt)
        || run.checkpoint.is_some()
        || run.error.is_some()
        || run.usage != crabber_core::Usage::default()
    {
        return Err(invalid());
    }
    let observed:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) originals")
        .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(count+1).map_err(|_|invalid())?).fetch_one(&mut **tx).await.map_err(db)?;
    let dirty:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE session_id=$1 AND run_id=$2) OR EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$2) OR EXISTS(SELECT 1 FROM epochs WHERE session_id=$1 AND run_id=$2 AND id<>$3)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(&run.epoch_id.0).fetch_one(&mut **tx).await.map_err(db)?;
    if observed != i64::try_from(count).map_err(|_| invalid())? || dirty {
        return Err(invalid());
    }
    let issuance = issuer(tx, &run.session_id, &run.id, None).await?;
    issuance.verify(run).map_err(|_| invalid())?;
    let value = admission_journal::NoEffect::new(&init, issuance)?;
    value.verify(&init, request)?;
    Ok(Some(value))
}

const SOURCE_COST: &str = "SELECT CAST(4+COALESCE(SUM(CAST(octet_length(data_json)+octet_length(kind)+octet_length(session_id) AS BIGINT)),0) AS BIGINT) FROM admission_execution_sources WHERE run_id=$1";
pub(super) async fn issuance_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    sequence: Option<u64>,
    proof: &mut Proof,
) -> Result<RunIssuance, ReadError<StoreError>> {
    let sequence = if let Some(sequence) = sequence {
        sequence
    } else {
        super::native_parent::source_protocol::history(tx, session, run, proof)
            .await
            .map_err(|e| e.map_domain(|_| invalid()))?;
        proof.charge(8).map_err(ReadError::Budget)?;
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(sequence) FROM native_run_issuances WHERE session_id=$1 AND run_id=$2",
        )
        .bind(&session.0)
        .bind(&run.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        u64::try_from(value.ok_or_else(invalid)?).map_err(|_| invalid())?
    };
    super::native_parent::source_protocol::original(tx, session, run, sequence, proof)
        .await
        .map_err(|e| e.map_domain(|_| invalid()))?
        .ok_or_else(|| invalid().into())
}
