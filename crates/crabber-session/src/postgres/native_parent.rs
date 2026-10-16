//! Native Begin witnesses bound to independent Run fence issuance.
#[path = "native_parent/run.rs"]
mod run;
#[path = "native_parent/source_protocol.rs"]
pub(super) mod source_protocol;
use crate::native_parent::{Error, Kind, Witness};
use crabber_core::{EventCursor, EventRecord, Run};
pub(super) use run::{change as record_run, change_completed as record_completed};
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn record_attempt(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &crate::AttemptMutation,
    attempt: &crate::attempt::AttemptRecord,
    receipt: &crate::AttemptReceipt,
) -> Result<(), crate::AttemptError> {
    if crate::POSTGRES_SCHEMA_VERSION >= 13
        && matches!(request.command, crate::AttemptCommand::Begin { .. })
    {
        let Some(issuance) = run::latest(tx, run)
            .await
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?
        else {
            return Ok(());
        };
        let witness = Witness::attempt(run, attempt, receipt, &issuance)
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
        save(tx, &witness)
            .await
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    }
    Ok(())
}
pub(super) async fn record_phase(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    phase: &crate::phase::PhaseRecord,
    receipt: &crate::PhaseReceipt,
) -> Result<(), crate::PhaseError> {
    if crate::POSTGRES_SCHEMA_VERSION >= 13 && receipt.operation == crate::PhaseOperation::Begin {
        let attempt = super::attempt::load_owned(tx, &run.id, &phase.snapshot.target.attempt.id)
            .await
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?
            .ok_or(crate::PhaseError::UnknownStoreOutcome)?;
        let Some(issuance) = run::latest(tx, run)
            .await
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?
        else {
            return Ok(());
        };
        let witness = Witness::phase(run, &attempt, phase, receipt, &issuance)
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?;
        save(tx, &witness)
            .await
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?;
    }
    Ok(())
}
async fn save(tx: &mut Transaction<'_, Postgres>, witness: &Witness) -> Result<(), Error> {
    witness.validate()?;
    sqlx::query("INSERT INTO native_parent_witnesses(event_seq,session_id,run_id,kind,operation_id,data_json) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(i64::try_from(witness.source.event_cursor.0).map_err(|_| Error)?)
        .bind(&witness.source.session_id.0).bind(&witness.source.run_id.0)
        .bind(kind(witness.source.kind)).bind(&witness.source.operation_id.0)
        .bind(serde_json::to_string(witness).map_err(|_| Error)?)
        .execute(&mut **tx).await.map_err(|_| Error)?;
    Ok(())
}
fn kind(kind: Kind) -> &'static str {
    match kind {
        Kind::Attempt => "attempt",
        Kind::Phase => "phase",
    }
}

pub(super) async fn verify_attempt(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    attempt: &crate::attempt::AttemptRecord,
) -> Result<Witness, Error> {
    let row = source(tx, run, Kind::Attempt, None).await?;
    let receipt: crate::AttemptReceipt = decode(&row, "receipt")?;
    let event = event(&row)?;
    let witness: Witness = decode(&row, "witness")?;
    let issuance = run::latest(tx, run).await?.ok_or(Error)?;
    witness.verify_attempt(run, attempt, &receipt, &event, &issuance)?;
    Ok(witness)
}
pub(super) async fn verify_phase(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    attempt: &crate::attempt::AttemptRecord,
    phase: &crate::phase::PhaseRecord,
) -> Result<Witness, Error> {
    verify_attempt(tx, run, attempt).await?;
    let row = source(tx, run, Kind::Phase, Some(&phase.snapshot.target.call_id.0)).await?;
    let receipt: crate::PhaseReceipt = decode(&row, "receipt")?;
    let event = event(&row)?;
    let witness: Witness = decode(&row, "witness")?;
    let issuance = run::latest(tx, run).await?.ok_or(Error)?;
    witness.verify_phase(run, attempt, phase, &receipt, &event, &issuance)?;
    Ok(witness)
}
async fn source(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    kind_: Kind,
    call: Option<&str>,
) -> Result<sqlx::postgres::PgRow, Error> {
    if crate::POSTGRES_SCHEMA_VERSION < 13 {
        return Err(Error);
    }
    sqlx::query(r#"WITH decoded AS (SELECT e.seq,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"(execution_attempt_changed|tool_phase_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND e.run_id=$2), latest AS (SELECT * FROM decoded WHERE d #>> '{kind}'=$3 AND (CAST($4 AS TEXT) IS NULL OR d #>> '{payload,phase,target,call_id}'=$4) ORDER BY seq DESC LIMIT 1) SELECT l.seq,CASE WHEN octet_length(l.data::text)<=65536 THEN l.data::text END AS event,CASE WHEN octet_length(r.data_json)<=8192 AND r.kind=$5 AND r.event_seq=l.seq THEN r.data_json END AS receipt,CASE WHEN octet_length(w.data_json)<=8192 AND w.session_id=$1 AND w.run_id=$2 AND w.kind=$5 AND w.operation_id=l.d #>> '{correlation}' THEN w.data_json END AS witness FROM latest l LEFT JOIN operation_receipts r ON r.session_id=$1 AND r.operation_id=l.d #>> '{correlation}' LEFT JOIN native_parent_witnesses w ON w.event_seq=l.seq"#)
        .bind(&run.session_id.0).bind(&run.id.0)
        .bind(match kind_ { Kind::Attempt => "execution_attempt_changed", Kind::Phase => "tool_phase_changed" })
        .bind(call).bind(kind(kind_)).fetch_optional(&mut **tx).await.map_err(|_| Error)?.ok_or(Error)
}
fn decode<T: serde::de::DeserializeOwned>(
    row: &sqlx::postgres::PgRow,
    field: &str,
) -> Result<T, Error> {
    let value: String = row
        .try_get::<Option<String>, _>(field)
        .map_err(|_| Error)?
        .ok_or(Error)?;
    serde_json::from_str(&value).map_err(|_| Error)
}
fn event(row: &sqlx::postgres::PgRow) -> Result<EventRecord, Error> {
    let value: serde_json::Value = decode(row, "event")?;
    let mut event =
        crate::public_state::decode_event(sqlx::types::Json(value)).map_err(|_| Error)?;
    let cursor = EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(|_| Error)?).map_err(|_| Error)?,
    );
    if event.cursor.is_some_and(|value| value != cursor) {
        return Err(Error);
    }
    event.cursor = Some(cursor);
    Ok(event)
}

pub(super) async fn verify_run(
    tx: &mut Transaction<'_, Postgres>,
    native: &Run,
) -> Result<(), Error> {
    run::latest(tx, native).await?.ok_or(Error)?.verify(native)
}

/// Authenticate the exact current issuer for preparation source inspection.
pub(super) async fn current_issuance(
    tx: &mut Transaction<'_, Postgres>,
    native: &Run,
) -> Result<crate::native_parent::RunIssuance, Error> {
    let value = run::latest(tx, native).await?.ok_or(Error)?;
    value.verify(native)?;
    Ok(value)
}
